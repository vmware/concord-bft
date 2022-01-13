// Concord
//
// Copyright (c) 2019-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// standard library includes
#include <chrono>
#include <thread>
#include <set>
#include <string>
#include <vector>
#include <random>
#include <climits>
#include <optional>

// 3rd party includes
#include "gtest/gtest.h"

// own includes
#include "SimpleBCStateTransfer.hpp"
#include "BCStateTran.hpp"
#include "test_app_state.hpp"
#include "test_replica.hpp"
#include "Logger.hpp"
#include "DBDataStore.hpp"
#include "direct_kv_db_adapter.h"
#include "memorydb/client.h"
#include "storage/direct_kv_key_manipulator.h"
#include "ReservedPagesMock.hpp"
#include "EpochManager.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "hex_tools.h"  //leave for debug
#include "RVBManager.hpp"

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
using concord::storage::rocksdb::Client;
using concord::storage::rocksdb::KeyComparator;
#endif

using namespace std;
using namespace bftEngine::bcst;

using std::chrono::milliseconds;
using random_bytes_engine = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char>;

#define ASSERT_NFF ASSERT_NO_FATAL_FAILURE
#define ASSERT_DEST_UNDER_TEST ASSERT_TRUE(testConfig_.testTarget == TestConfig::TestTarget::DESTINATION)
#define ASSERT_SRC_UNDER_TEST ASSERT_TRUE(testConfig_.testTarget == TestConfig::TestTarget::SOURCE)
#define EMPTY_FUNC            \
  std::function<void(void)> { \
    []() {}                   \
  }

namespace bftEngine::bcst::impl {

using FetchingState = BCStateTran::FetchingState;

/////////////////////////////////////////////////////////
// Config
//
// Target configuration - can be modified in test body before calling initialize()
/////////////////////////////////////////////////////////
Config targetConfig() {
  return {
      1,                  // myReplicaId
      2,                  // fVal
      0,                  // cVal
      7,                  // numReplicas
      0,                  // numRoReplicas
      false,              // pedanticChecks
      false,              // isReadOnly
      1024,               // maxChunkSize
      128,                // maxNumberOfChunksInBatch
      1024,               // maxBlockSize
      256 * 1024 * 1024,  // maxPendingDataFromSourceReplica
      2048,               // maxNumOfReservedPages
      4096,               // sizeOfReservedPage
      600,                // gettingMissingBlocksSummaryWindowSize
      10,                 // minPrePrepareMsgsForPrimaryAwarness
      16,                 // fetchRangeSize
      12,                 // RVT_K
      300,                // refreshTimerMs
      2500,               // checkpointSummariesRetransmissionTimeoutMs
      60000,              // maxAcceptableMsgDelayMs
      0,                  // sourceReplicaReplacementTimeoutMs
      2000,               // fetchRetransmissionTimeoutMs
      2,                  // maxFetchRetransmissions
      5,                  // metricsDumpIntervalSec
      true,               // enableReservedPages
      true,               // enableSourceBlocksPreFetch
      true,               // enableSourceSelectorPrimaryAwareness
      true                // enableStoreRvbDataDuringCheckpointing
  };
}

/////////////////////////////////////////////////////////
// TestConfig
//
// Test configuration - all configuration that is not part of 'struct Config'.
// Some of the members purely configure the test environment, while some are used to configure the product and are
// part of this struct since we do not run the full replica.
// Can be modified in test body before calling initialize()
/////////////////////////////////////////////////////////
struct TestConfig {
  /**
   *  TestTarget
   * SOURCE: testing ST source production code, destination is fake
   * DESTINAION: testing ST destination production code, source is fake
   */
  enum class TestTarget { SOURCE, DESTINATION };

  /**
   * Constants
   * You may decide a constant is configurable by moving it into the 'Configurables' part
   * In some cases you might need to write additional code to support the new configuration value
   */
  static constexpr char bcstDbPath[] = "./bcst_db";
  static constexpr char fakeBcstDbPath[] = "./fake_bcst_db";
  static constexpr size_t numExpectedSourceSelectorMetricCounters = 6;

  /**
   * Configurable
   * A configurable value might be overridden before the actual test starts
   * All defaults are inlined
   */
  uint64_t maxNumOfRequiredStoredCheckpoints = 3;
  uint32_t numberOfRequiredReservedPages = 100;
  uint32_t minNumberOfUpdatedReservedPages = 3;
  uint32_t maxNumberOfUpdatedReservedPages = 100;
  uint32_t checkpointWindowSize = 150;
  uint32_t minBlockDataSize = 300;
  uint32_t lastReachedConsensusCheckpointNum = 10;
  bool productDbDeleteOnStart = true;
  bool productDbDeleteOnEnd = true;
  bool fakeDbDeleteOnStart = true;
  bool fakeDbDeleteOnEnd = true;
  TestTarget testTarget = TestTarget::DESTINATION;
  string logLevel = "error";  // choose: "trace", "debug", "info", "warn", "error", "fatal"
};

static inline std::ostream& operator<<(std::ostream& os, const TestConfig::TestTarget& c) {
  os << ((c == TestConfig::TestTarget::DESTINATION) ? "DESTINATION" : "SOURCE");
  return os;
}

static inline std::ostream& operator<<(std::ostream& os, const TestConfig& c) {
  os << std::boolalpha
     << KVLOG(c.bcstDbPath,
              c.fakeBcstDbPath,
              c.maxNumOfRequiredStoredCheckpoints,
              c.numberOfRequiredReservedPages,
              c.minNumberOfUpdatedReservedPages,
              c.maxNumberOfUpdatedReservedPages,
              c.checkpointWindowSize,
              c.minBlockDataSize,
              c.lastReachedConsensusCheckpointNum,
              c.productDbDeleteOnStart,
              c.productDbDeleteOnEnd,
              c.fakeDbDeleteOnStart,
              c.fakeDbDeleteOnEnd,
              c.testTarget,
              c.logLevel);
  return os;
}

/////////////////////////////////////////////////////////
// TestState
//
// Test initial state is calculated usually as test starts. You shouldn't change a test state directly, you can alter it
// by changing test infra code, product code, or test configuration (for example).
/////////////////////////////////////////////////////////
class TestState {
 public:
  uint64_t minRequiredBlockId = 0;
  uint64_t maxRequiredBlockId = 0;
  uint64_t nextRequiredBlock = 0;
  uint64_t maxRepliedCheckpointNum = 0;
  uint64_t minRepliedCheckpointNum = 0;
  uint64_t numBlocksToCollect = 0;
  uint64_t lastCheckpointKnownToRequester = 0;

  void init(const TestConfig& testConfig, const TestAppState& appState, uint64_t _minRequiredBlockId = 0);
  void moveToNextCycle(TestConfig& testConfig,
                       const TestAppState& appState,
                       uint64_t _minRequiredBlockId,
                       uint32_t _lastReachedConsensusCheckpointNum);
};

void TestState::init(const TestConfig& testConfig, const TestAppState& appState, uint64_t _minRequiredBlockId) {
  minRequiredBlockId = (_minRequiredBlockId == 0) ? appState.getGenesisBlockNum() + 1 : _minRequiredBlockId;
  maxRequiredBlockId = (testConfig.lastReachedConsensusCheckpointNum + 1) * testConfig.checkpointWindowSize;
  ASSERT_LE(minRequiredBlockId, maxRequiredBlockId);

  maxRepliedCheckpointNum = testConfig.lastReachedConsensusCheckpointNum;
  minRepliedCheckpointNum = maxRepliedCheckpointNum - testConfig.maxNumOfRequiredStoredCheckpoints + 1;
  lastCheckpointKnownToRequester = std::max(((minRequiredBlockId - 1) / testConfig.checkpointWindowSize), (uint64_t)1);
  numBlocksToCollect = maxRequiredBlockId - minRequiredBlockId + 1;
  ASSERT_GE(maxRepliedCheckpointNum, minRepliedCheckpointNum);
  ASSERT_GT(maxRepliedCheckpointNum, lastCheckpointKnownToRequester);
}

void TestState::moveToNextCycle(TestConfig& testConfig,
                                const TestAppState& appState,
                                uint64_t _minRequiredBlockId,
                                uint32_t _lastReachedConsensusCheckpointNum) {
  ASSERT_GT(_lastReachedConsensusCheckpointNum, testConfig.lastReachedConsensusCheckpointNum);
  ASSERT_GT(_minRequiredBlockId, maxRequiredBlockId);
  testConfig.lastReachedConsensusCheckpointNum = _lastReachedConsensusCheckpointNum;
  init(testConfig, appState, _minRequiredBlockId);
}

static inline std::ostream& operator<<(std::ostream& os, const TestState& c) {
  os << std::boolalpha
     << KVLOG(c.minRequiredBlockId,
              c.maxRequiredBlockId,
              c.maxRepliedCheckpointNum,
              c.minRepliedCheckpointNum,
              c.numBlocksToCollect,
              c.lastCheckpointKnownToRequester);
  return os;
}

/////////////////////////////////////////////////////////
// Global static helper functions
/////////////////////////////////////////////////////////
static void fillRandomBytes(char* data, size_t bytesToFill) {
  using random_bytes_engine = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char>;
  random_bytes_engine rbe;
  std::generate(data, data + bytesToFill, std::ref(rbe));
}

static void assertMsgType(const Msg& msg, uint16_t type) {
  auto header = reinterpret_cast<BCStateTranBaseMsg*>(msg.data_.get());
  ASSERT_EQ(type, header->type);
}

static void deleteBcStateTransferDbFolder(const string& path) {
  string cmd = string("rm -rf ") + string(path);
  if (system(cmd.c_str())) {
    ASSERT_TRUE(false);
  }
}

static DataStore* createDataStore(const string& dbName, const Config& targetConfig) {
#ifdef USE_ROCKSDB
  // create a data store
  auto* db_key_comparator = new concord::kvbc::v1DirectKeyValue::DBKeyComparator();
  concord::storage::IDBClient::ptr dbc(
      new concord::storage::rocksdb::Client(string(dbName), make_unique<KeyComparator>(db_key_comparator)));
  dbc->init();
  return new DBDataStore(dbc,
                         targetConfig.sizeOfReservedPage,
                         make_shared<concord::storage::v1DirectKeyValue::STKeyManipulator>(),
                         targetConfig.enableReservedPages);
#else
  concord::storage::IDBClient::ptr dbc2(new concord::storage::memorydb::Client(comparator));
  return new InMemoryDataStore(targetConfig.sizeOfReservedPage);
#endif
}

/////////////////////////////////////////////////////////
// DataGenerator
//
// Generates blocks, reserved pages, blocks, digests and
// whatever other data needed for the test to run.
// The data generated is random, but the cryptography applied is valid
/////////////////////////////////////////////////////////
class DataGenerator {
 public:
  DataGenerator(const Config& targetConfig, const TestConfig& testConfig);
  void generateBlocks(TestAppState& appState, uint64_t fromBlockId, uint64_t toBlockId);
  void generateCheckpointDescriptors(const TestAppState& appstate,
                                     DataStore* datastore,
                                     uint64_t minRepliedCheckpointNum,
                                     uint64_t maxRepliedCheckpointNum,
                                     RVBManager* rvbm = nullptr);
  std::unique_ptr<MessageBase> generatePrePrepareMsg(ReplicaId sender_id);

 protected:
  void generateReservedPages(DataStore* datastore, uint64_t checkpointNumber);

  const Config& targetConfig_;
  const TestConfig& testConfig_;
  // needed by generatePrePrepareMsg()
  bftEngine::test::ReservedPagesMock<EpochManager> fakeReservedPages_;
};

/////////////////////////////////////////////////////////
// BcStTestDelegator
//
// To be able to call into ST non-public function, include this class and use it as an interface
/////////////////////////////////////////////////////////
class BcStTestDelegator {
 public:
  BcStTestDelegator(const std::unique_ptr<BCStateTran>& stateTransfer) : stateTransfer_(stateTransfer) {}

  // State Transfer
  static constexpr size_t sizeOfElementOfVirtualBlock = sizeof(BCStateTran::ElementOfVirtualBlock);
  static constexpr size_t sizeOfHeaderOfVirtualBlock = sizeof(BCStateTran::HeaderOfVirtualBlock);
  static constexpr uint64_t ID_OF_VBLOCK_RES_PAGES = BCStateTran::ID_OF_VBLOCK_RES_PAGES;

  void onTimerImp() { stateTransfer_->onTimerImp(); }
  uint64_t uniqueMsgSeqNum() { return stateTransfer_->uniqueMsgSeqNum(); }
  template <typename T>
  bool onMessage(const T* m, uint32_t msgLen, uint16_t replicaId) {
    return stateTransfer_->onMessage(m, msgLen, replicaId);
  }
  bool onMessage(const ItemDataMsg* m, uint32_t msgLen, uint16_t replicaId, time_point<steady_clock> msgArrivalTime) {
    return stateTransfer_->onMessage(m, msgLen, replicaId, msgArrivalTime);
  }
  uint64_t getNextRequiredBlock() { return stateTransfer_->fetchState_.nextBlockId; }
  RVBManager* getRvbManager() { return stateTransfer_->rvbm_.get(); }
  RangeValidationTree* getRvt() { return stateTransfer_->rvbm_->in_mem_rvt_.get(); }
  void createCheckpointOfCurrentState(uint64_t checkpointNum) {
    stateTransfer_->createCheckpointOfCurrentState(checkpointNum);
  };
  const std::vector<std::pair<BlockId, STDigest>> getPrunedBlocksDigests() {
    return stateTransfer_->rvbm_->pruned_blocks_digests_;
  }
  void fillHeaderOfVirtualBlock(std::unique_ptr<char[]>& rawVBlock,
                                uint32_t numberOfUpdatedPages,
                                uint64_t lastCheckpointKnownToRequester);
  void fillElementOfVirtualBlock(DataStore* datastore,
                                 char* position,
                                 uint32_t pageId,
                                 uint64_t checkpointNumber,
                                 const STDigest& pageDigest,
                                 uint32_t sizeOfReservedPage);
  uint32_t getSizeOfVirtualBlock(char* virtualBlock, uint32_t pageSize) {
    return stateTransfer_->getSizeOfVirtualBlock(virtualBlock, pageSize);
  }
  bool checkStructureOfVirtualBlock(char* virtualBlock,
                                    uint32_t virtualBlockSize,
                                    uint32_t pageSize,
                                    logging::Logger& logger) {
    return stateTransfer_->checkStructureOfVirtualBlock(virtualBlock, virtualBlockSize, pageSize, logger);
  }

  // Source Selector
  void assertSourceSelectorMetricKeyVal(const std::string& key, uint64_t val);
  SourceSelector& getSourceSelector() { return stateTransfer_->sourceSelector_; }

 private:
  const std::unique_ptr<BCStateTran>& stateTransfer_;
};

/////////////////////////////////////////////////////////
// FakeReplicaBase
//
// Base class for a fake replica
/////////////////////////////////////////////////////////
class FakeReplicaBase {
 public:
  FakeReplicaBase(const Config& targetConfig,
                  const TestConfig& testConfig,
                  const TestState& testState,
                  TestReplica& testedReplicaIf,
                  const std::shared_ptr<DataGenerator>& dataGen,
                  std::shared_ptr<BcStTestDelegator>& testAtapter);
  virtual ~FakeReplicaBase();
  const TestAppState& getAppState() const { return appState_; }

  // Helper functions
  size_t clearSentMessagesByMessageType(uint16_t type) { return filterSentMessagesByMessageType(type, false); }
  size_t keepSentMessagesByMessageType(uint16_t type) { return filterSentMessagesByMessageType(type, true); }

 private:
  size_t filterSentMessagesByMessageType(uint16_t type, bool keep);

 protected:
  const Config& targetConfig_;
  const TestConfig& testConfig_;
  const TestState& testState_;
  std::shared_ptr<DataStore> datastore_;
  TestAppState appState_;
  TestReplica& testedReplicaIf_;
  unique_ptr<RVBManager> rvbm_;
  const std::shared_ptr<DataGenerator> dataGen_;
  const std::shared_ptr<BcStTestDelegator> stDelegator_;
};

/////////////////////////////////////////////////////////
// FakeDestination
//
// Fake one or more destination replicas.
// Supposed to work against real ST product source.
/////////////////////////////////////////////////////////Fake a source or multiple sources.
class FakeDestination : public FakeReplicaBase {
 public:
  FakeDestination(const Config& targetConfig,
                  const TestConfig& testConfig,
                  const TestState& testState,
                  TestReplica& testedReplicaIf,
                  const std::shared_ptr<DataGenerator>& dataGen,
                  std::shared_ptr<BcStTestDelegator>& stAdapter)
      : FakeReplicaBase(targetConfig, testConfig, testState, testedReplicaIf, dataGen, stAdapter) {}
  ~FakeDestination() {}
  void sendAskForCheckpointSummariesMsg(uint64_t minRelevantCheckpointNum);
  void sendFetchBlocksMsg(uint64_t firstRequiredBlock, uint64_t lastRequiredBlock);
  void sendFetchResPagesMsg(uint64_t lastCheckpointKnownToRequester, uint64_t requiredCheckpointNum);
  uint64_t getLastMsgSeqNum() { return lastMsgSeqNum_; }

 protected:
  uint64_t lastMsgSeqNum_;
};

/////////////////////////////////////////////////////////
// FakeSources
//
// Fake a source or multiple sources.
// Supposed to work against real ST product destination.
/////////////////////////////////////////////////////////
class FakeSources : public FakeReplicaBase {
 public:
  FakeSources(const Config& targetConfig,
              const TestConfig& testConfig,
              const TestState& testState,
              TestReplica& testedReplicaIf,
              const std::shared_ptr<DataGenerator>& dataGen,
              std::shared_ptr<BcStTestDelegator>& stAdapter);

  // Source (fake) Replies
  void replyAskForCheckpointSummariesMsg();
  void replyFetchBlocksMsg();
  void replyResPagesMsg(bool& outDoneSending);

 protected:
  std::unique_ptr<char[]> rawVBlock_;
  std::optional<FetchResPagesMsg> lastReceivedFetchResPagesMsg_;
};

/////////////////////////////////////////////////////////
// BcStTest
//
// Test fixture for blockchain state transfer tests
/////////////////////////////////////////////////////////
class BcStTest : public ::testing::Test {
  /**
   * We are testing destination or source ST replica.
   * To simplify code, and to avoid having another hierarchy of production dest/source "replicas" which wrap ST
   * module in test, we include in this fixture 2 types of members:
   * 1) Test infrastructure-related members
   * 2) State Transfer building blocks, which represent an ST replica in test, with ST being tested.
   */
 protected:
  // Infra - Initializers and finalizers
  void SetUp() override{};
  void TearDown() override;
  void initialize();

  // Infra configuration and initialization
  Config targetConfig_ = targetConfig();
  TestConfig testConfig_;
  TestState testState_;

  // Infra Fake replicas
  std::unique_ptr<FakeSources> fakeSrcReplica_;
  std::unique_ptr<FakeDestination> fakeDstReplica_;

  // Infra services
  std::shared_ptr<DataGenerator> dataGen_;
  std::shared_ptr<BcStTestDelegator> stDelegator_;

 private:
  // Infra initialize helpers
  void printConfiguration();
  void configureLog(const string& logLevel);
  bool initialized_ = false;

 protected:
  // Infra member functions
  void compareAppStateblocks(uint64_t minBlockId, uint64_t maxBlockId) const;

  // Target/Product ST - destination API & assertions
  void dstStartRunningAndCollecting(FetchingState expectedState = FetchingState::NotFetching);
  void dstStartCollecting();
  void dstAssertAskForCheckpointSummariesSent(uint64_t checkpoint_num);
  void dstAssertFetchBlocksMsgSent();
  void dstAssertFetchResPagesMsgSent();

  // Target/Product ST - source API & assertions// This should be the same as TestConfig
  void srcAssertCheckpointSummariesSent(uint64_t minRepliedCheckpointNum, uint64_t maxRepliedCheckpointNum);
  void srcAssertItemDataMsgBatchSentWithBlocks(uint64_t minExpectedBlockId, uint64_t maxExpectedBlockId);
  void srcAssertItemDataMsgBatchSentWithResPages(uint32_t expectedChunksSent, uint64_t requiredCheckpointNum);

  // Target/Product ST - common (as source/destination) API & assertions
  void cmnStartRunning(FetchingState expectedState = FetchingState::NotFetching);

  // Target/Product ST - Source Selector
  using MetricKeyValPairs = std::map<std::string, uint64_t>;
  void validateSourceSelectorMetricCounters(const MetricKeyValPairs& metricCounters);

  // Target/Product ST - Convenience common code
  template <class R, class... Args>
  void getMissingblocksStage(std::function<R(Args...)> callAtStart = EMPTY_FUNC,
                             std::function<R(Args...)> callAtEnd = EMPTY_FUNC);
  void getReservedPagesStage();
  void dstRestart(bool productDbDeleteOnEnd, FetchingState expectedState);

 public:  // why public? quick workaround to allow binding on derived class
  void dstRestartWithIterations(std::set<size_t>& execOnIterations, FetchingState expectedState);

 protected:
  // These members are used to construct stateTransfer_
  TestAppState appState_;
  DataStore* datastore_;
  TestReplica testedReplicaIf_;
  std::unique_ptr<BCStateTran> stateTransfer_;
};  // class BcStTest

/////////////////////////////////////////////////////////
// DataGenerator - definition
/////////////////////////////////////////////////////////
DataGenerator::DataGenerator(const Config& targetConfig, const TestConfig& testConfig)
    : targetConfig_(targetConfig), testConfig_(testConfig) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&fakeReservedPages_);
}

/**
 * toBlockId is assumed to be a checkpoint block, we also assume
 * generatedBlocks_ is empty
 */
void DataGenerator::generateBlocks(TestAppState& appState, uint64_t fromBlockId, uint64_t toBlockId) {
  std::unique_ptr<char[]> buff = std::make_unique<char[]>(Block::getMaxTotalBlockSize());
  ConcordAssertEQ(toBlockId % testConfig_.checkpointWindowSize, 0);
  ConcordAssertGT(fromBlockId, 1);

  auto maxBlockDataSize = Block::calcMaxDataSize();
  std::shared_ptr<Block> prevBlk;
  for (size_t i = fromBlockId; i <= toBlockId; ++i) {
    if (appState.hasBlock(i)) continue;
    uint32_t dataSize = static_cast<uint32_t>(rand()) % (maxBlockDataSize - testConfig_.minBlockDataSize + 1) +
                        testConfig_.minBlockDataSize;
    ConcordAssertLE(dataSize, maxBlockDataSize);
    fillRandomBytes(buff.get(), dataSize);
    std::shared_ptr<Block> blk;
    StateTransferDigest digestPrev{1};
    if (i == fromBlockId) {
      blk = Block::createFromData(dataSize, buff.get(), i, digestPrev);
    } else {
      if (!prevBlk) {
        prevBlk = appState.peekBlock(i - 1);
        ASSERT_TRUE(prevBlk);
      }
      computeBlockDigest(
          prevBlk->blockId, reinterpret_cast<const char*>(prevBlk.get()), prevBlk->totalBlockSize, &digestPrev);
      blk = Block::createFromData(dataSize, buff.get(), i, digestPrev);
    }
    // we assume that last parameter is ignored
    ASSERT_TRUE(appState.putBlock(i, reinterpret_cast<const char*>(blk.get()), blk->totalBlockSize, false));
    prevBlk = blk;
  }
}

void DataGenerator::generateCheckpointDescriptors(const TestAppState& appState,
                                                  DataStore* datastore,
                                                  uint64_t minRepliedCheckpointNum,
                                                  uint64_t maxRepliedCheckpointNum,
                                                  RVBManager* rvbm) {
  ASSERT_LE(minRepliedCheckpointNum, maxRepliedCheckpointNum);
  ASSERT_TRUE(rvbm);

  // Compute digest of last block
  uint64_t lastBlockId = (maxRepliedCheckpointNum + 1) * testConfig_.checkpointWindowSize;
  auto lastBlk = appState.peekBlock(lastBlockId);
  ASSERT_TRUE(lastBlk);
  StateTransferDigest lastBlockDigest;
  computeBlockDigest(
      lastBlockId, reinterpret_cast<const char*>(lastBlk.get()), lastBlk->totalBlockSize, &lastBlockDigest);

  for (uint64_t i = minRepliedCheckpointNum; i <= maxRepliedCheckpointNum; ++i) {
    // for now, we do not support (expect) setting into an already set descriptor
    ASSERT_FALSE(datastore->hasCheckpointDesc(i));
    DataStore::CheckpointDesc desc;
    desc.checkpointNum = i;
    desc.maxBlockId = (i + 1) * testConfig_.checkpointWindowSize;
    auto digestBytes = desc.digestOfMaxBlockId.getForUpdate();
    if (i == maxRepliedCheckpointNum)
      memcpy(digestBytes, &lastBlockDigest, sizeof(lastBlockDigest));
    else {
      auto blk = appState.peekBlock(desc.maxBlockId + 1);
      ASSERT_TRUE(blk);
      memcpy(digestBytes, &blk->digestPrev, sizeof(blk->digestPrev));
    }

    ASSERT_NFF(generateReservedPages(datastore, i));
    DataStore::ResPagesDescriptor* resPagesDesc = datastore->getResPagesDescriptor(i);
    STDigest digestOfResPagesDescriptor;
    BCStateTran::computeDigestOfPagesDescriptor(resPagesDesc, digestOfResPagesDescriptor);

    desc.digestOfResPagesDescriptor = digestOfResPagesDescriptor;
    rvbm->updateRvbDataDuringCheckpoint(desc);
    datastore->setCheckpointDesc(i, desc);
  }

  datastore->setFirstStoredCheckpoint(minRepliedCheckpointNum);
  datastore->setLastStoredCheckpoint(maxRepliedCheckpointNum);
}

void DataGenerator::generateReservedPages(DataStore* datastore, uint64_t checkpointNumber) {
  uint32_t idx = 0;
  std::unique_ptr<char[]> buffer(new char[targetConfig_.sizeOfReservedPage]);
  for (uint32_t pageId{0}; pageId < testConfig_.maxNumberOfUpdatedReservedPages; ++pageId) {
    ConcordAssertLT(idx, testConfig_.maxNumberOfUpdatedReservedPages);
    STDigest pageDigest;
    fillRandomBytes(buffer.get(), targetConfig_.sizeOfReservedPage);
    BCStateTran::computeDigestOfPage(
        pageId, checkpointNumber, buffer.get(), targetConfig_.sizeOfReservedPage, pageDigest);
    ASSERT_TRUE(!pageDigest.isZero());
    datastore->setResPage(pageId, checkpointNumber, pageDigest, buffer.get());
    idx++;
  }
}

std::unique_ptr<MessageBase> DataGenerator::generatePrePrepareMsg(ReplicaId sender_id) {
  static constexpr ViewNum view_num_ = 1u;
  static constexpr SeqNum seq_num_ = 2u;
  static constexpr CommitPath commit_path_ = CommitPath::OPTIMISTIC_FAST;
  return make_unique<PrePrepareMsg>(sender_id, view_num_, seq_num_, commit_path_, 0);
}

/////////////////////////////////////////////////////////
// BcStTestDelegator - definition
/////////////////////////////////////////////////////////
void BcStTestDelegator::fillHeaderOfVirtualBlock(std::unique_ptr<char[]>& rawVBlock,
                                                 uint32_t numberOfUpdatedPages,
                                                 uint64_t lastCheckpointKnownToRequester) {
  BCStateTran::HeaderOfVirtualBlock* header = reinterpret_cast<BCStateTran::HeaderOfVirtualBlock*>(rawVBlock.get());
  header->lastCheckpointKnownToRequester = lastCheckpointKnownToRequester;
  header->numberOfUpdatedPages = numberOfUpdatedPages;
}

void BcStTestDelegator::fillElementOfVirtualBlock(DataStore* datastore,
                                                  char* position,
                                                  uint32_t pageId,
                                                  uint64_t checkpointNumber,
                                                  const STDigest& pageDigest,
                                                  uint32_t sizeOfReservedPage) {
  BCStateTran::ElementOfVirtualBlock* currElement = reinterpret_cast<BCStateTran::ElementOfVirtualBlock*>(position);
  currElement->pageId = pageId;
  currElement->checkpointNumber = checkpointNumber;
  currElement->pageDigest = pageDigest;
  ASSERT_TRUE(!currElement->pageDigest.isZero());
  datastore->getResPage(pageId, checkpointNumber, nullptr, currElement->page, sizeOfReservedPage);
  ASSERT_TRUE(!pageDigest.isZero());
}

void BcStTestDelegator::assertSourceSelectorMetricKeyVal(const std::string& key, uint64_t val) {
  auto& ssMetrics_ = stateTransfer_->sourceSelector_.metrics_;
  if (key == "total_replacements_") {
    ASSERT_EQ(ssMetrics_.total_replacements_.Get().Get(), val);
  } else if (key == "replacement_due_to_no_source_") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_no_source_.Get().Get(), val);
  } else if (key == "replacement_due_to_bad_data_") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_bad_data_.Get().Get(), val);
  } else if (key == "replacement_due_to_retransmission_timeout_") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_retransmission_timeout_.Get().Get(), val);
  } else if (key == "replacement_due_to_periodic_change_") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_periodic_change_.Get().Get(), val);
  } else if (key == "replacement_due_to_source_same_as_primary_") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_source_same_as_primary_.Get().Get(), val);
  } else {
    FAIL() << "Unexpected key!";
  }
}

/////////////////////////////////////////////////////////
// FakeReplicaBase - definition
/////////////////////////////////////////////////////////
FakeReplicaBase::FakeReplicaBase(const Config& targetConfig,
                                 const TestConfig& testConfig,
                                 const TestState& testState,
                                 TestReplica& testedReplicaIf,
                                 const std::shared_ptr<DataGenerator>& dataGen,
                                 std::shared_ptr<BcStTestDelegator>& stAdapter)
    : targetConfig_(targetConfig),
      testConfig_(testConfig),
      testState_(testState),
      testedReplicaIf_(testedReplicaIf),
      dataGen_(dataGen),
      stDelegator_(stAdapter) {
  if (testConfig_.fakeDbDeleteOnStart) deleteBcStateTransferDbFolder(testConfig_.fakeBcstDbPath);
  datastore_.reset(createDataStore(testConfig_.fakeBcstDbPath, targetConfig_));
  datastore_->setNumberOfReservedPages(testConfig_.numberOfRequiredReservedPages);
  rvbm_ = std::make_unique<RVBManager>(targetConfig, &appState_, datastore_);
}

FakeReplicaBase::~FakeReplicaBase() {
  if (testConfig_.fakeDbDeleteOnEnd) deleteBcStateTransferDbFolder(testConfig_.fakeBcstDbPath);
}

/**
 * keep is true: keep only messages with msg->type == type
 * keep is false: keep all message with msg->type != type
 * return number of messages deleted
 */
size_t FakeReplicaBase::filterSentMessagesByMessageType(uint16_t type, bool keep) {
  auto& sent_messages_ = testedReplicaIf_.sent_messages_;
  size_t n{0};
  for (auto it = sent_messages_.begin(); it != sent_messages_.end();) {
    auto header = reinterpret_cast<BCStateTranBaseMsg*>((*it).data_.get());
    // This block can be much shorter. For better readability, keep it like that
    if (keep) {
      if (header->type != type) {
        it = sent_messages_.erase(it);
        ++n;
        continue;
      }
    } else {  // keep == false
      if (header->type == type) {
        it = sent_messages_.erase(it);
        ++n;
        continue;
      }
    }
    ++it;
  }  // for
  return n;
}

/////////////////////////////////////////////////////////
// FakeDestination - definition
/////////////////////////////////////////////////////////
void FakeDestination::sendAskForCheckpointSummariesMsg(uint64_t minRelevantCheckpointNum) {
  ASSERT_SRC_UNDER_TEST;
  AskForCheckpointSummariesMsg msg;
  lastMsgSeqNum_ = stDelegator_->uniqueMsgSeqNum();
  msg.msgSeqNum = lastMsgSeqNum_;
  msg.minRelevantCheckpointNum = minRelevantCheckpointNum;
  stDelegator_->onMessage(&msg, sizeof(msg), (targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas);
}

void FakeDestination::sendFetchBlocksMsg(uint64_t firstRequiredBlock, uint64_t lastRequiredBlock) {
  ASSERT_SRC_UNDER_TEST;
  // Remove this line if we would like to make negative tests
  ASSERT_GE(lastRequiredBlock, firstRequiredBlock);
  FetchBlocksMsg msg;
  lastMsgSeqNum_ = stDelegator_->uniqueMsgSeqNum();
  msg.msgSeqNum = lastMsgSeqNum_;
  msg.minBlockId = firstRequiredBlock;  // change here too
  msg.maxBlockId = lastRequiredBlock;
  msg.lastKnownChunkInLastRequiredBlock = 0;  // for now, chunking is not supported
  stDelegator_->onMessage(&msg, sizeof(msg), (targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas);
}

void FakeDestination::sendFetchResPagesMsg(uint64_t lastCheckpointKnownToRequester, uint64_t requiredCheckpointNum) {
  ASSERT_SRC_UNDER_TEST;
  FetchResPagesMsg msg;
  lastMsgSeqNum_ = stDelegator_->uniqueMsgSeqNum();
  msg.msgSeqNum = lastMsgSeqNum_;
  msg.lastCheckpointKnownToRequester = lastCheckpointKnownToRequester;
  msg.requiredCheckpointNum = requiredCheckpointNum;
  msg.lastKnownChunk = 0;  // for now, chunking is not supported
  stDelegator_->onMessage(&msg, sizeof(msg), (targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas);
}

/////////////////////////////////////////////////////////
// FakeSources - definition
/////////////////////////////////////////////////////////
FakeSources::FakeSources(const Config& targetConfig,
                         const TestConfig& testConfig,
                         const TestState& testState,
                         TestReplica& testedReplicaIf,
                         const std::shared_ptr<DataGenerator>& dataGen,
                         std::shared_ptr<BcStTestDelegator>& stAdapter)
    : FakeReplicaBase(targetConfig, testConfig, testState, testedReplicaIf, dataGen, stAdapter) {}

void FakeSources::replyAskForCheckpointSummariesMsg() {
  // We expect a source not fetching. Sending a reject message is not yet supported
  ASSERT_FALSE(datastore_->getIsFetchingState());
  vector<shared_ptr<CheckpointSummaryMsg>> checkpointSummaryReplies;

  // Generate all the blocks until maxBlockId of the last checkpoint - set into appState_
  uint64_t lastBlockId = (testState_.maxRepliedCheckpointNum + 1) * testConfig_.checkpointWindowSize;
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, lastBlockId));

  // Generate checkpoint descriptors - - set into datastore_
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_.get(),
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     rvbm_.get()));

  // build a single copy of all replied messages, push to a vector
  const auto& firstMsg = testedReplicaIf_.sent_messages_.front();
  auto firstAskForCheckpointSummariesMsg = reinterpret_cast<AskForCheckpointSummariesMsg*>(firstMsg.data_.get());
  for (uint64_t i = testState_.maxRepliedCheckpointNum; i >= testState_.minRepliedCheckpointNum; i--) {
    ASSERT_TRUE(datastore_->hasCheckpointDesc(i));
    DataStore::CheckpointDesc desc = datastore_->getCheckpointDesc(i);
    std::shared_ptr<CheckpointSummaryMsg> reply =
        std::shared_ptr<CheckpointSummaryMsg>(CheckpointSummaryMsg::create(desc.rvbData.size()));
    reply->checkpointNum = desc.checkpointNum;
    reply->maxBlockId = desc.maxBlockId;
    reply->digestOfMaxBlockId = desc.digestOfMaxBlockId;
    reply->digestOfResPagesDescriptor = desc.digestOfResPagesDescriptor;
    reply->requestMsgSeqNum = firstAskForCheckpointSummariesMsg->msgSeqNum;
    std::copy(desc.rvbData.begin(), desc.rvbData.end(), reply->data);
    checkpointSummaryReplies.push_back(move(reply));
  }

  // send replies from all replicas (shuffle the requests to get a random reply order)
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(testedReplicaIf_.sent_messages_), std::end(testedReplicaIf_.sent_messages_), rng);
  for (const auto& reply : checkpointSummaryReplies) {
    for (auto& request : testedReplicaIf_.sent_messages_) {
      CheckpointSummaryMsg* uniqueReply = CheckpointSummaryMsg::create(reply.get());
      stDelegator_->onMessage(uniqueReply, uniqueReply->size(), request.to_);
    }
  }
  ASSERT_EQ(clearSentMessagesByMessageType(MsgType::AskForCheckpointSummaries), targetConfig_.numReplicas - 1);
}

void FakeSources::replyFetchBlocksMsg() {
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
  const auto& msg = testedReplicaIf_.sent_messages_.front();
  ASSERT_NFF(assertMsgType(msg, MsgType::FetchBlocks));
  auto fetchBlocksMsg = reinterpret_cast<FetchBlocksMsg*>(msg.data_.get());
  uint64_t nextBlockId = fetchBlocksMsg->maxBlockId;
  size_t numOfSentChunks = 0;

  // For now we assume no chunking is supported
  ConcordAssertEQ(fetchBlocksMsg->lastKnownChunkInLastRequiredBlock, 0);
  ConcordAssertLE(fetchBlocksMsg->minBlockId, fetchBlocksMsg->maxBlockId);
  size_t rvbGroupDigestsExpectedSize =
      (fetchBlocksMsg->rvbGroupid != 0) ? rvbm_->getSerializedByteSizeOfRvbGroup(fetchBlocksMsg->rvbGroupid) : 0;
  while (true) {
    size_t rvbGroupDigestsActualSize{0};
    auto blk = appState_.peekBlock(nextBlockId);
    ItemDataMsg* itemDataMsg = ItemDataMsg::alloc(blk->totalBlockSize + rvbGroupDigestsExpectedSize);
    bool lastInBatch = ((numOfSentChunks + 1) >= targetConfig_.maxNumberOfChunksInBatch) ||
                       ((nextBlockId - 1) < fetchBlocksMsg->minBlockId);
    itemDataMsg->lastInBatch = lastInBatch;
    itemDataMsg->blockNumber = nextBlockId;
    itemDataMsg->totalNumberOfChunksInBlock = 1;
    itemDataMsg->chunkNumber = 1;
    itemDataMsg->requestMsgSeqNum = fetchBlocksMsg->msgSeqNum;

    if (rvbGroupDigestsExpectedSize > 0) {
      // Serialize RVB digests
      rvbGroupDigestsActualSize = rvbm_->getSerializedDigestsOfRvbGroup(
          fetchBlocksMsg->rvbGroupid, itemDataMsg->data, rvbGroupDigestsExpectedSize);
      ConcordAssertLE(rvbGroupDigestsActualSize, rvbGroupDigestsActualSize);
      rvbGroupDigestsExpectedSize = 0;
    }
    itemDataMsg->dataSize = blk->totalBlockSize + rvbGroupDigestsActualSize;
    itemDataMsg->rvbDigestsSize = rvbGroupDigestsActualSize;
    memcpy(itemDataMsg->data + rvbGroupDigestsActualSize, blk.get(), blk->totalBlockSize);
    stDelegator_->onMessage(itemDataMsg, itemDataMsg->size(), msg.to_, std::chrono::steady_clock::now());
    if (lastInBatch) {
      break;
    }
    --nextBlockId;
    ++numOfSentChunks;
  }
  testedReplicaIf_.sent_messages_.pop_front();
}

// To ASSERT_ / EXPECT_  inside this function, we must pass output as a parameter
void FakeSources::replyResPagesMsg(bool& outDoneSending) {
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
  const auto& msg = testedReplicaIf_.sent_messages_.front();
  ASSERT_NFF(assertMsgType(msg, MsgType::FetchResPages));
  auto fetchResPagesMsg = reinterpret_cast<FetchResPagesMsg*>(msg.data_.get());

  // check if need to create a vBlock
  if (!lastReceivedFetchResPagesMsg_ ||
      lastReceivedFetchResPagesMsg_.value().lastCheckpointKnownToRequester !=
          fetchResPagesMsg->lastCheckpointKnownToRequester ||
      lastReceivedFetchResPagesMsg_.value().requiredCheckpointNum != fetchResPagesMsg->requiredCheckpointNum) {
    // need to generate pages - for now, lets assume all pages need to be updated
    uint32_t numberOfUpdatedPages = testConfig_.maxNumberOfUpdatedReservedPages;
    const uint32_t elementSize = BcStTestDelegator::sizeOfElementOfVirtualBlock + targetConfig_.sizeOfReservedPage - 1;
    const uint32_t size = BcStTestDelegator::sizeOfHeaderOfVirtualBlock + numberOfUpdatedPages * elementSize;
    lastReceivedFetchResPagesMsg_ = *fetchResPagesMsg;

    // allocate and fill vBlock
    rawVBlock_ = make_unique<char[]>(size);
    std::fill(rawVBlock_.get(), rawVBlock_.get() + size, 0);
    stDelegator_->fillHeaderOfVirtualBlock(
        rawVBlock_, numberOfUpdatedPages, fetchResPagesMsg->lastCheckpointKnownToRequester);
    char* elements = rawVBlock_.get() + BcStTestDelegator::sizeOfHeaderOfVirtualBlock;
    uint32_t idx = 0;

    DataStore::ResPagesDescriptor* resPagesDesc =
        datastore_->getResPagesDescriptor(fetchResPagesMsg->requiredCheckpointNum);

    for (uint32_t pageId{0}; pageId < numberOfUpdatedPages; ++pageId) {
      ConcordAssertLT(idx, numberOfUpdatedPages);
      stDelegator_->fillElementOfVirtualBlock(datastore_.get(),
                                              elements + idx * elementSize,
                                              pageId,
                                              fetchResPagesMsg->requiredCheckpointNum,
                                              resPagesDesc->d[pageId].pageDigest,
                                              targetConfig_.sizeOfReservedPage);
      idx++;
    }
  }
  ASSERT_TRUE(rawVBlock_.get());
  uint32_t vblockSize = stDelegator_->getSizeOfVirtualBlock(rawVBlock_.get(), targetConfig_.sizeOfReservedPage);
  ASSERT_GE(vblockSize, BcStTestDelegator::sizeOfHeaderOfVirtualBlock);
  ASSERT_TRUE(
      stDelegator_->checkStructureOfVirtualBlock(rawVBlock_.get(), vblockSize, targetConfig_.sizeOfReservedPage, GL));

  // compute information about next chunk
  uint32_t sizeOfLastChunk = targetConfig_.maxChunkSize;
  uint32_t numOfChunksInVBlock = vblockSize / targetConfig_.maxChunkSize;
  uint16_t nextChunk = fetchResPagesMsg->lastKnownChunk + 1;
  if (vblockSize % targetConfig_.maxChunkSize != 0) {
    sizeOfLastChunk = vblockSize % targetConfig_.maxChunkSize;
    numOfChunksInVBlock++;
  }
  // if msg is invalid (because lastKnownChunk+1 does not exist)
  ASSERT_LE(nextChunk, numOfChunksInVBlock);

  // send chunks
  uint16_t numOfSentChunks = 0;
  while (true) {
    uint32_t chunkSize = (nextChunk < numOfChunksInVBlock) ? targetConfig_.maxChunkSize : sizeOfLastChunk;
    ASSERT_GT(chunkSize, 0);

    char* pRawChunk = rawVBlock_.get() + (nextChunk - 1) * targetConfig_.maxChunkSize;
    ItemDataMsg* outMsg = ItemDataMsg::alloc(chunkSize);

    outMsg->requestMsgSeqNum = fetchResPagesMsg->msgSeqNum;
    outMsg->blockNumber = BcStTestDelegator::ID_OF_VBLOCK_RES_PAGES;
    outMsg->totalNumberOfChunksInBlock = numOfChunksInVBlock;
    outMsg->chunkNumber = nextChunk;
    outMsg->dataSize = chunkSize;
    outMsg->lastInBatch =
        ((numOfSentChunks + 1) >= targetConfig_.maxNumberOfChunksInBatch || (nextChunk == numOfChunksInVBlock));
    memcpy(outMsg->data, pRawChunk, chunkSize);

    stDelegator_->onMessage(outMsg, outMsg->size(), msg.to_, std::chrono::steady_clock::now());
    numOfSentChunks++;

    // if we've already sent enough chunks
    if (numOfSentChunks >= targetConfig_.maxNumberOfChunksInBatch) {
      outDoneSending = false;
      break;
    }
    // if we still have chunks in block
    if (nextChunk < numOfChunksInVBlock) {
      nextChunk++;
    } else {  // we sent all chunks
      outDoneSending = true;
      break;
    }
  }  // while
  testedReplicaIf_.sent_messages_.pop_front();
}

/////////////////////////////////////////////////////////
// BcStTest - definition
/////////////////////////////////////////////////////////
void BcStTest::TearDown() {
  if (stateTransfer_) {
    // Must stop running before destruction
    if (stateTransfer_->isRunning()) {
      stateTransfer_->stopRunning();
    }
  }
  if (testConfig_.productDbDeleteOnEnd) deleteBcStateTransferDbFolder(testConfig_.bcstDbPath);
}

// We should call this function after we made all the needed overrides (if needed) for:
// 1) testConfig_
// 2) targetConfig_
void BcStTest::initialize() {
  Block::setMaxTotalBlockSize(targetConfig_.maxBlockSize);
  ASSERT_NFF(configureLog(testConfig_.logLevel));
  // Set starting test state - blocks and checkpoints
  testState_.init(testConfig_, appState_);
  printConfiguration();
  if (testConfig_.productDbDeleteOnStart) deleteBcStateTransferDbFolder(testConfig_.bcstDbPath);
  ASSERT_LE(testConfig_.minNumberOfUpdatedReservedPages, testConfig_.maxNumberOfUpdatedReservedPages);
  // For now we assume no chunking is supported
  ASSERT_EQ(targetConfig_.maxChunkSize, targetConfig_.maxBlockSize);

  datastore_ = createDataStore(testConfig_.bcstDbPath, targetConfig_);
  dataGen_ = make_unique<DataGenerator>(targetConfig_, testConfig_);
  stateTransfer_ = make_unique<BCStateTran>(targetConfig_, &appState_, datastore_);
  stateTransfer_->init(testConfig_.maxNumOfRequiredStoredCheckpoints,
                       testConfig_.numberOfRequiredReservedPages,
                       targetConfig_.sizeOfReservedPage);
  for (uint32_t i{0}; i < testConfig_.numberOfRequiredReservedPages; ++i) {
    stateTransfer_->zeroReservedPage(i);
  }
  stDelegator_ = make_shared<BcStTestDelegator>(stateTransfer_);
  if (testConfig_.testTarget == TestConfig::TestTarget::DESTINATION)
    fakeSrcReplica_ =
        make_unique<FakeSources>(targetConfig_, testConfig_, testState_, testedReplicaIf_, dataGen_, stDelegator_);
  else
    fakeDstReplica_ =
        make_unique<FakeDestination>(targetConfig_, testConfig_, testState_, testedReplicaIf_, dataGen_, stDelegator_);
  initialized_ = true;
}

void BcStTest::dstStartRunningAndCollecting(FetchingState expectedState) {
  LOG_TRACE(GL, "");
  ASSERT_DEST_UNDER_TEST;
  ASSERT_TRUE(initialized_);
  cmnStartRunning(expectedState);
  dstStartCollecting();
}

void BcStTest::cmnStartRunning(FetchingState expectedState) {
  LOG_TRACE(GL, "");
  ASSERT_TRUE(initialized_);
  ASSERT_FALSE(stateTransfer_->isRunning());
  stateTransfer_->startRunning(&testedReplicaIf_);
  ASSERT_TRUE(stateTransfer_->isRunning());
  ASSERT_EQ(expectedState, stateTransfer_->getFetchingState());
}

void BcStTest::dstStartCollecting() {
  LOG_TRACE(GL, "");
  ASSERT_DEST_UNDER_TEST;
  ASSERT_TRUE(initialized_);
  ASSERT_TRUE(stateTransfer_->isRunning());
  stateTransfer_->startCollectingState();
  ASSERT_EQ(FetchingState::GettingCheckpointSummaries, stateTransfer_->getFetchingState());
  auto minRelevantCheckpoint = datastore_->getLastStoredCheckpoint() + 1;
  dstAssertAskForCheckpointSummariesSent(minRelevantCheckpoint);
}

void BcStTest::dstAssertAskForCheckpointSummariesSent(uint64_t checkpoint_num) {
  LOG_TRACE(GL, "");
  ASSERT_DEST_UNDER_TEST;
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), targetConfig_.numReplicas - 1);

  set<uint16_t> dests;
  for (auto& msg : testedReplicaIf_.sent_messages_) {
    auto p = dests.insert(msg.to_);
    ASSERT_TRUE(p.second);  // destinations must be unique
    ASSERT_NFF(assertMsgType(msg, MsgType::AskForCheckpointSummaries));
    auto askMsg = reinterpret_cast<AskForCheckpointSummariesMsg*>(msg.data_.get());
    ASSERT_TRUE(askMsg->msgSeqNum > 0);
    ASSERT_EQ(checkpoint_num, askMsg->minRelevantCheckpointNum);
  }
}

void BcStTest::dstAssertFetchBlocksMsgSent() {
  ASSERT_DEST_UNDER_TEST;

  auto currentSourceId = stDelegator_->getSourceSelector().currentReplica();
  ASSERT_NE(currentSourceId, NO_REPLICA);
  if (uint64_t firstRequiredBlock = datastore_->getFirstRequiredBlock(); firstRequiredBlock == 0) {
    // Get missing blocks is done, make sure St moved to next stage
    ASSERT_EQ(0, datastore_->getLastRequiredBlock());
    ASSERT_EQ(FetchingState::GettingMissingResPages, stateTransfer_->getFetchingState());
  } else {
    // We expect more batches
    ASSERT_EQ(FetchingState::GettingMissingBlocks, stateTransfer_->getFetchingState());
    // TODO - fix
    // if (testState_.minRequiredBlockId < firstRequiredBlock) {
    //   ASSERT_LT(testState_.nextRequiredBlock, stDelegator_->getNextRequiredBlock());
    // } else {
    //   ASSERT_GT(testState_.nextRequiredBlock, stDelegator_->getNextRequiredBlock());
    // }
    ASSERT_EQ(testState_.maxRequiredBlockId, datastore_->getLastRequiredBlock());
    ASSERT_NFF(assertMsgType(testedReplicaIf_.sent_messages_.front(), MsgType::FetchBlocks));
    ASSERT_EQ(testedReplicaIf_.sent_messages_.front().to_, currentSourceId);
  }
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
}

void BcStTest::dstAssertFetchResPagesMsgSent() {
  LOG_TRACE(GL, "");
  ASSERT_DEST_UNDER_TEST;
  ASSERT_EQ(FetchingState::GettingMissingResPages, stateTransfer_->getFetchingState());
  auto currentSourceId = stDelegator_->getSourceSelector().currentReplica();
  ASSERT_NE(currentSourceId, NO_REPLICA);
  ASSERT_EQ(datastore_->getFirstRequiredBlock(), datastore_->getLastRequiredBlock());
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
  ASSERT_NFF(assertMsgType(testedReplicaIf_.sent_messages_.front(), MsgType::FetchResPages));
  ASSERT_EQ(testedReplicaIf_.sent_messages_.front().to_, currentSourceId);
}

void BcStTest::srcAssertCheckpointSummariesSent(uint64_t minRepliedCheckpointNum, uint64_t maxRepliedCheckpointNum) {
  LOG_TRACE(GL, "");
  ASSERT_SRC_UNDER_TEST;
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), maxRepliedCheckpointNum - minRepliedCheckpointNum + 1);
  uint64_t expectedCheckpointNum = maxRepliedCheckpointNum;
  for (const auto& msg : testedReplicaIf_.sent_messages_) {
    ASSERT_NFF(assertMsgType(msg, MsgType::CheckpointsSummary));
    const auto* checkpointSummaryMsg = reinterpret_cast<CheckpointSummaryMsg*>(msg.data_.get());
    ASSERT_EQ(checkpointSummaryMsg->checkpointNum, expectedCheckpointNum);
    ASSERT_EQ(checkpointSummaryMsg->requestMsgSeqNum, fakeDstReplica_->getLastMsgSeqNum());
    ASSERT_EQ(checkpointSummaryMsg->maxBlockId, (expectedCheckpointNum + 1) * testConfig_.checkpointWindowSize);
    // We want to check that messages are sent, here we won't validate the content of the digests.
    ASSERT_TRUE(datastore_->hasCheckpointDesc(checkpointSummaryMsg->checkpointNum));
    DataStore::CheckpointDesc desc = datastore_->getCheckpointDesc(checkpointSummaryMsg->checkpointNum);
    ASSERT_EQ(checkpointSummaryMsg->digestOfMaxBlockId, desc.digestOfMaxBlockId);
    ASSERT_EQ(checkpointSummaryMsg->digestOfResPagesDescriptor, desc.digestOfResPagesDescriptor);
    ASSERT_EQ(checkpointSummaryMsg->sizeofRvbData(), desc.rvbData.size());
    ASSERT_EQ(memcmp(checkpointSummaryMsg->data, desc.rvbData.data(), checkpointSummaryMsg->sizeofRvbData()), 0);
    --expectedCheckpointNum;
  }
}

void BcStTest::srcAssertItemDataMsgBatchSentWithBlocks(uint64_t minExpectedBlockId, uint64_t maxExpectedBlockId) {
  LOG_TRACE(GL, "");
  ASSERT_SRC_UNDER_TEST;
  ASSERT_GE(maxExpectedBlockId, minExpectedBlockId);
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), maxExpectedBlockId - minExpectedBlockId + 1);
  uint64_t currentBlockId = maxExpectedBlockId;  // we expect to get blocks in reverse order, chunking not supported

  ASSERT_TRUE(datastore_->hasCheckpointDesc(testState_.maxRepliedCheckpointNum));
  const DataStore::CheckpointDesc desc = datastore_->getCheckpointDesc(testState_.maxRepliedCheckpointNum);
  ASSERT_EQ(desc.maxBlockId, maxExpectedBlockId);
  for (const auto& msg : testedReplicaIf_.sent_messages_) {
    const auto* itemDataMsg = reinterpret_cast<ItemDataMsg*>(msg.data_.get());
    ASSERT_EQ(1, itemDataMsg->totalNumberOfChunksInBlock);
    ASSERT_EQ(1, itemDataMsg->chunkNumber);
    ASSERT_EQ(itemDataMsg->requestMsgSeqNum, fakeDstReplica_->getLastMsgSeqNum());
    ASSERT_EQ(currentBlockId == minExpectedBlockId, (bool)itemDataMsg->lastInBatch);
    const auto blk = appState_.peekBlock(currentBlockId);
    ASSERT_TRUE(blk);
    ASSERT_EQ(blk->blockId, currentBlockId);
    // just compare the blocks, dont validate digests.
    if (itemDataMsg->rvbDigestsSize > 0) {
      ASSERT_GT(itemDataMsg->dataSize, itemDataMsg->rvbDigestsSize);
      ASSERT_EQ(blk->totalBlockSize, itemDataMsg->dataSize - itemDataMsg->rvbDigestsSize);
      ASSERT_EQ(memcmp(reinterpret_cast<char*>(blk.get()),
                       itemDataMsg->data + itemDataMsg->rvbDigestsSize,
                       itemDataMsg->dataSize - itemDataMsg->rvbDigestsSize),
                0);
      // TODO - add here check for the RVB data. Need to get RVB group id from fake dest?
    } else {
      ASSERT_EQ(blk->totalBlockSize, itemDataMsg->dataSize);
      ASSERT_EQ(memcmp(reinterpret_cast<char*>(blk.get()), itemDataMsg->data, itemDataMsg->dataSize), 0);
    }
    --currentBlockId;
  }
}

void BcStTest::srcAssertItemDataMsgBatchSentWithResPages(uint32_t expectedChunksSent, uint64_t requiredCheckpointNum) {
  LOG_TRACE(GL, "");
  ASSERT_SRC_UNDER_TEST;
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), expectedChunksSent);
  const auto& msg = testedReplicaIf_.sent_messages_.front();
  ASSERT_NFF(assertMsgType(msg, MsgType::ItemData));
  auto* itemDataMsg = reinterpret_cast<ItemDataMsg*>(msg.data_.get());
  ASSERT_EQ(itemDataMsg->requestMsgSeqNum, fakeDstReplica_->getLastMsgSeqNum());
  ASSERT_EQ(itemDataMsg->blockNumber, stDelegator_->ID_OF_VBLOCK_RES_PAGES);
  ASSERT_EQ(itemDataMsg->totalNumberOfChunksInBlock, 1);
  ASSERT_EQ(itemDataMsg->chunkNumber, 1);
  ASSERT_TRUE((bool)itemDataMsg->lastInBatch);
  // Comment: It is too complicated to validate the reserved pages descriptor.
  // Infra does not support it currently in a fake destination.
}

void BcStTest::printConfiguration() {
  LOG_INFO(GL, "testConfig_:" << std::boolalpha << testConfig_);
  LOG_INFO(GL, "targetConfig_:" << std::boolalpha << targetConfig_);
  LOG_INFO(GL, "testState_:" << std::boolalpha << testState_);
}

void BcStTest::configureLog(const string& logLevelStr) {
  std::set<string> possibleLogLevels = {"trace", "debug", "info", "warn", "error", "fatal"};
  ASSERT_TRUE(possibleLogLevels.find(logLevelStr) != possibleLogLevels.end());
#ifdef USE_LOG4CPP
  log4cplus::LogLevel logLevel =
      logLevelStr == "trace"
          ? log4cplus::TRACE_LOG_LEVEL
          : logLevelStr == "debug"
                ? log4cplus::DEBUG_LOG_LEVEL
                : logLevelStr == "info"
                      ? log4cplus::INFO_LOG_LEVEL
                      : logLevelStr == "warn"
                            ? log4cplus::WARN_LOG_LEVEL
                            : logLevelStr == "error"
                                  ? log4cplus::ERROR_LOG_LEVEL
                                  : logLevelStr == "fatal" ? log4cplus::FATAL_LOG_LEVEL : log4cplus::INFO_LOG_LEVEL;
#else
  logging::LogLevel logLevel =
      logLevelStr == "trace"
          ? logging::LogLevel::trace
          : logLevelStr == "debug"
                ? logging::LogLevel::debug
                : logLevelStr == "info"
                      ? logging::LogLevel::info
                      : logLevelStr == "warn"
                            ? logging::LogLevel::warn
                            : logLevelStr == "error"
                                  ? logging::LogLevel::error
                                  : logLevelStr == "fatal" ? logging::LogLevel::fatal : logging::LogLevel::info;
#endif
  // logging::Logger::getInstance("serializable").setLogLevel(logLevel);
  // logging::Logger::getInstance("concord.bft.st.dbdatastore").setLogLevel(logLevel);
  // logging::Logger::getInstance("rocksdb").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft.st.dst").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft.st.src").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.util.handoff").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft.st.rvb").setLogLevel(logLevel);
}

void BcStTest::compareAppStateblocks(uint64_t minBlockId, uint64_t maxBlockId) const {
  const auto& srcAppState = fakeSrcReplica_->getAppState();
  for (size_t i = minBlockId; i <= maxBlockId; ++i) {
    const auto b1 = appState_.peekBlock(i);
    const auto b2 = srcAppState.peekBlock(i);
    ASSERT_TRUE(b1);
    ASSERT_TRUE(b2);
    ASSERT_EQ(b1->totalBlockSize, b2->totalBlockSize);
    ASSERT_EQ(memcmp(b1.get(), b2.get(), b2->totalBlockSize), 0);
  }
}

void BcStTest::validateSourceSelectorMetricCounters(const MetricKeyValPairs& metricCounters) {
  ASSERT_EQ(metricCounters.size(), testConfig_.numExpectedSourceSelectorMetricCounters);
  for (const auto& p : metricCounters) {
    stDelegator_->assertSourceSelectorMetricKeyVal(p.first, p.second);
  }
}

void BcStTest::dstRestart(bool productDbDeleteOnEnd, FetchingState expectedState) {
  stateTransfer_->stopRunning();
  stateTransfer_.reset(nullptr);
  testedReplicaIf_.sent_messages_.clear();
  testConfig_.productDbDeleteOnStart = false;
  testConfig_.productDbDeleteOnEnd = productDbDeleteOnEnd;
  datastore_ = createDataStore(testConfig_.bcstDbPath, targetConfig_);
  stateTransfer_ = make_unique<BCStateTran>(targetConfig_, &appState_, datastore_);
  stateTransfer_->init(testConfig_.maxNumOfRequiredStoredCheckpoints,
                       testConfig_.numberOfRequiredReservedPages,
                       targetConfig_.sizeOfReservedPage);
  cmnStartRunning(expectedState);
  stDelegator_->onTimerImp();
}

void BcStTest::dstRestartWithIterations(std::set<size_t>& execOnIterations, FetchingState expectedState) {
  static size_t iteration{1};
  auto iter = execOnIterations.find(iteration);
  if (iter != execOnIterations.end()) {
    execOnIterations.erase(iteration);
    dstRestart(execOnIterations.empty(), expectedState);
  }
  ++iteration;
}

template <class R, class... Args>
void BcStTest::getMissingblocksStage(std::function<R(Args...)> callAtStart, std::function<R(Args...)> callAtEnd) {
  testState_.nextRequiredBlock = stDelegator_->getNextRequiredBlock();
  while (true) {
    if (callAtStart) callAtStart();
    ASSERT_NFF(fakeSrcReplica_->replyFetchBlocksMsg());
    // There might be pending jobs for putBlock, we need to wait some time and then finalize them by calling
    this_thread::sleep_for(chrono::milliseconds(20));
    stDelegator_->onTimerImp();
    ASSERT_NFF(dstAssertFetchBlocksMsgSent());
    if (datastore_->getFirstRequiredBlock() == 0) {
      break;
    }
    testState_.minRequiredBlockId = datastore_->getFirstRequiredBlock();
    testState_.nextRequiredBlock = stDelegator_->getNextRequiredBlock();
    if (callAtEnd) callAtEnd();
  }
}

void BcStTest::getReservedPagesStage() {
  ASSERT_NFF(dstAssertFetchResPagesMsgSent());
  for (bool doneSending = false; !doneSending;) {
    ASSERT_NFF(fakeSrcReplica_->replyResPagesMsg(doneSending));
  }
}

/////////////////////////////////////////////////////////
//
//       BcStTest Destination Test Cases
//
/////////////////////////////////////////////////////////

class BcStTestParamFixture1 : public BcStTest,
                              public testing::WithParamInterface<tuple<uint32_t, uint32_t, uint32_t>> {};

// Validate a full state transfer
// This is a parameterized test case, see BcStTestParamFixtureInput for all possible inputs
TEST_P(BcStTestParamFixture1, dstFullStateTransfer) {
  targetConfig_.maxNumberOfChunksInBatch = get<0>(GetParam());
  targetConfig_.fetchRangeSize = get<1>(GetParam());
  targetConfig_.RVT_K = get<2>(GetParam());
  ASSERT_NFF(initialize());
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  ASSERT_NFF(getMissingblocksStage<void>());
  ASSERT_NFF(getReservedPagesStage());
  // now validate completion
  ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
}

// 1st element - maxNumberOfChunksInBatch
// 2nd element - fetchRangeSize
// 3rd element - RVT_K
// The comma at the end is due to a bug in gtest 3.09 - https://github.com/google/googletest/issues/2271 - see last
using BcStTestParamFixtureInput = tuple<uint32_t, uint32_t, uint32_t>;
INSTANTIATE_TEST_CASE_P(BcStTest,
                        BcStTestParamFixture1,
                        ::testing::Values(
                            // BcStTestParamFixtureInput(128, 256),    // not supported for now
                            // BcStTestParamFixtureInput(100, 256),    // not supported for now
                            // BcStTestParamFixtureInput(512, 2048),   // not supported for now
                            // BcStTestParamFixtureInput(128, 1024),   // not supported for now
                            BcStTestParamFixtureInput(128, 16, 16),
                            BcStTestParamFixtureInput(64, 16, 1024),
                            BcStTestParamFixtureInput(128, 16, 16),
                            BcStTestParamFixtureInput(64, 16, 32),
                            BcStTestParamFixtureInput(128, 128, 1024),
                            BcStTestParamFixtureInput(256, 128, 16),
                            BcStTestParamFixtureInput(256, 100, 1024),
                            BcStTestParamFixtureInput(1024, 128, 16),
                            BcStTestParamFixtureInput(2048, 512, 1024)), );

class BcStTestParamFixture2 : public BcStTest, public testing::WithParamInterface<tuple<bool, uint8_t>> {};

// Validate that the source selector's primary awareness mechanism can be toggled on and off
TEST_P(BcStTestParamFixture2, dstSourceSelectorPrimaryAwareness) {
  auto [enable_primary_awareness, number_of_replacements] = GetParam();
  targetConfig_.enableSourceSelectorPrimaryAwareness = enable_primary_awareness;
  ASSERT_NFF(initialize());
  std::once_flag once_flag;
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  auto ss = stDelegator_->getSourceSelector();
  const std::function<void(void)> trigger_source_change = [&]() {
    std::call_once(once_flag, [&] {
      std::unique_ptr<MessageBase> msg;
      // Generate prePrepare messages to trigger source seletor to change the source to avoid primary.
      ASSERT_NFF(msg = dataGen_->generatePrePrepareMsg(ss.currentReplica()));
      for (uint16_t i = 1; i <= targetConfig_.minPrePrepareMsgsForPrimaryAwarness; i++) {
        auto cmsg = make_shared<ConsensusMsg>(msg->type(), msg->senderId());
        stateTransfer_->peekConsensusMessage(cmsg);
      }
    });
  };
  ASSERT_NFF(getMissingblocksStage(EMPTY_FUNC, trigger_source_change));
  const auto& sources = stDelegator_->getSourceSelector().getActualSources();
  ASSERT_EQ(sources.size(), number_of_replacements);

  validateSourceSelectorMetricCounters({{"total_replacements_", number_of_replacements},
                                        {"replacement_due_to_no_source_", 1},
                                        {"replacement_due_to_source_same_as_primary_", number_of_replacements - 1},
                                        {"replacement_due_to_bad_data_", 0},
                                        {"replacement_due_to_periodic_change_", 0},
                                        {"replacement_due_to_retransmission_timeout_", 0}});

  ASSERT_NFF(getReservedPagesStage());
  // validate completion
  ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

// 1st element - enable source selector primary awareness
// 2nd element - the number of source replacements
using BcStTestParamFixtureInput2 = tuple<bool, uint8_t>;
INSTANTIATE_TEST_CASE_P(BcStTest,
                        BcStTestParamFixture2,
                        ::testing::Values(BcStTestParamFixtureInput2(true, 2), BcStTestParamFixtureInput2(false, 1)), );
/**
 * Check that only actual resources are inserted into source selector's actualSources_
 * This is done by triggering multiple retransmissions and then  source replacements, and checking that only the sources
 * which replied are in the list, and in the expected order.
 * The check is done only for FetchingMissingblocks state sources.
 */
TEST_F(BcStTest, dstValidateRealSourceListReported) {
  ASSERT_NFF(initialize());
  uint16_t currentSrc;
  /**
   * Add callback to ST to be executed when transferring is completed.
   * Here we validate that only one actual source is in the sources list, although we had multiple
   * retransmissions and a few sources were selected.
   */
  stateTransfer_->addOnTransferringCompleteCallback([this, &currentSrc](std::uint64_t) {
    const auto& sources_ = stDelegator_->getSourceSelector().getActualSources();
    ASSERT_EQ(sources_.size(), 1);
    ASSERT_EQ(sources_[0], currentSrc);
  });

  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());

  // Trigger multiple retransmissions to 2 sources. none will be answered, then we expect the replica to move into the
  // 3rd source
  auto& sourceSelector = stDelegator_->getSourceSelector();
  set<uint16_t> sources;
  for (uint32_t i{0}; i < 2; ++i) {
    for (uint32_t j{0}; j < targetConfig_.maxFetchRetransmissions; ++j) {
      if (j == 0) {
        currentSrc = sourceSelector.currentReplica();
        auto result = sources.insert(currentSrc);
        ASSERT_TRUE(result.second);
      } else {
        ASSERT_EQ(currentSrc, sourceSelector.currentReplica());
      }
      ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
      ASSERT_EQ(testedReplicaIf_.sent_messages_.front().to_, currentSrc);
      testedReplicaIf_.sent_messages_.clear();
      this_thread::sleep_for(chrono::milliseconds(targetConfig_.fetchRetransmissionTimeoutMs + 10));
      stDelegator_->onTimerImp();
    }
  }
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
  currentSrc = sourceSelector.currentReplica();
  ASSERT_NFF(getMissingblocksStage<void>());
  ASSERT_NFF(getReservedPagesStage());
  // now validate completion
  ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

// Validate a recurring source selection, during ongoing state transfer;
TEST_F(BcStTest, dstValidatePeriodicSourceReplacement) {
  targetConfig_.sourceReplicaReplacementTimeoutMs = 2000;
  ASSERT_NFF(initialize());
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  uint32_t batch_count{0};
  auto const delay_periodically = std::function<void(void)>([&]() {
    if (batch_count < 2) {
      this_thread::sleep_for(milliseconds(targetConfig_.sourceReplicaReplacementTimeoutMs + 10));
    }
  });
  auto const increase_batches = std::function<void(void)>([&]() { batch_count++; });
  ASSERT_NFF(getMissingblocksStage(delay_periodically, increase_batches));
  const auto& actualSources_ = stDelegator_->getSourceSelector().getActualSources();
  ASSERT_GT(actualSources_.size(), 1);
  validateSourceSelectorMetricCounters({{"total_replacements_", actualSources_.size()},
                                        {"replacement_due_to_no_source_", 1},
                                        {"replacement_due_to_source_same_as_primary_", 0},
                                        {"replacement_due_to_periodic_change_", actualSources_.size() - 1},
                                        {"replacement_due_to_retransmission_timeout_", 0},
                                        {"replacement_due_to_bad_data_", 0}});
  ASSERT_NFF(getReservedPagesStage());
  ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

// TBD BC-14432
TEST_F(BcStTest, dstSendPrePrepareMsgsDuringStateTransfer) {
  ASSERT_NFF(initialize());
  std::once_flag once_flag;
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  auto ss = stDelegator_->getSourceSelector();
  const std::function<void(void)> trigger_source_change = [&]() {
    std::call_once(once_flag, [&] {
      std::unique_ptr<MessageBase> msg;
      // Generate prePrepare messages to trigger source seletor to change the source to avoid primary.
      ASSERT_NFF(msg = dataGen_->generatePrePrepareMsg(ss.currentReplica()));
      for (uint16_t i = 1; i <= targetConfig_.minPrePrepareMsgsForPrimaryAwarness; i++) {
        auto cmsg = make_shared<ConsensusMsg>(msg->type(), msg->senderId());
        stateTransfer_->peekConsensusMessage(cmsg);
      }
    });
  };
  ASSERT_NFF(getMissingblocksStage(EMPTY_FUNC, trigger_source_change));
  const auto& sources = stDelegator_->getSourceSelector().getActualSources();
  // TBD metric counters in source selector should be used to validate changed sources to avoid primary
  ASSERT_EQ(sources.size(), 2);
  validateSourceSelectorMetricCounters({{"total_replacements_", 2},
                                        {"replacement_due_to_no_source_", 1},
                                        {"replacement_due_to_source_same_as_primary_", 1},
                                        {"replacement_due_to_periodic_change_", 0},
                                        {"replacement_due_to_retransmission_timeout_", 0},
                                        {"replacement_due_to_bad_data_", 0}});
  ASSERT_NFF(getReservedPagesStage());
  // validate completion
  ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

TEST_F(BcStTest, dstPreprepareFromMultipleSourcesDuringStateTransfer) {
  ASSERT_NFF(initialize());
  std::once_flag once_flag;
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  auto ss = stDelegator_->getSourceSelector();
  const std::function<void(void)> generate_preprepare_messages = [&]() {
    std::call_once(once_flag, [&] {
      std::unique_ptr<MessageBase> msg;
      // Generate enough prePrepare messages but from more than one source so that source does not get changed
      ASSERT_NFF(msg = dataGen_->generatePrePrepareMsg(ss.currentReplica()));
      for (uint16_t i = 1; i <= targetConfig_.minPrePrepareMsgsForPrimaryAwarness - 1; i++) {
        auto cmsg = make_shared<ConsensusMsg>(msg->type(), msg->senderId());
        stateTransfer_->peekConsensusMessage(cmsg);
      }
      auto cmsg = make_shared<ConsensusMsg>(msg->type(), (msg->senderId() + 1) % targetConfig_.numReplicas);
      stateTransfer_->peekConsensusMessage(cmsg);
    });
  };
  ASSERT_NFF(getMissingblocksStage(EMPTY_FUNC, generate_preprepare_messages));
  const auto& sources = stDelegator_->getSourceSelector().getActualSources();
  // TBD metric counters in source selector should be used to validate changed sources to avoid primary
  ASSERT_EQ(sources.size(), 1);
  validateSourceSelectorMetricCounters({{"total_replacements_", 1},
                                        {"replacement_due_to_no_source_", 1},
                                        {"replacement_due_to_source_same_as_primary_", 0},
                                        {"replacement_due_to_periodic_change_", 0},
                                        {"replacement_due_to_retransmission_timeout_", 0},
                                        {"replacement_due_to_bad_data_", 0}});
  ASSERT_NFF(getReservedPagesStage());
  // validate completion
  ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

// Run a full state transfer with 3 cycles
TEST_F(BcStTest, dstFullStateTransferMultipleCycles) {
  vector<float> nextcycleSizeMultiplier{0.5, 0.25};  // How larger/smaller is the next cycle from the previous one
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());

  for (size_t i{0}; i < nextcycleSizeMultiplier.size() + 1; ++i) {
    ASSERT_NFF(dstStartCollecting());
    ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
    ASSERT_NFF(getMissingblocksStage<void>());
    ASSERT_NFF(getReservedPagesStage());
    // now validate completion
    ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
    ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
    if (i != nextcycleSizeMultiplier.size())
      testState_.moveToNextCycle(testConfig_,
                                 appState_,
                                 testState_.maxRequiredBlockId + 1,
                                 testConfig_.lastReachedConsensusCheckpointNum +
                                     testConfig_.lastReachedConsensusCheckpointNum * nextcycleSizeMultiplier[i]);
  }
}

// Run ST while restarting multiple times during blocks collection stage
TEST_F(BcStTest, dstFullStateTransferWithRestarts) {
  ASSERT_NFF(initialize());
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  // Restart on 3 batches during collection
  std::set<size_t> execOnIterations{3, 5, 7};
  const std::function<void(void)> restart_on_specific_iterations = [&]() {
    dstRestartWithIterations(execOnIterations, FetchingState::GettingMissingBlocks);
  };
  ASSERT_NFF(getMissingblocksStage<void>(restart_on_specific_iterations, EMPTY_FUNC));
  ASSERT_NFF(getReservedPagesStage());
  // now validate completion
  ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
  ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

/////////////////////////////////////////////////////////
//
//       BcStTest Source Test Cases
//
/////////////////////////////////////////////////////////

TEST_F(BcStTest, srcHandleAskForCheckpointSummariesMsg) {
  testConfig_.testTarget = TestConfig::TestTarget::SOURCE;
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  // Generate the data needed for a tested ST backup replica
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));
  // Fake ask for checkpoint summaries
  fakeDstReplica_->sendAskForCheckpointSummariesMsg(testState_.lastCheckpointKnownToRequester);
  // Validate response from tested ST backup replica
  ASSERT_NFF(srcAssertCheckpointSummariesSent(testState_.minRepliedCheckpointNum, testState_.maxRepliedCheckpointNum));
}

TEST_F(BcStTest, srcHandleFetchBlocksMsg) {
  testConfig_.testTarget = TestConfig::TestTarget::SOURCE;
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  // Generate the data needed for a tested ST backup replica
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));
  ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg(testState_.minRequiredBlockId, testState_.maxRequiredBlockId));
  uint64_t minExpectedBlockId = (testState_.numBlocksToCollect > targetConfig_.maxNumberOfChunksInBatch)
                                    ? (testState_.maxRequiredBlockId - targetConfig_.maxNumberOfChunksInBatch + 1)
                                    : testState_.minRequiredBlockId;
  ASSERT_NFF(srcAssertItemDataMsgBatchSentWithBlocks(minExpectedBlockId, testState_.maxRequiredBlockId));
}

TEST_F(BcStTest, srcHandleFetchResPagesMsg) {
  testConfig_.testTarget = TestConfig::TestTarget::SOURCE;
  // we want to make sure size of vBlock will enter a single chunk
  testConfig_.maxNumberOfUpdatedReservedPages =
      targetConfig_.maxNumberOfChunksInBatch /
      (targetConfig_.sizeOfReservedPage / targetConfig_.maxBlockSize + 1);  // approx calculation
  targetConfig_.maxBlockSize = std::max(
      targetConfig_.sizeOfReservedPage + testConfig_.maxNumberOfUpdatedReservedPages * targetConfig_.sizeOfReservedPage,
      targetConfig_.maxBlockSize);
  targetConfig_.maxChunkSize = targetConfig_.maxBlockSize;
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  // Generate the data needed for a tested ST backup replica
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));
  ASSERT_NFF(fakeDstReplica_->sendFetchResPagesMsg(testState_.lastCheckpointKnownToRequester,
                                                   testState_.maxRepliedCheckpointNum));
  ASSERT_NFF(srcAssertItemDataMsgBatchSentWithResPages(1, testState_.maxRepliedCheckpointNum));
}

/////////////////////////////////////////////////////////
//
//       BcStTest Backup Replica (Initialization, Checkpointing)
//
/////////////////////////////////////////////////////////

// Check that a backup replica save and load checkpoints, in particular the RVT as part of the CP
TEST_F(BcStTest, bkpCheckCheckpointsPersistency) {
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));
  auto rvt = stDelegator_->getRvt();
  auto h1 = rvt->getRootHashVal();
  ASSERT_NFF(dstRestart(false, FetchingState::NotFetching));
  rvt = stDelegator_->getRvt();
  auto h2 = rvt->getRootHashVal();
  ASSERT_EQ(h1, h2);
  testConfig_.productDbDeleteOnEnd = true;
}

// Check that a backup replica save and load pruned block digests which were not yet added to the RVT
TEST_F(BcStTest, bkpCheckCheckPruningPersistency) {
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));
  uint64_t midBlockId =
      testState_.maxRequiredBlockId - ((testState_.maxRequiredBlockId - appState_.getGenesisBlockNum() + 1) / 2);
  ASSERT_GT(midBlockId, appState_.getGenesisBlockNum() + 1);
  ASSERT_LT(midBlockId, testState_.maxRequiredBlockId);
  auto rvbm = stDelegator_->getRvbManager();
  rvbm->reportLastAgreedPrunableBlockId(midBlockId);
  const auto digestsBefore = stDelegator_->getPrunedBlocksDigests();
  ASSERT_NFF(dstRestart(false, FetchingState::NotFetching));
  const auto digestsafter = stDelegator_->getPrunedBlocksDigests();
  ASSERT_EQ(digestsBefore, digestsafter);
  testConfig_.productDbDeleteOnEnd = true;
}

// Check inter-versions compitability: period to version 1.6 there is no RVT data in checkpoint.
// We would like to check that replica is able to reconstruct the whole RVT from storage, when no data is found in
// Checkpoint
TEST_F(BcStTest, bkpCheckRvbDataReconstructionFromStorageInterVersion) {
  // do not store RVB data in checkpoints (simulate v1.5)
  targetConfig_.enableStoreRvbDataDuringCheckpointing = false;
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));
  stDelegator_->createCheckpointOfCurrentState(datastore_->getLastStoredCheckpoint() + 1);
  auto rvt = stDelegator_->getRvt();
  auto h1 = rvt->getRootHashVal();

  // Now enable keeping RVB data
  targetConfig_.enableStoreRvbDataDuringCheckpointing = false;
  ASSERT_NFF(dstRestart(false, FetchingState::NotFetching));
  rvt = stDelegator_->getRvt();
  auto h2 = rvt->getRootHashVal();
  ASSERT_EQ(h1, h2);
  testConfig_.productDbDeleteOnEnd = true;
}

// ASSERT_NFF(initialize());
// ASSERT_NFF(dstStartRunningAndCollecting());
// ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
// // Restart on 3 batches during collection
// std::set<size_t> execOnIterations{3, 5, 7};
// const std::function<void(void)> restart_on_specific_iterations = [&]() {
//   dstRestartWithIterations(execOnIterations, FetchingState::GettingMissingBlocks);
// };
// ASSERT_NFF(getMissingblocksStage<void>(restart_on_specific_iterations, EMPTY_FUNC));
// ASSERT_NFF(getReservedPagesStage());
// // now validate completion
// ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
// ASSERT_EQ(FetchingState::NotFetching, stateTransfer_->getFetchingState());

}  // namespace bftEngine::bcst::impl

int main(int argc, char** argv) {
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_death_test_style =
      "threadsafe";  // mitigate the risks of testing in a possibly multithreaded environment
  return RUN_ALL_TESTS();
}
