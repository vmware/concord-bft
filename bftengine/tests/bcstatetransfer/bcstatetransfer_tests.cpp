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
#include <cstring>
#include <algorithm>

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
#include "Messages.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "hex_tools.h"
#include "RVBManager.hpp"
#include "RangeValidationTree.hpp"
#include "messages/StateTransferMsg.hpp"

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
using concord::storage::rocksdb::Client;
using concord::storage::rocksdb::KeyComparator;
#endif

using namespace std;
using namespace bftEngine::bcst;
using namespace concord::util;

using std::chrono::milliseconds;
using random_bytes_engine = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char>;

#define ASSERT_NFF ASSERT_NO_FATAL_FAILURE
#define ASSERT_DEST_UNDER_TEST ASSERT_TRUE(testConfig_.testTarget == TestConfig::TestTarget::DESTINATION)
#define ASSERT_SRC_UNDER_TEST ASSERT_TRUE(testConfig_.testTarget == TestConfig::TestTarget::SOURCE)
#define EMPTY_FUNC            \
  std::function<void(void)> { \
    []() {}                   \
  }

// Extract user input from command line to override configuration and avoid the need for re-compilation for some of
// the configuration parameters
struct UserInput {
  std::string loglevel_ = "";
  const set<std::string> expectedInputArgs{"--log_level"};
  static UserInput* getInstance() {
    static UserInput inputConfig;
    return &inputConfig;
  }

  void extractUserInput(int argc, char** argv) {
    vector<string> tokens;

    for (size_t i{1}; i < argc; ++i) {
      std::string s(argv[i]);
      char* token = std::strtok(const_cast<char*>(s.c_str()), "= ");
      while (token) {
        tokens.push_back(token);
        token = std::strtok(nullptr, "= ");
      }
    }

    set<std::string>::iterator iter = expectedInputArgs.end();
    for (const auto& tok : tokens) {
      if (iter == expectedInputArgs.end()) {
        iter = std::find(expectedInputArgs.begin(), expectedInputArgs.end(), tok);
      } else {
        if (*iter == std::string("--log_level")) {
          loglevel_ = tok;
          iter = expectedInputArgs.end();
        }
      }
    }
  }
};

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
      24,                 // maxNumberOfChunksInBatch
      1024,               // maxBlockSize
      256 * 1024 * 1024,  // maxPendingDataFromSourceReplica
      2048,               // maxNumOfReservedPages
      4096,               // sizeOfReservedPage
      600,                // gettingMissingBlocksSummaryWindowSize
      10,                 // minPrePrepareMsgsForPrimaryAwareness
      24,                 // fetchRangeSize
      6,                  // RVT_K
      300,                // refreshTimerMs
      2500,               // checkpointSummariesRetransmissionTimeoutMs
      60000,              // maxAcceptableMsgDelayMs
      0,                  // sourceReplicaReplacementTimeoutMs
      2000,               // fetchRetransmissionTimeoutMs
      2,                  // maxFetchRetransmissions
      5,                  // metricsDumpIntervalSec
      5000,               // maxTimeSinceLastExecutionInMainWindowMs
      2050,               // sourceSessionExpiryDurationMs
      3,                  // sourcePerformanceSnapshotFrequencySec
      false,              // runInSeparateThread
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
   * You may decide a constant is configurable by moving it into the 'Configurable' part
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
// TestUtils
//
// Group of utility functions which can be used by any other class
/////////////////////////////////////////////////////////
class TestUtils {
 public:
  static void mallocCopy(void* inputBuff, size_t numBytes, char** outputBuff);
  static void allocCopyStateTransferMsg(void* inputBuff, size_t numBytes, char** outputBuff);
};

void TestUtils::mallocCopy(void* inputBuff, size_t numBytes, char** outputBuff) {
  ASSERT_GT(numBytes, 0);
  *outputBuff = static_cast<char*>(std::malloc(numBytes));
  ASSERT_TRUE(*outputBuff);
  memcpy(*outputBuff, inputBuff, numBytes);
}

void TestUtils::allocCopyStateTransferMsg(void* inputBuff, size_t numBytes, char** outputBuff) {
  // We don't allocate real MessageBase::Header, only the body. This is done in order to be sure that the right
  // call to deallocate the is done from target code
  ASSERT_GT(numBytes, 0);
  char* body = static_cast<char*>(std::malloc(numBytes + sizeof(MessageBase::Header)));
  ASSERT_TRUE(body);
  *outputBuff = body + sizeof(MessageBase::Header);
  memcpy(*outputBuff, inputBuff, numBytes);
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

  void assertBCStateTranMetricKeyVal(const std::string& key, uint64_t val);
  uint64_t uniqueMsgSeqNum() { return stateTransfer_->uniqueMsgSeqNum(); }

  void handleStateTransferMessage(char* msg,
                                  uint32_t msgLen,
                                  uint16_t senderId,
                                  time_point<steady_clock> incomingEventsQPushTime = steady_clock::now()) {
    stateTransfer_->handleStateTransferMessageImpl(msg, msgLen, senderId, incomingEventsQPushTime);
  }
  uint64_t getNextRequiredBlock() { return stateTransfer_->fetchState_.nextBlockId; }
  RVBManager* getRvbManager() { return stateTransfer_->rvbm_.get(); }
  size_t getSizeOfRvbDigestInfo() const { return sizeof(RVBManager::RvbDigestInfo); }
  SimpleMemoryPool<BCStateTran::BlockIOContext>& getIoPool() const { return stateTransfer_->ioPool_; }
  std::deque<BCStateTran::BlockIOContextPtr>& getIoContexts() const { return stateTransfer_->ioContexts_; }
  void clearIoContexts() { stateTransfer_->clearIoContexts(); }
  RVBId nextRvbBlockId(BlockId blockId) const { return stateTransfer_->rvbm_->nextRvbBlockId(blockId); }
  RVBId prevRvbBlockId(BlockId blockId) const { return stateTransfer_->rvbm_->prevRvbBlockId(blockId); }
  RangeValidationTree* getRvt() { return stateTransfer_->rvbm_->in_mem_rvt_.get(); }
  void createCheckpointOfCurrentState(uint64_t checkpointNum) {
    stateTransfer_->createCheckpointOfCurrentState(checkpointNum);
  };
  void deleteOldCheckpoints(uint64_t checkpointNumber, DataStoreTransaction* txn) {
    stateTransfer_->deleteOldCheckpoints(checkpointNumber, txn);
  }
  std::vector<std::pair<BlockId, Digest>> getPrunedBlocksDigests() {
    return stateTransfer_->rvbm_->pruned_blocks_digests_;
  }
  void fillHeaderOfVirtualBlock(std::unique_ptr<char[]>& rawVBlock,
                                uint32_t numberOfUpdatedPages,
                                uint64_t lastCheckpointKnownToRequester);
  void fillElementOfVirtualBlock(DataStore* datastore,
                                 char* position,
                                 uint32_t pageId,
                                 uint64_t checkpointNumber,
                                 const Digest& pageDigest,
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
  const Digest& computeDefaultRvbDataDigest() const { return stateTransfer_->computeDefaultRvbDataDigest(); }
  void enableFetchingState(void) {
    DataStoreTransaction::Guard g(stateTransfer_->psd_->beginTransaction());
    g.txn()->setIsFetchingState(true);
  }

  // Source Selector
  void assertSourceSelectorMetricKeyVal(const std::string& key, uint64_t val);
  SourceSelector& getSourceSelector() { return stateTransfer_->sourceSelector_; }
  const std::set<uint16_t>& getPreferredReplicas() { return stateTransfer_->sourceSelector_.preferredReplicas_; }
  void validateEqualRVTs(const RangeValidationTree& rvtA, const RangeValidationTree& rvtB) const;
  FetchingState getFetchingState() { return stateTransfer_->getFetchingState(); }
  bool isSrcSessionOpen() const { return stateTransfer_->sourceSession_.isOpen(); }
  uint16_t srcSessionOwnerDestReplicaId() const { return stateTransfer_->sourceSession_.ownerDestReplicaId(); }

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
                  std::shared_ptr<BcStTestDelegator>& testAdapter,
                  BCStateTran* peerStateTransfer);
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
  BCStateTran* peerStateTransfer_;
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
                  std::shared_ptr<BcStTestDelegator>& stAdapter,
                  BCStateTran* srcStateTransfer)
      : FakeReplicaBase(targetConfig, testConfig, testState, testedReplicaIf, dataGen, stAdapter, srcStateTransfer) {}
  ~FakeDestination() {}
  void sendAskForCheckpointSummariesMsg(uint64_t minRelevantCheckpointNum, uint16_t senderReplicaId = UINT_LEAST16_MAX);
  static constexpr uint16_t kDefaultSenderReplicaId = UINT_LEAST16_MAX;

  template <class R, class... Args>
  void sendFetchBlocksMsg(uint64_t minBlockId,
                          uint64_t maxBlockIdInCycle,
                          // when 0 , (minBlockId + targetConfig_.maxNumberOfChunksInBatch -1) is assigned
                          uint64_t maxBlockId = 0,
                          // when 0 , targetConfig_.maxNumberOfChunksInBatch is assigned
                          size_t numExpectedItemDataMsgsInReply = 0,
                          // when kDefaultSenderReplicaId , just adding 1 to targetConfig_.myReplicaId
                          uint16_t senderReplicaId = kDefaultSenderReplicaId,
                          // when 0, caluclated internally by default logic
                          uint64_t numexpectedRvbs = 0,
                          // if non-zero, request RVB digests and validate them
                          uint64_t rvbGroupId = 0,
                          // if non-zero - reject is expected and some other arguments are ignored
                          uint16_t rejectReason = 0,
                          // These 2 callbcks can be used to perform operations between triggers of onTimer callback
                          const std::function<R(Args...)>& callBeforeTriggerTimerCb = EMPTY_FUNC,
                          const std::function<R(Args...)>& callAfterTriggerTimerCb = EMPTY_FUNC);
  void sendFetchResPagesMsg(uint64_t lastCheckpointKnownToRequester,
                            uint64_t requiredCheckpointNum,
                            uint16_t senderReplicaId = UINT_LEAST16_MAX);
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
              std::shared_ptr<BcStTestDelegator>& stAdapter,
              BCStateTran* peerStateTransfer);

  // Source (fake) Replies
  void replyAskForCheckpointSummariesMsg(bool generateBlocksAndDescriptors = true);
  void replyFetchBlocksMsg();
  void replyResPagesMsg(bool& outDoneSending);
  void rejectFetchingMsg(uint16_t rejCode, uint64_t reqMsgSeqNum, uint16_t destReplicaId);

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
  void configureLog();
  bool initialized_ = false;

 protected:
  // Infra member functions
  void compareAppStateblocks(uint64_t minBlockId, uint64_t maxBlockId) const;

  // Target/Product - backup replica, during checkpointing
  void bkpPrune(uint64_t maxBlockIdToDelete);

  // Target/Product ST - destination API & assertions
  void dstStartRunningAndCollecting(FetchingState expectedState = FetchingState::NotFetching);
  void dstStartCollecting();
  void dstAssertAskForCheckpointSummariesSent(uint64_t checkpoint_num);
  void dstAssertFetchBlocksMsgSent();
  void dstAssertFetchResPagesMsgSent();
  void dstReportMultiplePrePrepareMessagesReceived(size_t numberOfPreprepareMessages, uint16_t senderId);

  // Target/Product ST - source API & assertions// This should be the same as TestConfig
  void srcAssertCheckpointSummariesSent(uint64_t minRepliedCheckpointNum, uint64_t maxRepliedCheckpointNum);
  void srcAssertItemDataMsgBatchSentWithBlocks(uint64_t minExpectedBlockId, uint64_t maxExpectedBlockId);
  void srcAssertItemDataMsgBatchSentWithResPages(uint32_t expectedChunksSent, uint64_t requiredCheckpointNum);

  // Target/Product ST - common (as source/destination) API & assertions
  void cmnStartRunning(FetchingState expectedState = FetchingState::NotFetching);

  // Target/Product ST - Source Selector
  using MetricKeyValPairs = std::map<std::string, uint64_t>;
  void validateSourceSelectorMetricCounters(const MetricKeyValPairs& metricCounters);

  enum class TRejectFlag { True, False };
  enum class TSkipReplyFlag { True, False };
  template <class R, class... Args>
  void getMissingblocksStage(const std::function<R(Args...)>& callAtStart = EMPTY_FUNC,
                             const std::function<R(Args...)>& callAtEnd = EMPTY_FUNC,
                             TSkipReplyFlag skipReply = TSkipReplyFlag::False,
                             size_t numberOfSkips = 1,
                             TRejectFlag reject = TRejectFlag::False,
                             list<uint16_t> rejectionReasons = list<uint16_t>(),
                             size_t sleepDurationAfterReplyMilli = 20);

  void getReservedPagesStage(TSkipReplyFlag skipReply = TSkipReplyFlag::False,
                             TRejectFlag reject = TRejectFlag::False,
                             uint16_t rejectionReason = 0,
                             size_t sleepDurationAfterReplyMilli = 20);

  void dstValidateCycleEnd(size_t timeToSleepAfterreportCompletedMilli = 10);
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
  ConcordAssertGT(fromBlockId, appState.getGenesisBlockNum());

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
    if ((i == fromBlockId) && (!appState.hasBlock(i - 1))) {
      blk.reset(Block::createFromData(dataSize, buff.get(), i, digestPrev), Block::BlockDeleter());
    } else {
      if (!prevBlk) {
        prevBlk = appState.peekBlock(i - 1);
        ASSERT_TRUE(prevBlk);
      }
      computeBlockDigest(
          prevBlk->blockId, reinterpret_cast<const char*>(prevBlk.get()), prevBlk->totalBlockSize, &digestPrev);
      blk.reset(Block::createFromData(dataSize, buff.get(), i, digestPrev), Block::BlockDeleter());
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
    Digest digestOfResPagesDescriptor;
    BCStateTran::computeDigestOfPagesDescriptor(resPagesDesc, digestOfResPagesDescriptor);
    datastore->free(resPagesDesc);

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
    Digest pageDigest;
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
                                                  const Digest& pageDigest,
                                                  uint32_t sizeOfReservedPage) {
  BCStateTran::ElementOfVirtualBlock* currElement = reinterpret_cast<BCStateTran::ElementOfVirtualBlock*>(position);
  currElement->pageId = pageId;
  currElement->checkpointNumber = checkpointNumber;
  currElement->pageDigest = pageDigest;
  ASSERT_TRUE(!currElement->pageDigest.isZero());
  datastore->getResPage(pageId, checkpointNumber, nullptr, currElement->page, sizeOfReservedPage);
  ASSERT_TRUE(!pageDigest.isZero());
}

// Not all metrics are here, you may add more as needed
void BcStTestDelegator::assertBCStateTranMetricKeyVal(const std::string& key, uint64_t val) {
  auto& stMetrics_ = stateTransfer_->metrics_;
  if (key == "src_overall_batches_sent") {
    ASSERT_EQ(stMetrics_.src_overall_batches_sent_.Get().Get(), val);
  } else if (key == "src_overall_prefetched_batches_sent") {
    ASSERT_EQ(stMetrics_.src_overall_prefetched_batches_sent_.Get().Get(), val);
  } else if (key == "src_overall_on_spot_batches_sent") {
    ASSERT_EQ(stMetrics_.src_overall_on_spot_batches_sent_.Get().Get(), val);
  } else if (key == "src_num_io_contexts_dropped") {
    ASSERT_EQ(stMetrics_.src_num_io_contexts_dropped_.Get().Get(), val);
  } else if (key == "src_num_io_contexts_invoked") {
    ASSERT_EQ(stMetrics_.src_num_io_contexts_invoked_.Get().Get(), val);
  } else if (key == "src_num_io_contexts_consumed") {
    ASSERT_EQ(stMetrics_.src_num_io_contexts_consumed_.Get().Get(), val);
  } else if (key == "received_reject_fetching_msg") {
    ASSERT_EQ(stMetrics_.received_reject_fetching_msg_.Get().Get(), val);
  } else {
    FAIL() << "Unexpected key!";
  }
}

// Not all metrics are here, you may add more as needed
void BcStTestDelegator::assertSourceSelectorMetricKeyVal(const std::string& key, uint64_t val) {
  auto& ssMetrics_ = stateTransfer_->sourceSelector_.metrics_;
  if (key == "total_replacements") {
    ASSERT_EQ(ssMetrics_.total_replacements_.Get().Get(), val);
  } else if (key == "replacement_due_to_no_source") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_no_source_.Get().Get(), val);
  } else if (key == "replacement_due_to_bad_data") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_bad_data_.Get().Get(), val);
  } else if (key == "replacement_due_to_retransmission_timeout") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_retransmission_timeout_.Get().Get(), val);
  } else if (key == "replacement_due_to_periodic_change") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_periodic_change_.Get().Get(), val);
  } else if (key == "replacement_due_to_source_same_as_primary") {
    ASSERT_EQ(ssMetrics_.replacement_due_to_source_same_as_primary_.Get().Get(), val);
  } else {
    FAIL() << "Unexpected key!";
  }
}

void BcStTestDelegator::validateEqualRVTs(const RangeValidationTree& rvtA, const RangeValidationTree& rvtB) const {
  ASSERT_EQ(rvtA.getRootCurrentValueStr(), rvtB.getRootCurrentValueStr());
  ASSERT_EQ(rvtA.totalNodes(), rvtB.totalNodes());
  ASSERT_EQ(rvtA.totalLevels(), rvtB.totalLevels());
  ASSERT_EQ(rvtA.getMinRvbId(), rvtB.getMinRvbId());
  ASSERT_EQ(rvtA.getMaxRvbId(), rvtB.getMaxRvbId());
  ASSERT_EQ(rvtA.rightmost_rvt_node_.size(), rvtB.rightmost_rvt_node_.size());
  ASSERT_EQ(rvtA.leftmost_rvt_node_.size(), rvtB.leftmost_rvt_node_.size());
  ASSERT_EQ(rvtA.leftmost_rvt_node_.size(), RangeValidationTree::NodeInfo::kMaxLevels);
  ASSERT_EQ(rvtA.rightmost_rvt_node_.size(), RangeValidationTree::NodeInfo::kMaxLevels);

  const auto& rmA = rvtA.rightmost_rvt_node_;
  const auto& rmB = rvtB.rightmost_rvt_node_;
  const auto& lmA = rvtA.leftmost_rvt_node_;
  const auto& lmB = rvtB.leftmost_rvt_node_;
  for (size_t i{}; i < RangeValidationTree::NodeInfo::kMaxLevels; ++i) {
    if (rmA[i]) {
      ASSERT_EQ(rmA[i]->info_.id(), rmB[i]->info_.id());
    } else {
      ASSERT_EQ(rmA[i], rmB[i]);
    }
    if (lmA[i]) {
      ASSERT_EQ(lmA[i]->info_.id(), lmB[i]->info_.id());
    } else {
      ASSERT_EQ(lmA[i], lmB[i]);
    }
  }
  ASSERT_EQ(rvtA.max_rvb_index_, rvtB.max_rvb_index_);
  ASSERT_EQ(rvtA.min_rvb_index_, rvtB.min_rvb_index_);
  if (rvtA.root_ && (rvtA.root_ == rvtB.root_)) {
    ASSERT_EQ(rvtA.root_->numChildren(), rvtB.root_->numChildren());
    ASSERT_EQ(rvtA.root_->parent_id_, rvtB.root_->parent_id_);
    ASSERT_EQ(rvtA.root_->insertion_counter_, rvtB.root_->insertion_counter_);
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
                                 std::shared_ptr<BcStTestDelegator>& stAdapter,
                                 BCStateTran* peerStateTransfer)
    : targetConfig_(targetConfig),
      testConfig_(testConfig),
      testState_(testState),
      testedReplicaIf_(testedReplicaIf),
      dataGen_(dataGen),
      stDelegator_(stAdapter),
      peerStateTransfer_(peerStateTransfer) {
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
void FakeDestination::sendAskForCheckpointSummariesMsg(uint64_t minRelevantCheckpointNum, uint16_t senderReplicaId) {
  ASSERT_SRC_UNDER_TEST;
  AskForCheckpointSummariesMsg askForCheckpointSummariesMsg;
  lastMsgSeqNum_ = stDelegator_->uniqueMsgSeqNum();
  askForCheckpointSummariesMsg.msgSeqNum = lastMsgSeqNum_;
  askForCheckpointSummariesMsg.minRelevantCheckpointNum = minRelevantCheckpointNum;
  char* askForCheckpointSummariesMsgBuff{nullptr};
  ASSERT_NFF(TestUtils::allocCopyStateTransferMsg(reinterpret_cast<char*>(&askForCheckpointSummariesMsg),
                                                  sizeof(AskForCheckpointSummariesMsg),
                                                  &askForCheckpointSummariesMsgBuff));
  senderReplicaId = (senderReplicaId == UINT_LEAST16_MAX)
                        ? ((targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas)
                        : senderReplicaId;
  stDelegator_->handleStateTransferMessage(
      askForCheckpointSummariesMsgBuff, sizeof(AskForCheckpointSummariesMsg), senderReplicaId);
}

template <class R, class... Args>
void FakeDestination::sendFetchBlocksMsg(uint64_t minBlockId,
                                         uint64_t maxBlockIdInCycle,
                                         uint64_t maxBlockId,
                                         size_t numExpectedItemDataMsgsInReply,
                                         uint16_t senderReplicaId,
                                         uint64_t numexpectedRvbs,
                                         uint64_t rvbGroupId,
                                         uint16_t rejectReason,
                                         const std::function<R(Args...)>& callBeforeTriggerTimerCb,
                                         const std::function<R(Args...)>& callAfterTriggerTimerCb) {
  ASSERT_SRC_UNDER_TEST;

  if (maxBlockId == 0) {
    maxBlockId = minBlockId + targetConfig_.maxNumberOfChunksInBatch - 1;
  }
  if (numExpectedItemDataMsgsInReply == 0) {
    numExpectedItemDataMsgsInReply = targetConfig_.maxNumberOfChunksInBatch;
  }
  // Remove this line if we would like to make negative tests
  ASSERT_GE(maxBlockId, minBlockId);
  ASSERT_GE(maxBlockIdInCycle, maxBlockId);
  FetchBlocksMsg fetchBlocksMsg;
  lastMsgSeqNum_ = stDelegator_->uniqueMsgSeqNum();

  // Simplify things compare to a real destination - ask for a batch of a size up to maxNumberOfChunksInBatch, without
  // referring to fetchRangeSize
  fetchBlocksMsg.msgSeqNum = lastMsgSeqNum_;
  fetchBlocksMsg.minBlockId = minBlockId;
  fetchBlocksMsg.maxBlockId = maxBlockId;
  fetchBlocksMsg.maxBlockIdInCycle = maxBlockIdInCycle;
  fetchBlocksMsg.lastKnownChunkInLastRequiredBlock = 0;  // for now, chunking is not supported
  fetchBlocksMsg.rvbGroupId = rvbGroupId;
  ASSERT_NE(senderReplicaId, targetConfig_.myReplicaId);
  senderReplicaId = (senderReplicaId == kDefaultSenderReplicaId)
                        ? ((targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas)
                        : senderReplicaId;
  char* fetchBlocksMsgBuff{nullptr};
  ASSERT_NFF(TestUtils::allocCopyStateTransferMsg(
      reinterpret_cast<char*>(&fetchBlocksMsg), sizeof(FetchBlocksMsg), &fetchBlocksMsgBuff));
  stDelegator_->handleStateTransferMessage(fetchBlocksMsgBuff, sizeof(FetchBlocksMsg), senderReplicaId);
  do {
    const auto [isTriggered, duration] = testedReplicaIf_.popOneShotTimerDurationMilli();
    if (!isTriggered) {
      break;
    }
    this_thread::sleep_for(chrono::milliseconds(duration));
    if (callBeforeTriggerTimerCb) {
      callBeforeTriggerTimerCb();
    }
    peerStateTransfer_->onTimer();
    if (callAfterTriggerTimerCb) {
      callAfterTriggerTimerCb();
    }
  } while (true);

  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), (rejectReason == 0) ? numExpectedItemDataMsgsInReply : 1);
  if (rejectReason != 0) {
    ASSERT_EQ(FetchingState::NotFetching, stDelegator_->getFetchingState());
    const auto& msg = testedReplicaIf_.sent_messages_.front();
    ASSERT_NFF(assertMsgType(msg, MsgType::RejectFetching));
    auto* rejectFetchingMsg = reinterpret_cast<RejectFetchingMsg*>(msg.data_.get());
    ASSERT_EQ(rejectFetchingMsg->rejectionCode, rejectReason);
    testedReplicaIf_.sent_messages_.clear();
  } else if (rvbGroupId > 0) {
    // As of now, we support a single RVB group within the collection range
    // A batch is sent, we expect the 1st item data message to include partial left RVB Group. Lets validate it.
    // TODO: assert for the above supported assumption
    const auto& msg = testedReplicaIf_.sent_messages_.front();
    auto itemDataMsg = reinterpret_cast<ItemDataMsg*>(msg.data_.get());
    ASSERT_GT(itemDataMsg->rvbDigestsSize, 0);
    const auto sizeOfRvbDigestInfo = stDelegator_->getSizeOfRvbDigestInfo();
    ASSERT_EQ(itemDataMsg->rvbDigestsSize % sizeOfRvbDigestInfo, 0);
    auto startRvbId = stDelegator_->nextRvbBlockId(minBlockId);
    if (startRvbId == 0) {
      startRvbId = targetConfig_.fetchRangeSize;
    }
    const auto endRvbId = stDelegator_->prevRvbBlockId(maxBlockIdInCycle);
    ASSERT_GE(endRvbId, startRvbId);

    // numexpectedRvbs should be in the range [1,RVT_K]. It is too long to calculate to calculate in a Mock.
    // If there is no pruning it should be RVT_K
    // If given from outside, we test equality, else test for being in the range
    ASSERT_EQ(itemDataMsg->rvbDigestsSize % sizeOfRvbDigestInfo, 0);
    auto numRvbs = itemDataMsg->rvbDigestsSize / sizeOfRvbDigestInfo;
    if (numexpectedRvbs != 0) {
      ASSERT_EQ(numexpectedRvbs, numRvbs);
    } else {
      ASSERT_GE(numRvbs, 1);
      ASSERT_LE(numRvbs, targetConfig_.RVT_K);
    }
  }
}

void FakeDestination::sendFetchResPagesMsg(uint64_t lastCheckpointKnownToRequester,
                                           uint64_t requiredCheckpointNum,
                                           uint16_t senderReplicaId) {
  ASSERT_SRC_UNDER_TEST;
  FetchResPagesMsg fetchResPagesMsg;
  lastMsgSeqNum_ = stDelegator_->uniqueMsgSeqNum();
  fetchResPagesMsg.msgSeqNum = lastMsgSeqNum_;
  fetchResPagesMsg.lastCheckpointKnownToRequester = lastCheckpointKnownToRequester;
  fetchResPagesMsg.requiredCheckpointNum = requiredCheckpointNum;
  fetchResPagesMsg.lastKnownChunk = 0;  // for now, chunking is not supported
  senderReplicaId = (senderReplicaId == UINT_LEAST16_MAX)
                        ? ((targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas)
                        : senderReplicaId;
  char* fetchResPagesMsgBuff{nullptr};
  ASSERT_NFF(TestUtils::allocCopyStateTransferMsg(
      reinterpret_cast<char*>(&fetchResPagesMsg), sizeof(FetchResPagesMsg), &fetchResPagesMsgBuff));
  stDelegator_->handleStateTransferMessage(fetchResPagesMsgBuff, sizeof(FetchResPagesMsg), senderReplicaId);
}

/////////////////////////////////////////////////////////
// FakeSources - definition
/////////////////////////////////////////////////////////
FakeSources::FakeSources(const Config& targetConfig,
                         const TestConfig& testConfig,
                         const TestState& testState,
                         TestReplica& testedReplicaIf,
                         const std::shared_ptr<DataGenerator>& dataGen,
                         std::shared_ptr<BcStTestDelegator>& stAdapter,
                         BCStateTran* peerStateTransfer)
    : FakeReplicaBase(targetConfig, testConfig, testState, testedReplicaIf, dataGen, stAdapter, peerStateTransfer) {}

// TODO - For now we support generating only the initial configuration blocks
void FakeSources::replyAskForCheckpointSummariesMsg(bool generateBlocksAndDescriptors) {
  // We expect a source not fetching. Sending a reject message is not yet supported
  ASSERT_FALSE(datastore_->getIsFetchingState());
  vector<CheckpointSummaryMsg*> checkpointSummaryReplies;

  if (generateBlocksAndDescriptors) {
    // Generate all the blocks until maxBlockId of the last checkpoint - set into appState_
    uint64_t lastBlockId = (testState_.maxRepliedCheckpointNum + 1) * testConfig_.checkpointWindowSize;
    ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, lastBlockId));

    // Generate checkpoint descriptors - - set into datastore_
    ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                       datastore_.get(),
                                                       testState_.minRepliedCheckpointNum,
                                                       testState_.maxRepliedCheckpointNum,
                                                       rvbm_.get()));
  }

  // build a single copy of all replied messages, push to a vector
  const auto& firstMsg = testedReplicaIf_.sent_messages_.front();
  auto firstAskForCheckpointSummariesMsg = reinterpret_cast<AskForCheckpointSummariesMsg*>(firstMsg.data_.get());
  for (uint64_t i = testState_.maxRepliedCheckpointNum; i >= testState_.minRepliedCheckpointNum; i--) {
    ASSERT_TRUE(datastore_->hasCheckpointDesc(i));
    DataStore::CheckpointDesc desc = datastore_->getCheckpointDesc(i);
    CheckpointSummaryMsg* reply(CheckpointSummaryMsg::alloc(desc.rvbData.size()));
    ASSERT_TRUE(reply);
    reply->checkpointNum = desc.checkpointNum;
    reply->maxBlockId = desc.maxBlockId;
    reply->digestOfMaxBlockId = desc.digestOfMaxBlockId;
    reply->digestOfResPagesDescriptor = desc.digestOfResPagesDescriptor;
    reply->requestMsgSeqNum = firstAskForCheckpointSummariesMsg->msgSeqNum;
    std::copy(desc.rvbData.begin(), desc.rvbData.end(), reply->data);
    checkpointSummaryReplies.push_back(reply);
  }

  // send replies from all replicas (shuffle the requests to get a random reply order)
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(testedReplicaIf_.sent_messages_), std::end(testedReplicaIf_.sent_messages_), rng);
  for (const auto reply : checkpointSummaryReplies) {
    for (auto& request : testedReplicaIf_.sent_messages_) {
      CheckpointSummaryMsg* uniqueReply = CheckpointSummaryMsg::alloc(reply);
      ASSERT_TRUE(uniqueReply);
      char* msgBytes{nullptr};
      ASSERT_NFF(
          TestUtils::allocCopyStateTransferMsg(reinterpret_cast<char*>(uniqueReply), uniqueReply->size(), &msgBytes));
      stDelegator_->handleStateTransferMessage(reinterpret_cast<char*>(msgBytes), uniqueReply->size(), request.to_);
      CheckpointSummaryMsg::free(uniqueReply);
    }
    CheckpointSummaryMsg::free(reply);
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

  // very basic validity check, no simulate corruption
  if ((fetchBlocksMsg->minBlockId == 0) || (fetchBlocksMsg->maxBlockId == 0)) {
    rejectFetchingMsg(RejectFetchingMsg::Reason::BLOCK_NOT_FOUND_IN_STORAGE, fetchBlocksMsg->msgSeqNum, msg.to_);
    testedReplicaIf_.sent_messages_.pop_front();
    return;
  }

  // For now we assume no chunking is supported
  ConcordAssertEQ(fetchBlocksMsg->lastKnownChunkInLastRequiredBlock, 0);
  ConcordAssertLE(fetchBlocksMsg->minBlockId, fetchBlocksMsg->maxBlockId);
  size_t rvbGroupDigestsExpectedSize =
      (fetchBlocksMsg->rvbGroupId != 0)
          ? rvbm_->getSerializedDigestsOfRvbGroup(fetchBlocksMsg->rvbGroupId, nullptr, 0, true)
          : 0;
  while (true) {
    size_t rvbGroupDigestsActualSize{0};
    auto blk = appState_.peekBlock(nextBlockId);
    ItemDataMsg* itemDataMsg = ItemDataMsg::alloc(blk->totalBlockSize + rvbGroupDigestsExpectedSize);
    ASSERT_TRUE(itemDataMsg);
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
          fetchBlocksMsg->rvbGroupId, itemDataMsg->data, rvbGroupDigestsExpectedSize, false);
      ConcordAssertLE(rvbGroupDigestsActualSize, rvbGroupDigestsActualSize);
      rvbGroupDigestsExpectedSize = 0;
    }
    itemDataMsg->dataSize = blk->totalBlockSize + rvbGroupDigestsActualSize;
    itemDataMsg->rvbDigestsSize = rvbGroupDigestsActualSize;
    memcpy(itemDataMsg->data + rvbGroupDigestsActualSize, blk.get(), blk->totalBlockSize);
    char* msgBytes{nullptr};
    ASSERT_NFF(
        TestUtils::allocCopyStateTransferMsg(reinterpret_cast<char*>(itemDataMsg), itemDataMsg->size(), &msgBytes));
    stDelegator_->handleStateTransferMessage(reinterpret_cast<char*>(msgBytes), itemDataMsg->size(), msg.to_);
    ItemDataMsg::free(itemDataMsg);
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
    datastore_->free(resPagesDesc);
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
    ItemDataMsg* itemDataMsg = ItemDataMsg::alloc(chunkSize);
    ASSERT_TRUE(itemDataMsg);

    itemDataMsg->requestMsgSeqNum = fetchResPagesMsg->msgSeqNum;
    itemDataMsg->blockNumber = BcStTestDelegator::ID_OF_VBLOCK_RES_PAGES;
    itemDataMsg->totalNumberOfChunksInBlock = numOfChunksInVBlock;
    itemDataMsg->chunkNumber = nextChunk;
    itemDataMsg->dataSize = chunkSize;
    itemDataMsg->lastInBatch =
        ((numOfSentChunks + 1) >= targetConfig_.maxNumberOfChunksInBatch || (nextChunk == numOfChunksInVBlock));
    memcpy(itemDataMsg->data, pRawChunk, chunkSize);

    char* msgBytes{nullptr};
    ASSERT_NFF(
        TestUtils::allocCopyStateTransferMsg(reinterpret_cast<char*>(itemDataMsg), itemDataMsg->size(), &msgBytes));
    stDelegator_->handleStateTransferMessage(reinterpret_cast<char*>(msgBytes), itemDataMsg->size(), msg.to_);
    ItemDataMsg::free(itemDataMsg);
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

void FakeSources::rejectFetchingMsg(uint16_t rejCode, uint64_t reqMsgSeqNum, uint16_t destReplicaId) {
  RejectFetchingMsg rejectFetchingMsg(rejCode, reqMsgSeqNum);
  char* rejectFetchingMsgBuff{nullptr};
  ASSERT_NFF(TestUtils::allocCopyStateTransferMsg(
      reinterpret_cast<char*>(&rejectFetchingMsg), sizeof(RejectFetchingMsg), &rejectFetchingMsgBuff));
  stDelegator_->handleStateTransferMessage(rejectFetchingMsgBuff, sizeof(RejectFetchingMsg), destReplicaId);
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
  ASSERT_NFF(configureLog());
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
    fakeSrcReplica_ = make_unique<FakeSources>(
        targetConfig_, testConfig_, testState_, testedReplicaIf_, dataGen_, stDelegator_, stateTransfer_.get());
  else
    fakeDstReplica_ = make_unique<FakeDestination>(
        targetConfig_, testConfig_, testState_, testedReplicaIf_, dataGen_, stDelegator_, stateTransfer_.get());
  initialized_ = true;
}

void BcStTest::bkpPrune(uint64_t maxBlockIdToDelete) {
  auto rvbm = stDelegator_->getRvbManager();
  rvbm->reportLastAgreedPrunableBlockId(maxBlockIdToDelete);
  // we need to actually delete all blocks in storage to simulate pruning
  ASSERT_NFF(appState_.deleteBlocksUntil(maxBlockIdToDelete + 1));
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
  ASSERT_EQ(expectedState, stDelegator_->getFetchingState());
}

void BcStTest::dstStartCollecting() {
  LOG_TRACE(GL, "");
  ASSERT_DEST_UNDER_TEST;
  ASSERT_TRUE(initialized_);
  ASSERT_TRUE(stateTransfer_->isRunning());
  stateTransfer_->startCollectingState();
  ASSERT_EQ(FetchingState::GettingCheckpointSummaries, stDelegator_->getFetchingState());
  auto minRelevantCheckpoint = datastore_->getLastStoredCheckpoint() ? datastore_->getLastStoredCheckpoint() : 1;
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
    ASSERT_EQ(FetchingState::GettingMissingResPages, stDelegator_->getFetchingState());
  } else {
    // We expect more batches
    ASSERT_EQ(FetchingState::GettingMissingBlocks, stDelegator_->getFetchingState());
    ASSERT_EQ(testState_.maxRequiredBlockId, datastore_->getLastRequiredBlock());
    ASSERT_TRUE(!testedReplicaIf_.sent_messages_.empty());
    ASSERT_NFF(assertMsgType(testedReplicaIf_.sent_messages_.front(), MsgType::FetchBlocks));
    ASSERT_EQ(testedReplicaIf_.sent_messages_.front().to_, currentSourceId);
  }
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
}

void BcStTest::dstAssertFetchResPagesMsgSent() {
  LOG_TRACE(GL, "");
  ASSERT_DEST_UNDER_TEST;
  ASSERT_EQ(FetchingState::GettingMissingResPages, stDelegator_->getFetchingState());
  auto currentSourceId = stDelegator_->getSourceSelector().currentReplica();
  ASSERT_NE(currentSourceId, NO_REPLICA);
  ASSERT_EQ(datastore_->getFirstRequiredBlock(), datastore_->getLastRequiredBlock());
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
  ASSERT_NFF(assertMsgType(testedReplicaIf_.sent_messages_.front(), MsgType::FetchResPages));
  ASSERT_EQ(testedReplicaIf_.sent_messages_.front().to_, currentSourceId);
}

void BcStTest::dstReportMultiplePrePrepareMessagesReceived(size_t numberOfPreprepareMessages, uint16_t senderId) {
  std::unique_ptr<MessageBase> msg;
  ASSERT_NFF(msg = dataGen_->generatePrePrepareMsg(senderId));
  for (size_t i = 1; i <= numberOfPreprepareMessages; i++) {
    stateTransfer_->handleIncomingConsensusMessage(ConsensusMsg(msg->type(), senderId));
  }
}

void BcStTest::srcAssertCheckpointSummariesSent(uint64_t minRepliedCheckpointNum, uint64_t maxRepliedCheckpointNum) {
  LOG_TRACE(GL, "");
  ASSERT_SRC_UNDER_TEST;
  ASSERT_EQ(FetchingState::NotFetching, stDelegator_->getFetchingState());
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
  ASSERT_EQ(FetchingState::NotFetching, stDelegator_->getFetchingState());
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), maxExpectedBlockId - minExpectedBlockId + 1);
  uint64_t currentBlockId = maxExpectedBlockId;  // we expect to get blocks in reverse order, chunking not supported

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
  ASSERT_EQ(FetchingState::NotFetching, stDelegator_->getFetchingState());
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

void BcStTest::configureLog() {
  std::set<string> possibleLogLevels = {"trace", "debug", "info", "warn", "error", "fatal"};
  if (!UserInput::getInstance()->loglevel_.empty()) {
    testConfig_.logLevel = UserInput::getInstance()->loglevel_;
  }
  auto logLevelStr = testConfig_.logLevel;
  if (possibleLogLevels.find(logLevelStr) == possibleLogLevels.end()) {
    std::cout << "\n===\n\n"
              << "Unknown log level! " << logLevelStr << "\n\n===\n\n";
    exit(1);
  }
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
  ASSERT_NFF(stateTransfer_->init(testConfig_.maxNumOfRequiredStoredCheckpoints,
                                  testConfig_.numberOfRequiredReservedPages,
                                  targetConfig_.sizeOfReservedPage));
  cmnStartRunning(expectedState);
  stateTransfer_->onTimer();
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
void BcStTest::getMissingblocksStage(const std::function<R(Args...)>& callAtStart,
                                     const std::function<R(Args...)>& callAtEnd,
                                     TSkipReplyFlag skipReply,
                                     size_t numberOfSkips,
                                     TRejectFlag reject,
                                     list<uint16_t> rejectionReasons,
                                     size_t sleepDurationAfterReplyMilli) {
  ASSERT_FALSE((skipReply == TSkipReplyFlag::True) && (reject == TRejectFlag::True));
  if (skipReply == TSkipReplyFlag::True) {
    ASSERT_GT(numberOfSkips, 0);
  }
  if (reject == TRejectFlag::True) {
    ASSERT_TRUE(!rejectionReasons.empty());
  }
  testState_.nextRequiredBlock = stDelegator_->getNextRequiredBlock();
  while (true) {
    if (callAtStart) {
      ASSERT_NFF(callAtStart());
    }

    if (reject == TRejectFlag::True) {
      ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
      const auto& msg = testedReplicaIf_.sent_messages_.front();
      ASSERT_NFF(assertMsgType(msg, MsgType::FetchBlocks));
      auto fetchBlocksMsg = reinterpret_cast<FetchBlocksMsg*>(msg.data_.get());
      ASSERT_NFF(fakeSrcReplica_->rejectFetchingMsg(rejectionReasons.front(), fetchBlocksMsg->msgSeqNum, msg.to_));
      testedReplicaIf_.sent_messages_.pop_front();
      rejectionReasons.pop_front();
      if (rejectionReasons.empty()) {
        reject = TRejectFlag::False;
      }
    } else if (skipReply == TSkipReplyFlag::False) {
      ASSERT_NFF(fakeSrcReplica_->replyFetchBlocksMsg());
      // There might be pending jobs for putBlock, we need to wait some time and then finalize them by calling onTimer
      this_thread::sleep_for(chrono::milliseconds(sleepDurationAfterReplyMilli));
      stateTransfer_->onTimer();
      ASSERT_NFF(dstAssertFetchBlocksMsgSent());
    } else {
      --numberOfSkips;
      if (numberOfSkips == 0) {
        skipReply = TSkipReplyFlag::False;
      }
      testedReplicaIf_.sent_messages_.pop_front();
    }

    if (datastore_->getFirstRequiredBlock() == 0) {
      break;
    }
    testState_.minRequiredBlockId = datastore_->getFirstRequiredBlock();
    testState_.nextRequiredBlock = stDelegator_->getNextRequiredBlock();

    if (callAtEnd) {
      ASSERT_NFF(callAtEnd());
    }
  }
}

void BcStTest::getReservedPagesStage(TSkipReplyFlag skipReply,
                                     TRejectFlag reject,
                                     uint16_t rejectionReason,
                                     size_t sleepDurationAfterReplyMilli) {
  ASSERT_FALSE((skipReply == TSkipReplyFlag::True) && (reject == TRejectFlag::True));
  ASSERT_NFF(dstAssertFetchResPagesMsgSent());
  if (reject == TRejectFlag::True) {
    ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
    const auto& msg = testedReplicaIf_.sent_messages_.front();
    ASSERT_NFF(assertMsgType(msg, MsgType::FetchResPages));
    auto fetchResPagesMsg = reinterpret_cast<FetchResPagesMsg*>(msg.data_.get());
    ASSERT_NFF(fakeSrcReplica_->rejectFetchingMsg(rejectionReason, fetchResPagesMsg->msgSeqNum, msg.to_));
    testedReplicaIf_.sent_messages_.pop_front();
  } else if (skipReply == TSkipReplyFlag::False) {
    for (bool doneSending = false; !doneSending;) {
      ASSERT_NFF(fakeSrcReplica_->replyResPagesMsg(doneSending));
    }
  }
  if (sleepDurationAfterReplyMilli > 0) {
    this_thread::sleep_for(chrono::milliseconds(sleepDurationAfterReplyMilli));
    stateTransfer_->onTimer();
  }
}

void BcStTest::dstValidateCycleEnd(size_t timeToSleepAfterreportCompletedMilli) {
  this_thread::sleep_for(chrono::milliseconds(timeToSleepAfterreportCompletedMilli));
  ASSERT_TRUE(testedReplicaIf_.onTransferringCompleteCalled_);
  stateTransfer_->onTimer();
  ASSERT_EQ(FetchingState::NotFetching, stDelegator_->getFetchingState());
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
  ASSERT_NFF(dstValidateCycleEnd());
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
      ASSERT_NFF(dstReportMultiplePrePrepareMessagesReceived(targetConfig_.minPrePrepareMsgsForPrimaryAwareness,
                                                             ss.currentReplica()));
    });
  };
  ASSERT_NFF(getMissingblocksStage(EMPTY_FUNC, trigger_source_change));
  const auto& sources = stDelegator_->getSourceSelector().getActualSources();
  ASSERT_EQ(sources.size(), number_of_replacements);

  validateSourceSelectorMetricCounters({{"total_replacements", number_of_replacements},
                                        {"replacement_due_to_no_source", 1},
                                        {"replacement_due_to_source_same_as_primary", number_of_replacements - 1},
                                        {"replacement_due_to_bad_data", 0},
                                        {"replacement_due_to_periodic_change", 0},
                                        {"replacement_due_to_retransmission_timeout", 0}});

  ASSERT_NFF(getReservedPagesStage());
  // validate completion
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
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
      stateTransfer_->onTimer();
    }
  }
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
  currentSrc = sourceSelector.currentReplica();
  ASSERT_NFF(getMissingblocksStage<void>());
  ASSERT_NFF(getReservedPagesStage());
  // now validate completion
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
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
  validateSourceSelectorMetricCounters({{"total_replacements", actualSources_.size()},
                                        {"replacement_due_to_no_source", 1},
                                        {"replacement_due_to_source_same_as_primary", 0},
                                        {"replacement_due_to_periodic_change", actualSources_.size() - 1},
                                        {"replacement_due_to_retransmission_timeout", 0},
                                        {"replacement_due_to_bad_data", 0}});
  ASSERT_NFF(getReservedPagesStage());
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
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
      ASSERT_NFF(dstReportMultiplePrePrepareMessagesReceived(targetConfig_.minPrePrepareMsgsForPrimaryAwareness,
                                                             ss.currentReplica()));
    });
  };
  ASSERT_NFF(getMissingblocksStage(EMPTY_FUNC, trigger_source_change));
  const auto& sources = stDelegator_->getSourceSelector().getActualSources();
  // TBD metric counters in source selector should be used to validate changed sources to avoid primary
  ASSERT_EQ(sources.size(), 2);
  validateSourceSelectorMetricCounters({{"total_replacements", 2},
                                        {"replacement_due_to_no_source", 1},
                                        {"replacement_due_to_source_same_as_primary", 1},
                                        {"replacement_due_to_periodic_change", 0},
                                        {"replacement_due_to_retransmission_timeout", 0},
                                        {"replacement_due_to_bad_data", 0}});
  ASSERT_NFF(getReservedPagesStage());
  // validate completion
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
}

TEST_F(BcStTest, dstProcessDataWithNoKnownSources) {
  ASSERT_NFF(initialize());
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  auto& ss = stDelegator_->getSourceSelector();

  // Scenario:
  // Retransmission time out reached (or some other bad reason) for f replicas & new source selected.
  // This newly selected source is apparently last source in preferred list.
  // Destination sends fetchBlockMsg() to new (also last one) source.
  // This newly selected source becomes primary & as a result it also gets removed from preferred list.
  // Assert will hit as destination replica assumes that there is at least one source replica in preferred list
  // to get (V)block.

  while (ss.numberOfPreferredReplicas() > 1) {
    ss.updateSource(getMonotonicTimeMilli());
  }
  ss.removeCurrentReplica();

  ASSERT_NFF(getMissingblocksStage<void>());
  ASSERT_NFF(getReservedPagesStage());
  // validate completion
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
}

TEST_F(BcStTest, dstPreprepareFromMultipleSourcesDuringStateTransfer) {
  ASSERT_NFF(initialize());
  std::once_flag once_flag;
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  auto ss = stDelegator_->getSourceSelector();
  const std::function<void(void)> generate_preprepare_messages = [&]() {
    std::call_once(once_flag, [&] {
      auto senderId = ss.currentReplica();
      ASSERT_NFF(dstReportMultiplePrePrepareMessagesReceived(targetConfig_.minPrePrepareMsgsForPrimaryAwareness - 1,
                                                             senderId));
      ASSERT_NFF(dstReportMultiplePrePrepareMessagesReceived(1, (senderId + 1) % targetConfig_.numReplicas));
    });
  };
  ASSERT_NFF(getMissingblocksStage(EMPTY_FUNC, generate_preprepare_messages));
  const auto& sources = stDelegator_->getSourceSelector().getActualSources();
  // TBD metric counters in source selector should be used to validate changed sources to avoid primary
  ASSERT_EQ(sources.size(), 1);
  validateSourceSelectorMetricCounters({{"total_replacements", 1},
                                        {"replacement_due_to_no_source", 1},
                                        {"replacement_due_to_source_same_as_primary", 0},
                                        {"replacement_due_to_periodic_change", 0},
                                        {"replacement_due_to_retransmission_timeout", 0},
                                        {"replacement_due_to_bad_data", 0}});
  ASSERT_NFF(getReservedPagesStage());
  // validate completion
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
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
    ASSERT_NFF(dstValidateCycleEnd());
    ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                     testState_.maxRequiredBlockId));
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
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
}

// Since state is getting missing blocks, replica re-set preffered group and continues without the rejecting source
TEST_F(BcStTest, dstSetNewPrefferedReplicasOnFetchBlocksMsgRejection) {
  list<uint16_t> rejectReasons{RejectFetchingMsg::Reason::IN_STATE_TRANSFER,
                               RejectFetchingMsg::Reason::BLOCK_RANGE_NOT_FOUND,
                               RejectFetchingMsg::Reason::IN_ACTIVE_SESSION,
                               RejectFetchingMsg::Reason::INVALID_NUMBER_OF_BLOCKS_REQUESTED,
                               RejectFetchingMsg::Reason::BLOCK_NOT_FOUND_IN_STORAGE,
                               RejectFetchingMsg::Reason::DIGESTS_FOR_RVBGROUP_NOT_FOUND};

  targetConfig_.maxFetchRetransmissions = 1;  // to trigger an immediate source change after single msg corruption
  ASSERT_NFF(initialize());
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  ASSERT_NFF(getMissingblocksStage(
      EMPTY_FUNC, EMPTY_FUNC, TSkipReplyFlag::False, 0, TRejectFlag::True, std::move(rejectReasons)));
  ASSERT_NFF(getReservedPagesStage());
  // now validate completion
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
}

// This test checks that replica enters a new internal cycle when reserved pages reuqest is rejected.
TEST_F(BcStTest, dstEnterInternalCycleOnFetchReservedPagesRejection) {
  ASSERT_NFF(initialize());

  // 1st cycle - source will not answer destination request for reserved pages
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  ASSERT_NFF(getMissingblocksStage<void>());
  const auto& preferredReplicas = stDelegator_->getPreferredReplicas();
  ASSERT_EQ(preferredReplicas.size(), targetConfig_.fVal + 1);
  ASSERT_NFF(
      getReservedPagesStage(TSkipReplyFlag::False, TRejectFlag::True, RejectFetchingMsg::Reason::RES_PAGE_NOT_FOUND));
  ASSERT_TRUE(preferredReplicas.empty());
  ASSERT_EQ(FetchingState::GettingCheckpointSummaries, stDelegator_->getFetchingState());

  // 2nd (internal) cycle starts, this time nothing 'bad' happens, but cycle is only for getting the reserved pages
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg(false));
  ASSERT_NFF(getReservedPagesStage());
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
}

// This test makes sure that current primary is not modified during GettingCheckpointSummaries
// Its doing so by checking the value of currentPrimary in source selector.
// It sends 2 cycles of preprepare messages that in any other St state whould have triggerred a new primary awarness.
// Since they are sent during GettingCheckpointSummaries, they are being  ignored.
TEST_F(BcStTest, dstTestprimaryAwarnessduringAskForCheckpointSummariesMsg) {
  ASSERT_NFF(initialize());
  ASSERT_NFF(dstStartRunningAndCollecting());
  auto ss = stDelegator_->getSourceSelector();
  ASSERT_EQ(ss.currentPrimary(), NO_REPLICA);  // make sure primary unknown
  // ST cycle has started, askForCheckpointSummariesMsg sent and destination is waiting for reply.
  // Before replying - Generate prePrepare messages to trigger source selector to change the source to avoid primary.

  // report <minPrePrepareMsgsForPrimaryAwareness> pre prepare messages with primary as <currentPrimary>
  ASSERT_NFF(dstReportMultiplePrePrepareMessagesReceived(targetConfig_.minPrePrepareMsgsForPrimaryAwareness,
                                                         (targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas));

  // Validate that primary has not changed and is still unknown, due to the fact that ST should ignore preprepares
  // during askForCheckpointSummariesMsg
  ASSERT_EQ(ss.currentPrimary(), NO_REPLICA);

  // report another <minPrePrepareMsgsForPrimaryAwareness> pre prepare messages with a different primary
  ASSERT_NFF(dstReportMultiplePrePrepareMessagesReceived(targetConfig_.minPrePrepareMsgsForPrimaryAwareness,
                                                         (targetConfig_.myReplicaId + 2) % targetConfig_.numReplicas));

  // Validate again that primary is still unknown
  ASSERT_EQ(ss.currentPrimary(), NO_REPLICA);

  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  ASSERT_NFF(getMissingblocksStage<void>());
  ASSERT_NFF(getReservedPagesStage());
  // now validate completion
  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
}

class BcStTestParamFixtureRejectReason : public BcStTest, public testing::WithParamInterface<uint16_t> {};
// Rejection is handled gracefully while getting missing blocks
TEST_P(BcStTestParamFixtureRejectReason, dstRejectFetchBlocksMsgOnce) {
  auto rejectionReason = GetParam();
  ASSERT_NFF(initialize());
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());

  ASSERT_NFF(getMissingblocksStage<void>(
      EMPTY_FUNC, EMPTY_FUNC, TSkipReplyFlag::False, 0, TRejectFlag::True, std::list<uint16_t>{rejectionReason}));
  ASSERT_NFF(getReservedPagesStage());
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("received_reject_fetching_msg", 1));

  ASSERT_NFF(dstValidateCycleEnd());
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
}

INSTANTIATE_TEST_CASE_P(BcStTest,
                        BcStTestParamFixtureRejectReason,
                        ::testing::Values(RejectFetchingMsg::Reason::IN_STATE_TRANSFER,
                                          RejectFetchingMsg::Reason::BLOCK_RANGE_NOT_FOUND,
                                          RejectFetchingMsg::Reason::IN_ACTIVE_SESSION,
                                          RejectFetchingMsg::Reason::INVALID_NUMBER_OF_BLOCKS_REQUESTED,
                                          RejectFetchingMsg::Reason::BLOCK_NOT_FOUND_IN_STORAGE,
                                          RejectFetchingMsg::Reason::DIGESTS_FOR_RVBGROUP_NOT_FOUND), );

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

  // Make sure the following metrics are zeroed
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_batches_sent", 0));
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_prefetched_batches_sent", 0));
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_on_spot_batches_sent", 0));

  // Send FetchBlocksMsg and validate the messages in outgoing queue testedReplicaIf_.sent_messages_
  // Make sure the message signals source also for prefetch a full batch
  ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(testState_.minRequiredBlockId, testState_.maxRequiredBlockId));
  uint64_t maxExpectedBlockId = (testState_.numBlocksToCollect > targetConfig_.maxNumberOfChunksInBatch)
                                    ? (testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch - 1)
                                    : testState_.maxRequiredBlockId;
  ASSERT_NFF(srcAssertItemDataMsgBatchSentWithBlocks(testState_.minRequiredBlockId, maxExpectedBlockId));

  // Make sure the following metrics are zeroed
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_batches_sent", 1));
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_prefetched_batches_sent", 0));
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_on_spot_batches_sent", 1));

  // clear the outgoing queue, and send another request, validate that the it was prefetched using metrics
  testState_.minRequiredBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  testedReplicaIf_.sent_messages_.clear();
  ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(testState_.minRequiredBlockId, testState_.maxRequiredBlockId));
  maxExpectedBlockId = (testState_.numBlocksToCollect > targetConfig_.maxNumberOfChunksInBatch)
                           ? (testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch - 1)
                           : testState_.maxRequiredBlockId;
  ASSERT_NFF(srcAssertItemDataMsgBatchSentWithBlocks(testState_.minRequiredBlockId, maxExpectedBlockId));

  // Make sure the following metrics are zeroed
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_batches_sent", 2));
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_prefetched_batches_sent", 1));
  ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_on_spot_batches_sent", 1));
}

TEST_F(BcStTest, srcRejectFetchBlocksMsgOnRvbGroupDigests) {
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

  // Send FetchBlocksMsg with an illegal rvbGroupId, we expect it to be rejected
  ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(testState_.minRequiredBlockId,
                                                       testState_.maxRequiredBlockId,
                                                       0,
                                                       0,
                                                       FakeDestination::kDefaultSenderReplicaId,
                                                       0,
                                                       1,
                                                       RejectFetchingMsg::Reason::DIGESTS_FOR_RVBGROUP_NOT_FOUND));
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

TEST_F(BcStTest, srcTestSessionManagement) {
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

  auto senderReplicaId2 = (targetConfig_.myReplicaId + 2) % targetConfig_.numReplicas;
  auto senderReplicaId1 = (targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas;

  // for senderReplicaId2 only:
  ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(
      testState_.minRequiredBlockId, testState_.maxRequiredBlockId, 0, 0, senderReplicaId2));
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), targetConfig_.maxNumberOfChunksInBatch);
  testedReplicaIf_.sent_messages_.clear();
  // Validate that session was really openned for senderReplicaId
  ASSERT_TRUE(stDelegator_->isSrcSessionOpen());
  ASSERT_EQ(stDelegator_->srcSessionOwnerDestReplicaId(), senderReplicaId2);
  // wait some time and see that session is still open
  this_thread::sleep_for(chrono::milliseconds(targetConfig_.sourceSessionExpiryDurationMs / 2));
  stateTransfer_->onTimer();
  ASSERT_TRUE(stDelegator_->isSrcSessionOpen());
  ASSERT_EQ(stDelegator_->srcSessionOwnerDestReplicaId(), senderReplicaId2);
  // Send another request and validate an open session for senderReplicaId
  ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(
      testState_.minRequiredBlockId, testState_.maxRequiredBlockId, 0, 0, senderReplicaId2));
  auto timeLeftForSession = targetConfig_.sourceSessionExpiryDurationMs;
  ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), targetConfig_.maxNumberOfChunksInBatch);
  testedReplicaIf_.sent_messages_.clear();
  ASSERT_TRUE(stDelegator_->isSrcSessionOpen());
  ASSERT_EQ(stDelegator_->srcSessionOwnerDestReplicaId(), senderReplicaId2);

  //
  // sleep 50 millisec, then send request from senderReplicaId1 and validate rejection x 3 times
  for (size_t i{}; i < 3; ++i) {
    this_thread::sleep_for(chrono::milliseconds(50));
    stateTransfer_->onTimer();
    timeLeftForSession -= 50;
    ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(
        testState_.minRequiredBlockId, testState_.maxRequiredBlockId, 0, 1, senderReplicaId1));
    ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), 1);
    const auto& msg = testedReplicaIf_.sent_messages_.front();
    ASSERT_NFF(assertMsgType(msg, MsgType::RejectFetching));
    testedReplicaIf_.sent_messages_.clear();
    stateTransfer_->onTimer();
  }

  // Now wait till session expire and send request x3 times from senderReplicaId2, see that we get ItemDataMsg
  this_thread::sleep_for(chrono::milliseconds(timeLeftForSession + 50));
  stateTransfer_->onTimer();
  for (size_t i{}; i < 3; ++i) {
    this_thread::sleep_for(chrono::milliseconds(50));
    ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(
        testState_.minRequiredBlockId, testState_.maxRequiredBlockId, 0, 0, senderReplicaId2));
    ASSERT_EQ(testedReplicaIf_.sent_messages_.size(), targetConfig_.maxNumberOfChunksInBatch);
    testedReplicaIf_.sent_messages_.clear();
    // Validate that session was really openned for senderReplicaId2
    stateTransfer_->onTimer();
    ASSERT_TRUE(stDelegator_->isSrcSessionOpen());
    ASSERT_EQ(stDelegator_->srcSessionOwnerDestReplicaId(), senderReplicaId2);
  }
}

// check that when sourceSessionExpiryDurationMs=0, no destination is rejected by source
TEST_F(BcStTest, srcTestSessionManagementDisabled) {
  testConfig_.testTarget = TestConfig::TestTarget::SOURCE;
  targetConfig_.sourceSessionExpiryDurationMs = 0;
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  // Generate the data needed for a tested ST backup replica
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));

  // send request from all replicas, non should be rejected
  for (size_t i{}, j{}; i < targetConfig_.numReplicas; ++i) {
    if (i == targetConfig_.myReplicaId) {
      continue;
    }
    ++j;
    ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(
        testState_.minRequiredBlockId, testState_.maxRequiredBlockId, 0, j * targetConfig_.maxNumberOfChunksInBatch, i))
        << KVLOG(testState_.minRequiredBlockId, testState_.maxRequiredBlockId, i, j);
    testState_.minRequiredBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  }
}

// check that source closes cycle at the end of a cycle
TEST_F(BcStTest, srcTestCloseSessionOnCycleEnd) {
  testConfig_.testTarget = TestConfig::TestTarget::SOURCE;
  targetConfig_.sourceSessionExpiryDurationMs = 0;
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  // Generate the data needed for a tested ST backup replica
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));

  // send 2 regular requests, check that cycle stays open after each of them
  ASSERT_TRUE(!stDelegator_->isSrcSessionOpen());
  for (size_t i{0}; i < 2; ++i) {
    ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(testState_.minRequiredBlockId,
                                                         testState_.maxRequiredBlockId,
                                                         0,
                                                         (i + 1) * targetConfig_.maxNumberOfChunksInBatch));
    ASSERT_TRUE(stDelegator_->isSrcSessionOpen());
    ASSERT_EQ(stDelegator_->srcSessionOwnerDestReplicaId(),
              ((targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas));
  }

  // now send a request for last batch in cycle, 2 blocks only, validate that session is closed after
  testedReplicaIf_.sent_messages_.clear();
  ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(
      testState_.maxRequiredBlockId - 1, testState_.maxRequiredBlockId, testState_.maxRequiredBlockId, 2));
  ASSERT_TRUE(!stDelegator_->isSrcSessionOpen());
}

// test all scenarios where source is required to send RVB group digests from storage and pruning vector
TEST_F(BcStTest, srcTestSendRvbGroupDigests) {
  testConfig_.testTarget = TestConfig::TestTarget::SOURCE;
  // set RVT_K to 1024, so we can be sure all fetch range is contained in a single RVB group
  targetConfig_.RVT_K = 1024;

  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  // Generate the data needed for a tested ST backup replica
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));

  // Send a FetchBlocksMsg and validate the rvb data (this part is optional)
  const auto rvbGroupId = stDelegator_->getRvbManager()->getFetchBlocksRvbGroupId(testState_.minRequiredBlockId,
                                                                                  testState_.maxRequiredBlockId);
  ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(testState_.minRequiredBlockId,
                                                       testState_.maxRequiredBlockId,
                                                       0,
                                                       0,
                                                       ((targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas),
                                                       0,
                                                       rvbGroupId));
  // clear the outgoing message queue, but 1st copy the RVB digests for later use
  const auto& msg1 = testedReplicaIf_.sent_messages_.front();
  auto fetchBlocksMsg1 = reinterpret_cast<ItemDataMsg*>(msg1.data_.get());
  std::unique_ptr<char[]> buffer1(new char[fetchBlocksMsg1->rvbDigestsSize]);
  memcpy(buffer1.get(), fetchBlocksMsg1->data, fetchBlocksMsg1->rvbDigestsSize);
  auto rvbDigestsSize1 = fetchBlocksMsg1->rvbDigestsSize;
  testedReplicaIf_.sent_messages_.clear();

  // Prune approx half of the blocks and send another request, We expect source to send the missing blocks
  // from the pruned blocks digests vector. So the batch should start from much newer blocks, but the digests should be
  // exactly the same since the RVT has not changed.
  auto maxBlockIdToDelete =
      testState_.minRequiredBlockId + ((testState_.maxRequiredBlockId - testState_.minRequiredBlockId) / 2);
  // Prune all blocks until block ID maxBlockIdToDelete
  ASSERT_NFF(bkpPrune(maxBlockIdToDelete));
  auto startRvbId = stDelegator_->nextRvbBlockId(testState_.minRequiredBlockId);
  if (startRvbId == 0) {
    startRvbId = targetConfig_.fetchRangeSize;
  }
  const auto endRvbId = stDelegator_->prevRvbBlockId(testState_.maxRequiredBlockId);
  ASSERT_GT(endRvbId, startRvbId);
  ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(maxBlockIdToDelete + 1,
                                                       testState_.maxRequiredBlockId,
                                                       0,
                                                       0,
                                                       ((targetConfig_.myReplicaId + 1) % targetConfig_.numReplicas),
                                                       ((endRvbId - startRvbId) / targetConfig_.fetchRangeSize) + 1,
                                                       rvbGroupId));
  // compare 2 RVB digests data - they should be equal
  const auto& msg2 = testedReplicaIf_.sent_messages_.front();
  auto fetchBlocksMsg2 = reinterpret_cast<ItemDataMsg*>(msg2.data_.get());
  std::unique_ptr<char[]> buffer2(new char[fetchBlocksMsg2->rvbDigestsSize]);
  memcpy(buffer2.get(), fetchBlocksMsg2->data, fetchBlocksMsg2->rvbDigestsSize);
  auto rvbDigestsSize2 = fetchBlocksMsg2->rvbDigestsSize;
  ASSERT_EQ(rvbDigestsSize2, rvbDigestsSize1);
  ASSERT_EQ(memcmp(buffer1.get(), buffer2.get(), rvbDigestsSize2), 0);
}

// test all scenarios where source is required to send RVB group digests from storage and pruning vector
TEST_F(BcStTest, srcTestSourcePrepareIoContexts) {
  testConfig_.testTarget = TestConfig::TestTarget::SOURCE;
  // set maxNumberOfChunksInBatch/fetchRangeSize to 10 so we can easily test and debug this function
  targetConfig_.maxNumberOfChunksInBatch = 10;
  targetConfig_.fetchRangeSize = 10;

  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  // Generate the data needed for a tested ST backup replica
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_NFF(dataGen_->generateCheckpointDescriptors(appState_,
                                                     datastore_,
                                                     testState_.minRepliedCheckpointNum,
                                                     testState_.maxRepliedCheckpointNum,
                                                     stDelegator_->getRvbManager()));
  auto& ioPool = stDelegator_->getIoPool();
  auto& ioContexts = stDelegator_->getIoContexts();

  auto sendFetchBlocksMsgAndValidatePrefetch = [&](uint64_t minBlockId,
                                                   uint64_t maxBlockIdInCycle,
                                                   uint64_t maxBlockId,
                                                   uint64_t numExpectedItemDataMsgsInReply,
                                                   uint64_t minPreFetchedBlockId,
                                                   uint64_t maxPrefetchedBlockId,
                                                   uint64_t src_overall_batches_sent,
                                                   uint64_t src_overall_prefetched_batches_sent,
                                                   uint64_t src_overall_on_spot_batches_sent,
                                                   uint64_t src_num_io_contexts_dropped,
                                                   uint64_t src_num_io_contexts_invoked,
                                                   uint64_t src_num_io_contexts_consumed) {
    ASSERT_NFF(fakeDstReplica_->sendFetchBlocksMsg<void>(minBlockId,
                                                         maxBlockIdInCycle,
                                                         maxBlockId,
                                                         numExpectedItemDataMsgsInReply,
                                                         FakeDestination::kDefaultSenderReplicaId,
                                                         0,
                                                         0));

    ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_batches_sent", src_overall_batches_sent));
    ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_prefetched_batches_sent",
                                                           src_overall_prefetched_batches_sent));
    ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_overall_on_spot_batches_sent",
                                                           src_overall_on_spot_batches_sent));
    ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_num_io_contexts_dropped", src_num_io_contexts_dropped));
    ASSERT_NFF(stDelegator_->assertBCStateTranMetricKeyVal("src_num_io_contexts_invoked", src_num_io_contexts_invoked));
    ASSERT_NFF(
        stDelegator_->assertBCStateTranMetricKeyVal("src_num_io_contexts_consumed", src_num_io_contexts_consumed));
    testedReplicaIf_.sent_messages_.clear();

    ASSERT_EQ(targetConfig_.maxNumberOfChunksInBatch, ioContexts.size());

    size_t numBlocksRequested{maxPrefetchedBlockId - minPreFetchedBlockId + 1};
    auto j{maxPrefetchedBlockId};
    size_t i{};
    for (; i < std::min(numBlocksRequested, ioContexts.size()); ++i, --j) {
      ASSERT_EQ(ioContexts[i]->blockId, j);
      ASSERT_TRUE(ioContexts[i]->future.valid());
    }
  };

  uint64_t src_overall_batches_sent{};
  uint64_t src_overall_prefetched_batches_sent{};
  uint64_t src_overall_on_spot_batches_sent{};
  uint64_t src_num_io_contexts_dropped{};
  uint64_t src_num_io_contexts_invoked{};
  uint64_t src_num_io_contexts_consumed{};

  // 1) Lets send a standard FetchBlocksMsg(request full batch), we expect batch prediction to trigger
  uint64_t minPreFetchedBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  uint64_t maxPrefetchedBlockId = minPreFetchedBlockId + targetConfig_.maxNumberOfChunksInBatch - 1;
  ASSERT_NFF(
      sendFetchBlocksMsgAndValidatePrefetch(testState_.minRequiredBlockId,
                                            testState_.maxRequiredBlockId,
                                            0,
                                            0,
                                            minPreFetchedBlockId,
                                            maxPrefetchedBlockId,
                                            ++src_overall_batches_sent,
                                            src_overall_prefetched_batches_sent,
                                            ++src_overall_on_spot_batches_sent,
                                            src_num_io_contexts_dropped,
                                            src_num_io_contexts_invoked = targetConfig_.maxNumberOfChunksInBatch * 2,
                                            src_num_io_contexts_consumed = targetConfig_.maxNumberOfChunksInBatch));

  // 2) Send another FetchBlocksMsg and see that prefetch goes as expected
  testState_.minRequiredBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  minPreFetchedBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  maxPrefetchedBlockId = minPreFetchedBlockId + targetConfig_.maxNumberOfChunksInBatch - 1;
  ASSERT_NFF(
      sendFetchBlocksMsgAndValidatePrefetch(testState_.minRequiredBlockId,
                                            testState_.maxRequiredBlockId,
                                            0,
                                            0,
                                            minPreFetchedBlockId,
                                            maxPrefetchedBlockId,
                                            ++src_overall_batches_sent,
                                            ++src_overall_prefetched_batches_sent,
                                            src_overall_on_spot_batches_sent,
                                            src_num_io_contexts_dropped,
                                            src_num_io_contexts_invoked += targetConfig_.maxNumberOfChunksInBatch,
                                            src_num_io_contexts_consumed += targetConfig_.maxNumberOfChunksInBatch));

  // 3) Clear Io contexts and see that (again) when sending FetchBlocksMsg, on-spot batch is sent
  stDelegator_->clearIoContexts();
  testState_.minRequiredBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  minPreFetchedBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  maxPrefetchedBlockId = minPreFetchedBlockId + targetConfig_.maxNumberOfChunksInBatch - 1;
  ASSERT_NFF(
      sendFetchBlocksMsgAndValidatePrefetch(testState_.minRequiredBlockId,
                                            testState_.maxRequiredBlockId,
                                            0,
                                            0,
                                            minPreFetchedBlockId,
                                            maxPrefetchedBlockId,
                                            ++src_overall_batches_sent,
                                            src_overall_prefetched_batches_sent,
                                            ++src_overall_on_spot_batches_sent,
                                            src_num_io_contexts_dropped,
                                            src_num_io_contexts_invoked += 2 * targetConfig_.maxNumberOfChunksInBatch,
                                            src_num_io_contexts_consumed += targetConfig_.maxNumberOfChunksInBatch));

  // 4) Change front block Id on ioContexts, and check that pre-fetch is cleared while sending another FetchBlocksMsg
  testState_.minRequiredBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  ioContexts.front()->blockId = ioContexts.front()->blockId - 1;
  minPreFetchedBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  maxPrefetchedBlockId = minPreFetchedBlockId + targetConfig_.maxNumberOfChunksInBatch - 1;
  ASSERT_NFF(
      sendFetchBlocksMsgAndValidatePrefetch(testState_.minRequiredBlockId,
                                            testState_.maxRequiredBlockId,
                                            0,
                                            0,
                                            minPreFetchedBlockId,
                                            maxPrefetchedBlockId,
                                            ++src_overall_batches_sent,
                                            src_overall_prefetched_batches_sent,
                                            ++src_overall_on_spot_batches_sent,
                                            src_num_io_contexts_dropped += targetConfig_.maxNumberOfChunksInBatch,
                                            src_num_io_contexts_invoked += 2 * targetConfig_.maxNumberOfChunksInBatch,
                                            src_num_io_contexts_consumed += targetConfig_.maxNumberOfChunksInBatch));

  // 5) Change mid block Id on ioContexts, and check that pre-fetch is cleared while sending another FetchBlocksMsg
  testState_.minRequiredBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  ioContexts[3]->blockId = ioContexts[3]->blockId - 1;
  minPreFetchedBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  maxPrefetchedBlockId = minPreFetchedBlockId + targetConfig_.maxNumberOfChunksInBatch - 1;
  ASSERT_NFF(
      sendFetchBlocksMsgAndValidatePrefetch(testState_.minRequiredBlockId,
                                            testState_.maxRequiredBlockId,
                                            0,
                                            0,
                                            minPreFetchedBlockId,
                                            maxPrefetchedBlockId,
                                            ++src_overall_batches_sent,
                                            src_overall_prefetched_batches_sent,
                                            ++src_overall_on_spot_batches_sent,
                                            src_num_io_contexts_dropped += targetConfig_.maxNumberOfChunksInBatch,
                                            src_num_io_contexts_invoked += 2 * targetConfig_.maxNumberOfChunksInBatch,
                                            src_num_io_contexts_consumed += targetConfig_.maxNumberOfChunksInBatch));

  // 6) Consume front future on ioContexts, and check that pre-fetch is cleared while sending another FetchBlocksMsg
  testState_.minRequiredBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  ioContexts[0]->future.get();
  minPreFetchedBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  maxPrefetchedBlockId = minPreFetchedBlockId + targetConfig_.maxNumberOfChunksInBatch - 1;
  ASSERT_NFF(
      sendFetchBlocksMsgAndValidatePrefetch(testState_.minRequiredBlockId,
                                            testState_.maxRequiredBlockId,
                                            0,
                                            0,
                                            minPreFetchedBlockId,
                                            maxPrefetchedBlockId,
                                            ++src_overall_batches_sent,
                                            src_overall_prefetched_batches_sent,
                                            ++src_overall_on_spot_batches_sent,
                                            src_num_io_contexts_dropped += targetConfig_.maxNumberOfChunksInBatch,
                                            src_num_io_contexts_invoked += 2 * targetConfig_.maxNumberOfChunksInBatch,
                                            src_num_io_contexts_consumed += targetConfig_.maxNumberOfChunksInBatch));

  // 7) Partial prediction test: Clear last 3 contexts, As of now we do not support partial prediciton. So we expect
  // all contexts to be cleared and fetched
  for (auto it = ioContexts.begin() + (targetConfig_.maxNumberOfChunksInBatch - 3); it != ioContexts.end();) {
    ioPool.free(*it);
    it = ioContexts.erase(it);
  }
  testState_.minRequiredBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  minPreFetchedBlockId = testState_.minRequiredBlockId + targetConfig_.maxNumberOfChunksInBatch;
  maxPrefetchedBlockId = minPreFetchedBlockId + targetConfig_.maxNumberOfChunksInBatch - 1;
  ASSERT_NFF(
      sendFetchBlocksMsgAndValidatePrefetch(testState_.minRequiredBlockId,
                                            testState_.maxRequiredBlockId,
                                            0,
                                            0,
                                            minPreFetchedBlockId,
                                            maxPrefetchedBlockId,
                                            ++src_overall_batches_sent,
                                            src_overall_prefetched_batches_sent,
                                            ++src_overall_on_spot_batches_sent,
                                            src_num_io_contexts_dropped += (targetConfig_.maxNumberOfChunksInBatch - 3),
                                            src_num_io_contexts_invoked += 2 * targetConfig_.maxNumberOfChunksInBatch,
                                            src_num_io_contexts_consumed += targetConfig_.maxNumberOfChunksInBatch));
}

/////////////////////////////////////////////////////////////////
//
//  BcStTest Backup Replica (Initialization, Checkpointing) Tests
//
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
  auto h1 = rvt->getRootCurrentValueStr();
  ASSERT_NFF(dstRestart(false, FetchingState::NotFetching));
  rvt = stDelegator_->getRvt();
  auto h2 = rvt->getRootCurrentValueStr();
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
  ASSERT_NFF(bkpPrune(midBlockId));
  const auto digestsBefore = stDelegator_->getPrunedBlocksDigests();
  ASSERT_NFF(dstRestart(false, FetchingState::NotFetching));
  const auto digestsafter = stDelegator_->getPrunedBlocksDigests();
  ASSERT_EQ(digestsBefore, digestsafter);
  testConfig_.productDbDeleteOnEnd = true;
}

// Check inter-versions compatibility: period to version 1.6 there is no RVT data in checkpoint.
// We would like to check that replica is able to reconstruct the whole RVT from storage, when no data is found in
// Checkpoint
TEST_F(BcStTest, bkpValidateRvbDataInitialSource) {
  // do not store RVB data in checkpoints (simulate v1.5)
  targetConfig_.enableStoreRvbDataDuringCheckpointing = false;
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  auto rvt = stDelegator_->getRvt();
  auto rvbm = stDelegator_->getRvbManager();
  std::string root_hash;

  // Node is up with an empty storage: Check that tree is empty and RVB data source is NIL
  ASSERT_EQ(rvbm->getRvbDataSource(), RVBManager::RvbDataInitialSource::NIL);
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  ASSERT_TRUE(rvt->getRootCurrentValueStr().empty());

  // Create some checkpoints. Since enableStoreRvbDataDuringCheckpointing=false, no RVB data is stored in dataStore
  // This will trigger the next stage to reconstruct from storage
  for (size_t i{testState_.minRepliedCheckpointNum}; i <= testState_.maxRepliedCheckpointNum; ++i) {
    stDelegator_->createCheckpointOfCurrentState(i);
  }

  // Restart the replica and see that it reconstructed the tree from storage - checkpoints are found but there is no
  // RVB data inside
  ASSERT_NFF(dstRestart(false, FetchingState::NotFetching));
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));
  rvt = stDelegator_->getRvt();
  rvbm = stDelegator_->getRvbManager();
  root_hash = rvt->getRootCurrentValueStr();
  ASSERT_EQ(rvbm->getRvbDataSource(), RVBManager::RvbDataInitialSource::FROM_STORAGE_RECONSTRUCTION);
  ASSERT_TRUE(!root_hash.empty());

  targetConfig_.enableStoreRvbDataDuringCheckpointing = true;
  ASSERT_NFF(dstRestart(false, FetchingState::NotFetching));

  // create new checkpoints this time with RVB data. Then restart the replica, expect RvbDataInitialSource ==
  // FROM_STORAGE_CP
  testState_.minRequiredBlockId = testState_.maxRequiredBlockId + 1;
  uint64_t nextCheckpointNum = datastore_->getLastStoredCheckpoint() + 1;
  testState_.maxRequiredBlockId += testConfig_.checkpointWindowSize;
  ASSERT_NFF(dataGen_->generateBlocks(appState_, testState_.minRequiredBlockId, testState_.maxRequiredBlockId));
  stDelegator_->createCheckpointOfCurrentState(nextCheckpointNum);
  ASSERT_NFF(dstRestart(false, FetchingState::NotFetching));
  rvt = stDelegator_->getRvt();
  rvbm = stDelegator_->getRvbManager();
  root_hash = rvt->getRootCurrentValueStr();
  ASSERT_EQ(rvbm->getRvbDataSource(), RVBManager::RvbDataInitialSource::FROM_STORAGE_CP);
  ASSERT_TRUE(!root_hash.empty());

  // Get the serialized data, and set it back, expect RvbDataInitialSource == FROM_NETWORK
  auto rvbData = rvbm->getRvbData();
  string s = rvbData.str();
  rvbm->setRvbData(s.data(), s.size(), testState_.minRequiredBlockId, testState_.maxRequiredBlockId);
  ASSERT_EQ(rvbm->getRvbDataSource(), RVBManager::RvbDataInitialSource::FROM_NETWORK);

  testConfig_.productDbDeleteOnEnd = true;
}

TEST_F(BcStTest, askChkptSummeriesFromStoppedNetwork) {
  ASSERT_NFF(initialize());
  ASSERT_NFF(dstStartRunningAndCollecting());
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg());
  ASSERT_NFF(getMissingblocksStage<void>());
  ASSERT_NFF(getReservedPagesStage());
  // Last stored checkpoint would have been updated by now.
  // Restarted replica is expected to resume from where it left.
  // As last cycle wasn't completed it should enter into GettingCheckpointSummaries stage.
  ASSERT_NFF(stDelegator_->enableFetchingState());
  ASSERT_NFF(dstRestart(false, FetchingState::GettingCheckpointSummaries));
  ASSERT_NFF(fakeSrcReplica_->replyAskForCheckpointSummariesMsg(false));
  ASSERT_NFF(getReservedPagesStage());
  // now validate completion
  ASSERT_NFF(dstValidateCycleEnd(10));
  ASSERT_NFF(compareAppStateblocks(testState_.maxRequiredBlockId - testState_.numBlocksToCollect + 1,
                                   testState_.maxRequiredBlockId));
}

class BcStTestParamFixture3 : public BcStTest,
                              public testing::WithParamInterface<tuple<size_t, size_t, size_t, size_t, bool>> {};

// generate blocks and checkpoint them to simulate consensus "advancing"
// Then validate the checkpoints and compare the ones in memory with the one built from the checkpoint data
TEST_P(BcStTestParamFixture3, bkpValidateCheckpointingWithConsensusCommitsAndPruning) {
  auto maxBlockIdOnFirstCycle = get<0>(GetParam()) * testConfig_.checkpointWindowSize / 100;
  auto numBlocksToAdd = get<1>(GetParam()) * testConfig_.checkpointWindowSize / 100;
  auto numBlocksToPrune = get<2>(GetParam()) * testConfig_.checkpointWindowSize / 100;
  auto totalCP = get<3>(GetParam());
  bool resetartBetweenCP = get<4>(GetParam());
  bool firstIteration = true;

  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  uint64_t nextCheckpointNum = datastore_->getLastStoredCheckpoint() ? datastore_->getLastStoredCheckpoint() : 1;

  uint64_t minBlockInCp = appState_.getGenesisBlockNum() + 1;
  uint64_t maxBlockInCp = maxBlockIdOnFirstCycle;
  uint64_t lastTotalLevels{}, lastTotalNodes{};
  uint64_t pruneTillBlockId = (numBlocksToPrune == 0) ? 0 : minBlockInCp + numBlocksToPrune - 1;
  ASSERT_GT(maxBlockInCp, minBlockInCp);
  for (size_t i{}; i < totalCP; ++i) {
    // Add blocks
    if (firstIteration || (numBlocksToAdd > 0)) {
      ASSERT_NFF(dataGen_->generateBlocks(appState_, minBlockInCp, maxBlockInCp));
      firstIteration = false;
    }
    // Prune blocks
    if (pruneTillBlockId > 0) {
      ASSERT_NFF(bkpPrune(pruneTillBlockId));
    }
    // create checkpoint
    if (resetartBetweenCP) {
      ASSERT_NFF(dstRestart(false, FetchingState::NotFetching));
    }
    stDelegator_->createCheckpointOfCurrentState(nextCheckpointNum);

    // Fetch the checkpoint, construct the tree, and check that number of nodes grows as expected
    ASSERT_TRUE(datastore_->hasCheckpointDesc(nextCheckpointNum));
    auto desc = datastore_->getCheckpointDesc(nextCheckpointNum);
    ASSERT_EQ(desc.checkpointNum, nextCheckpointNum);
    ASSERT_EQ(desc.maxBlockId, maxBlockInCp);
    ASSERT_TRUE(!desc.rvbData.empty());

    RangeValidationTree helper_rvt(GL, targetConfig_.RVT_K, targetConfig_.fetchRangeSize);
    auto rvt = stDelegator_->getRvt();

    std::istringstream rvb_data(std::string(reinterpret_cast<const char*>(desc.rvbData.data()), desc.rvbData.size()));
    ASSERT_TRUE(helper_rvt.setSerializedRvbData(rvb_data));
    ASSERT_NFF(stDelegator_->validateEqualRVTs(helper_rvt, *rvt));
    ASSERT_TRUE(!helper_rvt.getRootCurrentValueStr().empty());

    // leave for debug
    // helper_rvt.printToLog(LogPrintVerbosity::DETAILED);
    // rvt->printToLog(LogPrintVerbosity::DETAILED);

    // when only pruning, tree must shrink over timestartCollectingStateInternal
    // when only adding tree must grow over time
    if (lastTotalNodes > 0) {
      if ((numBlocksToAdd > 0) && numBlocksToPrune == 0) {
        ASSERT_LE(lastTotalNodes, rvt->totalNodes());
      } else if ((numBlocksToAdd == 0) && numBlocksToPrune > 0) {
        ASSERT_GE(lastTotalNodes, rvt->totalNodes());
      }
    }
    if (lastTotalLevels > 0) {
      if ((numBlocksToAdd > 0) && numBlocksToPrune == 0) {
        ASSERT_LE(lastTotalLevels, rvt->totalLevels());
      } else if ((numBlocksToAdd == 0) && numBlocksToPrune > 0) {
        ASSERT_GE(lastTotalLevels, rvt->totalLevels());
      }
    }
    lastTotalNodes = rvt->totalNodes();
    lastTotalLevels = rvt->totalLevels();

    nextCheckpointNum++;
    if (numBlocksToAdd) {
      minBlockInCp = maxBlockInCp + 1;
      maxBlockInCp = minBlockInCp + numBlocksToAdd - 1;
    }
    if (numBlocksToPrune > 0) {
      pruneTillBlockId += numBlocksToPrune - 1;
    }
  }
}

// All 1st 3 elements are % of testConfig_.checkpointWindowSize. They can be > 100% too.
// 1st element - max block ID to reach while adding blocks on 1st cycle
// 2nd element - # blocks to add on every next cycle
// 3rd element - # blocks to prune each cycle
// 4th element - # of cycles
// 5th element - resetart between checkpoints?
using BcStTestParamFixtureInput3 = tuple<size_t, size_t, size_t, size_t, bool>;
INSTANTIATE_TEST_CASE_P(BcStTest,
                        BcStTestParamFixture3,
                        ::testing::Values(
                            // Add blocks only
                            BcStTestParamFixtureInput3(100, 100, 0, 100, false),
                            // Prune blocks only
                            BcStTestParamFixtureInput3(1000, 0, 100, 9, false),
                            // Add1 blocks and Prune
                            BcStTestParamFixtureInput3(100, 100, 50, 100, false),
                            // Add blocks and Prune II
                            BcStTestParamFixtureInput3(100, 100, 5, 500, false),
                            // Add blocks only and restart between checkpointing
                            BcStTestParamFixtureInput3(100, 100, 0, 100, true),
                            // Prune blocks only and restart between checkpointing
                            BcStTestParamFixtureInput3(1000, 0, 100, 9, true),
                            // Add blocks and Prune and restart between checkpointing
                            BcStTestParamFixtureInput3(100, 100, 50, 100, true)), );

// This specific test reproduces a bug found on deployment. It Tests checkpointing with non-equal checkpoint window and
// some pruning between, The last pruning delete almost the whole blockchain.
TEST_F(BcStTest, bkpCheckpointingWithPruning) {
  targetConfig_.fetchRangeSize = 64;
  targetConfig_.maxNumberOfChunksInBatch = 64;
  targetConfig_.RVT_K = 1024;

  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());

  // 1st checkpoint window - last block Id added = 140, no pruning
  ASSERT_NFF(dataGen_->generateBlocks(appState_, 2, 140));
  stDelegator_->createCheckpointOfCurrentState(1);

  // 2nd checkpoint window - last block Id added = 291, prune till block ID 159
  ASSERT_NFF(dataGen_->generateBlocks(appState_, 141, 291));
  ASSERT_NFF(bkpPrune(159));
  stDelegator_->createCheckpointOfCurrentState(2);

  // 3rd checkpoint window - last block Id added = 441 , prune till block ID 268
  ASSERT_NFF(dataGen_->generateBlocks(appState_, 292, 441));
  ASSERT_NFF(bkpPrune(268));
  stDelegator_->createCheckpointOfCurrentState(3);

  // 4th checkpoint window - last block Id added = 592 , prune till block ID 528
  ASSERT_NFF(dataGen_->generateBlocks(appState_, 442, 592));
  ASSERT_NFF(bkpPrune(528));
  stDelegator_->createCheckpointOfCurrentState(4);
}

// This test runs a random scenario, similar to the one in bkpCheckpointingWithPruning
TEST_F(BcStTest, bkpRandomCheckpointingWithPruning) {
  static constexpr size_t chanceBlocksAddedDuringCheckpointPcg = 90;
  static constexpr size_t chanceBlocksPrunedDuringCheckpointPcg = 20;
  static constexpr size_t minBlocksToAdd = 1;
  static constexpr size_t maxBlocksToAdd = 150;
  static constexpr size_t numIterations = 150;

  targetConfig_.RVT_K = 64;
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());

  uint64_t genesisBlockId = appState_.getGenesisBlockNum();
  uint64_t lastRechableBlockId = appState_.getLastReachableBlockNum();
  uint64_t checkpointNumber{1};
  size_t numBlocksToAdd, numBlocksToPrune, pcgBlocksToPrune;
  for (size_t i{0}; i < numIterations; ++i, ++checkpointNumber) {
    if ((rand() % 101) <= chanceBlocksAddedDuringCheckpointPcg) {
      numBlocksToAdd = (rand() % (maxBlocksToAdd - minBlocksToAdd + 1)) + minBlocksToAdd;
    } else {
      numBlocksToAdd = 0;
    }
    if ((rand() % 101) <= chanceBlocksPrunedDuringCheckpointPcg) {
      // prune 1 to 100 percent of blocks
      pcgBlocksToPrune = (rand() % 100) + 1;
      numBlocksToPrune = (((lastRechableBlockId + numBlocksToAdd) - (genesisBlockId) + 1) * pcgBlocksToPrune) / 100;
    } else {
      numBlocksToPrune = 0;
    }

    // add blocks
    if (numBlocksToAdd > 0) {
      // leave for debug
      // auto fromBlockId{lastRechableBlockId + 1};
      // auto toBlockId{lastRechableBlockId + numBlocksToAdd};
      // LOG_ERROR(GL,
      //           "Add blocks:" << KVLOG(
      //               checkpointNumber, numBlocksToAdd, genesisBlockId, lastRechableBlockId, fromBlockId, toBlockId));
      ASSERT_NFF(dataGen_->generateBlocks(appState_, lastRechableBlockId + 1, lastRechableBlockId + numBlocksToAdd));
      ASSERT_EQ(appState_.getLastReachableBlockNum(), lastRechableBlockId + numBlocksToAdd);
      lastRechableBlockId = appState_.getLastReachableBlockNum();
    }
    // Prune only if not passing the last new reachable block
    if ((numBlocksToPrune > 0) && (genesisBlockId + numBlocksToPrune < lastRechableBlockId + numBlocksToAdd)) {
      // Prune % of total blocks, 1% to 100%.
      auto maxBlockIdToDelete{genesisBlockId + numBlocksToPrune - 1};
      if (maxBlockIdToDelete == lastRechableBlockId) {
        // Keep at least one block
        --maxBlockIdToDelete;
      }
      // leave for debug
      // LOG_ERROR(GL, "Prune blocks:" << KVLOG(checkpointNumber, numBlocksToPrune, genesisBlockId,
      // maxBlockIdToDelete));
      ASSERT_NFF(bkpPrune(maxBlockIdToDelete));
      genesisBlockId = maxBlockIdToDelete + 1;
      ASSERT_EQ(appState_.getGenesisBlockNum(), genesisBlockId);
    }
    // checkpointing stage
    stDelegator_->createCheckpointOfCurrentState(checkpointNumber);
  }
}

// Test correctness of RVB Data conflict detection. RVB data digest is part of the checkpoint and should be compared
// in addition to comparing Reserved pages digest, and current state digests.
TEST_F(BcStTest, bkpTestRvbDataConflictDetection) {
  // do not store RVB data in checkpoints (simulate v1.5)
  ASSERT_NFF(initialize());
  ASSERT_NFF(cmnStartRunning());
  size_t i{testState_.minRepliedCheckpointNum};

  // Node is up with an empty storage: Check that tree is empty and RVB data source is NIL
  stDelegator_->createCheckpointOfCurrentState(i);
  ASSERT_NFF(dataGen_->generateBlocks(appState_, appState_.getGenesisBlockNum() + 1, testState_.maxRequiredBlockId));

  // Create few checkpoints and check that the checkpoint values in the data store are the same like the ones returned
  // by getDigestOfCheckpoint
  for (; i <= testState_.maxRepliedCheckpointNum; ++i) {
    ASSERT_TRUE(datastore_->hasCheckpointDesc(i));

    DataStore::CheckpointDesc desc = datastore_->getCheckpointDesc(i);
    digest::Digest stateDigest, reservedPagesDigest, rvbDataDigest;
    uint64_t outBlockId;

    stateTransfer_->getDigestOfCheckpoint(
        i, sizeof(Digest), outBlockId, stateDigest.content(), reservedPagesDigest.content(), rvbDataDigest.content());
    ASSERT_EQ(desc.checkpointNum, i);
    ASSERT_EQ(desc.maxBlockId, outBlockId);
    ASSERT_TRUE(!memcmp(desc.digestOfMaxBlockId.content(), stateDigest.content(), sizeof(Digest)));
    ASSERT_TRUE(!memcmp(desc.digestOfResPagesDescriptor.content(), reservedPagesDigest.content(), sizeof(Digest)));

    if (i == testState_.minRepliedCheckpointNum) {
      // first checkpoint has a default RVB
      const auto& defaultRvbDataDigest = stDelegator_->computeDefaultRvbDataDigest();
      ASSERT_TRUE(!memcmp(defaultRvbDataDigest.content(), rvbDataDigest.content(), sizeof(Digest)));
    } else {
      digest::DigestUtil::Context digestCtx;
      Digest rvbDataDigest;
      auto rvbDataSize = desc.rvbData.size();
      digestCtx.update(reinterpret_cast<const char*>(desc.rvbData.data()), rvbDataSize);
      digestCtx.update(reinterpret_cast<const char*>(&rvbDataSize), sizeof(rvbDataSize));
      digestCtx.writeDigest(rvbDataDigest.getForUpdate());
      ASSERT_TRUE(!memcmp(rvbDataDigest.content(), rvbDataDigest.content(), sizeof(Digest)));
    }
    // Create the next checkpoint
    stDelegator_->createCheckpointOfCurrentState(i + 1);
  }
}

}  // namespace bftEngine::bcst::impl

int main(int argc, char** argv) {
  srand(time(NULL));
  UserInput::getInstance()->extractUserInput(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_death_test_style =
      "threadsafe";  // mitigate the risks of testing in a possibly multithreaded environment

  return RUN_ALL_TESTS();
}