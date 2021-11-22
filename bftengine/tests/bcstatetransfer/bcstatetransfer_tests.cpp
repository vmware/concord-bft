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
#include "Logger.hpp"

#include "ReservedPagesMock.hpp"
#include "EpochManager.hpp"
#include "messages/PrePrepareMsg.hpp"

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
using concord::storage::rocksdb::Client;
using concord::storage::rocksdb::KeyComparator;
#endif
using std::chrono::milliseconds;
using namespace std;
using random_bytes_engine = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char>;
using namespace bftEngine::bcst;

namespace bftEngine::bcst::impl {

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
      256,                // maxNumberOfChunksInBatch
      kMaxBlockSize,      // maxBlockSize
      256 * 1024 * 1024,  // maxPendingDataFromSourceReplica
      2048,               // maxNumOfReservedPages
      4096,               // sizeOfReservedPage
      600,                // gettingMissingBlocksSummaryWindowSize
      10,                 // minPrePrepareMsgsForPrimaryAwarness
      300,                // refreshTimerMs
      2500,               // checkpointSummariesRetransmissionTimeoutMs
      60000,              // maxAcceptableMsgDelayMs
      0,                  // sourceReplicaReplacementTimeoutMs
      2000,               // fetchRetransmissionTimeoutMs
      2,                  // maxFetchRetransmissions
      5,                  // metricsDumpIntervalSec
      true,               // enableReservedPages
      true                // enableSourceBlocksPreFetch
  };
}

// TBD BC-14432
class MsgGenerator {
 private:
  static constexpr ViewNum view_num_ = 1u;
  static constexpr SeqNum seq_num_ = 2u;
  static constexpr CommitPath commit_path_ = CommitPath::OPTIMISTIC_FAST;

 public:
  std::unique_ptr<MessageBase> generatePrePrepareMsg(ReplicaId sender_id) {
    return make_unique<PrePrepareMsg>(sender_id, view_num_, seq_num_, commit_path_, 0);
  }
};

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
   * Constants
   * You may decide a constant is configurable by moving it into the 'Configurables' part
   * In some cases you might need to write additional code to support the new configuration value
   */
  static constexpr char BCST_DB[] = "./bcst_db";
  static constexpr char MOCKED_BCST_DB[] = "./mocked_bcst_db";

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
  uint32_t lastReachedcheckpointNum = 10;
  bool productDbDeleteOnStart = true;
  bool productDbDeleteOnEnd = true;
  bool mockDbDeleteOnStart = true;
  bool mockDbDeleteOnEnd = true;
};

static inline std::ostream& operator<<(std::ostream& os, const TestConfig& c) {
  os << std::boolalpha
     << KVLOG(c.BCST_DB,
              c.MOCKED_BCST_DB,
              c.maxNumOfRequiredStoredCheckpoints,
              c.numberOfRequiredReservedPages,
              c.minNumberOfUpdatedReservedPages,
              c.maxNumberOfUpdatedReservedPages,
              c.checkpointWindowSize,
              c.minBlockDataSize,
              c.lastReachedcheckpointNum,
              c.productDbDeleteOnStart,
              c.productDbDeleteOnEnd,
              c.mockDbDeleteOnStart,
              c.mockDbDeleteOnEnd);
  return os;
}

/////////////////////////////////////////////////////////
// TestState
//
// Test initial state is calculated usually as test starts. You shouldn't change a test state directly, you can alter it
// by changing test infra code, product code, or test configuration (for example).
/////////////////////////////////////////////////////////
struct TestState {
  uint64_t expectedFirstRequiredBlockNum;
  uint64_t expectedLastRequiredBlockNum;
};

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

/////////////////////////////////////////////////////////
// MockedSources
//
// Mock a source or multiple sources. Supposed to work against real St product destination.
/////////////////////////////////////////////////////////
// TODO(GL) - consider transforming to gMock? + the mocked interfaces (replica, app)
class MockedSources {
 public:
  MockedSources(Config& targetConfig,
                TestConfig& testConfig,
                TestReplica& replica,
                const std::shared_ptr<BCStateTran>& stateTransfer);
  ~MockedSources();

  // Source (Mock) Replies
  void replyAskForCheckpointSummariesMsg();
  void replyFetchBlocksMsg();
  void replyResPagesMsg(bool& outDoneSending);

  // Helper functions
  void generateReservedPages(uint64_t checkpointNumber);
  void generateBlocks(uint64_t fromBlockId, uint64_t toBlockId);
  size_t clearSentMessagesByMessageType(uint16_t type) { return filterSentMessagesByMessageType(type, false); }
  size_t keepSentMessagesByMessageType(uint16_t type) { return filterSentMessagesByMessageType(type, true); }

 private:
  size_t filterSentMessagesByMessageType(uint16_t type, bool keep);

 protected:
  static constexpr char MOCKED_BCST_DB[] = "./mocked_bcst_db";
  DataStore* mockedDatastore_ = nullptr;
  std::unique_ptr<char[]> rawVBlock_;
  std::map<uint64_t, std::shared_ptr<Block>> generatedBlocks_;  // map: blockId -> Block
  std::optional<FetchResPagesMsg> lastReceivedFetchResPagesMsg_;
  Config& targetConfig_;
  TestConfig& testConfig_;
  TestAppState appState_;
  TestReplica& replica_;
  const std::shared_ptr<BCStateTran>& stateTransfer_;
};  // class MockedSources

/////////////////////////////////////////////////////////
// MockedSources - definition
/////////////////////////////////////////////////////////

MockedSources::MockedSources(Config& targetConfig,
                             TestConfig& testConfig,
                             TestReplica& replica,
                             const std::shared_ptr<BCStateTran>& stateTransfer)
    : targetConfig_(targetConfig), testConfig_(testConfig), replica_(replica), stateTransfer_(stateTransfer) {
  if (testConfig_.mockDbDeleteOnStart) deleteBcStateTransferDbFolder(testConfig_.MOCKED_BCST_DB);
#ifdef USE_ROCKSDB
  // create a mocked data store
  auto* mocked_db_key_comparator = new concord::kvbc::v1DirectKeyValue::DBKeyComparator();
  concord::storage::IDBClient::ptr mockedDbc(new concord::storage::rocksdb::Client(
      string(MOCKED_BCST_DB), make_unique<KeyComparator>(mocked_db_key_comparator)));
  mockedDbc->init();
  mockedDatastore_ = new DBDataStore(mockedDbc,
                                     targetConfig_.sizeOfReservedPage,
                                     make_shared<concord::storage::v1DirectKeyValue::STKeyManipulator>(),
                                     targetConfig_.enableReservedPages);
  mockedDatastore_->setNumberOfReservedPages(testConfig_.numberOfRequiredReservedPages);
#else
  concord::storage::IDBClient::ptr dbc2(new concord::storage::memorydb::Client(comparator));
  mockedDatastore_ = new InMemoryDataStore(targetConfig_.sizeOfReservedPage);
#endif
}

MockedSources::~MockedSources() {
  delete mockedDatastore_;
  if (testConfig_.mockDbDeleteOnEnd) deleteBcStateTransferDbFolder(testConfig_.MOCKED_BCST_DB);
}

void MockedSources::generateReservedPages(uint64_t checkpointNumber) {
  uint32_t idx = 0;
  std::unique_ptr<char[]> buffer(new char[targetConfig_.sizeOfReservedPage]);
  for (uint32_t pageId{0}; pageId < testConfig_.maxNumberOfUpdatedReservedPages; ++pageId) {
    ConcordAssertLT(idx, testConfig_.maxNumberOfUpdatedReservedPages);
    STDigest pageDigest;
    fillRandomBytes(buffer.get(), targetConfig_.sizeOfReservedPage);
    BCStateTran::computeDigestOfPage(
        pageId, checkpointNumber, buffer.get(), targetConfig_.sizeOfReservedPage, pageDigest);
    ASSERT_TRUE(!pageDigest.isZero());
    mockedDatastore_->setResPage(pageId, checkpointNumber, pageDigest, buffer.get());
    idx++;
  }
}

/**
 * toBlockId is assumed to be a checkpoint block, we also assume
 * generatedBlocks_ is empty
 */
void MockedSources::generateBlocks(uint64_t fromBlockId, uint64_t toBlockId) {
  char buff[kMaxBlockSize];

  ConcordAssertEQ(toBlockId % testConfig_.checkpointWindowSize, 0);
  ConcordAssert(generatedBlocks_.empty());
  ConcordAssertGT(fromBlockId, 1);

  auto maxBlockDataSize = Block::calcMaxDataSize();
  for (size_t i = fromBlockId; i <= toBlockId; ++i) {
    uint32_t dataSize = static_cast<uint32_t>(rand()) % (maxBlockDataSize - testConfig_.minBlockDataSize + 1) +
                        testConfig_.minBlockDataSize;
    ConcordAssertLE(dataSize, maxBlockDataSize);
    fillRandomBytes(buff, dataSize);
    std::shared_ptr<Block> blk;
    StateTransferDigest prevBlkDigest{1};
    if (i == fromBlockId) {
      blk = Block::createFromData(dataSize, buff, i, prevBlkDigest);
    } else {
      auto prevBlk = generatedBlocks_[i - 1];
      computeBlockDigest(
          prevBlk->blockId, reinterpret_cast<const char*>(prevBlk.get()), prevBlk->totalBlockSize, &prevBlkDigest);
      blk = Block::createFromData(dataSize, buff, i, prevBlkDigest);
    }
    generatedBlocks_[i] = blk;
  }
}

/**
 * keep is true: keep only messages with msg->type == type
 * keep is false: keep all message with msg->type != type
 * return number of messages deleted
 */
size_t MockedSources::filterSentMessagesByMessageType(uint16_t type, bool keep) {
  auto& sent_messages_ = replica_.sent_messages_;
  size_t n{0};
  for (auto it = sent_messages_.begin(); it != sent_messages_.end();) {
    auto header = reinterpret_cast<BCStateTranBaseMsg*>((*it).data_.get());
    // This block can be much shorter. For better readibility, keep it like that
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

void MockedSources::replyAskForCheckpointSummariesMsg() {
  const uint64_t toCheckpoint = testConfig_.lastReachedcheckpointNum;
  const uint64_t fromCheckpoint =
      testConfig_.lastReachedcheckpointNum - testConfig_.maxNumOfRequiredStoredCheckpoints + 1;
  vector<unique_ptr<CheckpointSummaryMsg>> checkpointSummaryReplies;

  // Generate all the blocks until lastBlock of the last checkpoint
  auto lastBlockId = (toCheckpoint + 1) * testConfig_.checkpointWindowSize;
  generateBlocks(appState_.getGenesisBlockNum() + 1, lastBlockId);

  // Compute digest of last block
  StateTransferDigest lastBlockDigest;
  const auto& lastBlk = generatedBlocks_[lastBlockId];
  computeBlockDigest(
      lastBlockId, reinterpret_cast<const char*>(lastBlk.get()), lastBlk->totalBlockSize, &lastBlockDigest);

  // build a single copy of all replied messages, push to a vector
  const auto& firstMsg = replica_.sent_messages_.front();
  auto firstAskForCheckpointSummariesMsg = reinterpret_cast<AskForCheckpointSummariesMsg*>(firstMsg.data_.get());
  for (uint64_t i = toCheckpoint; i >= fromCheckpoint; i--) {
    unique_ptr<CheckpointSummaryMsg> reply = make_unique<CheckpointSummaryMsg>();
    reply->checkpointNum = i;
    reply->lastBlock = (i + 1) * testConfig_.checkpointWindowSize;
    auto digestBytes = reply->digestOfLastBlock.getForUpdate();
    if (i == toCheckpoint)
      memcpy(digestBytes, &lastBlockDigest, sizeof(lastBlockDigest));
    else {
      auto blk = generatedBlocks_[reply->lastBlock + 1];
      memcpy(digestBytes, &blk->digestPrev, sizeof(blk->digestPrev));
    }
    generateReservedPages(i);
    DataStore::ResPagesDescriptor* resPagesDesc = mockedDatastore_->getResPagesDescriptor(i);
    STDigest digestOfResPagesDescriptor;
    BCStateTran::computeDigestOfPagesDescriptor(resPagesDesc, digestOfResPagesDescriptor);

    reply->digestOfResPagesDescriptor = digestOfResPagesDescriptor;
    reply->requestMsgSeqNum = firstAskForCheckpointSummariesMsg->msgSeqNum;
    checkpointSummaryReplies.push_back(move(reply));
  }

  // send replies from all replicas (shuffle the requests to get a random reply order)
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(replica_.sent_messages_), std::end(replica_.sent_messages_), rng);
  for (const auto& reply : checkpointSummaryReplies) {
    for (auto& request : replica_.sent_messages_) {
      CheckpointSummaryMsg* uniqueReply = new CheckpointSummaryMsg();
      *uniqueReply = *reply.get();
      stateTransfer_->onMessage(uniqueReply, sizeof(CheckpointSummaryMsg), request.to_);
    }
  }
  ASSERT_EQ(clearSentMessagesByMessageType(MsgType::AskForCheckpointSummaries), targetConfig_.numReplicas - 1);
}

void MockedSources::replyFetchBlocksMsg() {
  ASSERT_EQ(replica_.sent_messages_.size(), 1);
  const auto& msg = replica_.sent_messages_.front();
  assertMsgType(msg, MsgType::FetchBlocks);
  auto fetchBlocksMsg = reinterpret_cast<FetchBlocksMsg*>(msg.data_.get());
  uint64_t nextBlockId = fetchBlocksMsg->lastRequiredBlock;
  size_t numOfSentChunks = 0;

  // For now we assume no chunking is supported
  ConcordAssertEQ(fetchBlocksMsg->lastKnownChunkInLastRequiredBlock, 0);
  ConcordAssertLE(fetchBlocksMsg->firstRequiredBlock, fetchBlocksMsg->lastRequiredBlock);

  while (true) {
    const auto& blk = generatedBlocks_[nextBlockId];
    ItemDataMsg* itemDataMsg = ItemDataMsg::alloc(blk->totalBlockSize);
    bool lastInBatch = ((numOfSentChunks + 1) >= targetConfig_.maxNumberOfChunksInBatch) ||
                       ((nextBlockId - 1) < fetchBlocksMsg->firstRequiredBlock);
    itemDataMsg->lastInBatch = lastInBatch;
    itemDataMsg->blockNumber = nextBlockId;
    itemDataMsg->totalNumberOfChunksInBlock = 1;
    itemDataMsg->chunkNumber = 1;
    itemDataMsg->requestMsgSeqNum = fetchBlocksMsg->msgSeqNum;
    itemDataMsg->dataSize = blk->totalBlockSize;
    memcpy(itemDataMsg->data, blk.get(), blk->totalBlockSize);
    stateTransfer_->onMessage(itemDataMsg, itemDataMsg->size(), msg.to_, std::chrono::steady_clock::now());
    if (lastInBatch) {
      break;
    }
    --nextBlockId;
    ++numOfSentChunks;
  }
  replica_.sent_messages_.pop_front();
}

// To ASSERT_ / EXPECT_  inside this function, we must pass output as a parameter
void MockedSources::replyResPagesMsg(bool& outDoneSending) {
  ASSERT_EQ(replica_.sent_messages_.size(), 1);
  const auto& msg = replica_.sent_messages_.front();
  assertMsgType(msg, MsgType::FetchResPages);
  auto fetchResPagesMsg = reinterpret_cast<FetchResPagesMsg*>(msg.data_.get());

  // check if need to create a vBlock
  if (!lastReceivedFetchResPagesMsg_ ||
      lastReceivedFetchResPagesMsg_.value().lastCheckpointKnownToRequester !=
          fetchResPagesMsg->lastCheckpointKnownToRequester ||
      lastReceivedFetchResPagesMsg_.value().requiredCheckpointNum != fetchResPagesMsg->requiredCheckpointNum) {
    // need to generate pages - for now, lets assume all pages need to be updated
    uint32_t numberOfUpdatedPages = testConfig_.maxNumberOfUpdatedReservedPages;
    const uint32_t elementSize = sizeof(BCStateTran::ElementOfVirtualBlock) + targetConfig_.sizeOfReservedPage - 1;
    const uint32_t size = sizeof(BCStateTran::HeaderOfVirtualBlock) + numberOfUpdatedPages * elementSize;
    lastReceivedFetchResPagesMsg_ = *fetchResPagesMsg;

    // allocate and fill vBlock
    rawVBlock_.reset(new char[size]);
    std::fill(rawVBlock_.get(), rawVBlock_.get() + size, 0);
    BCStateTran::HeaderOfVirtualBlock* header = reinterpret_cast<BCStateTran::HeaderOfVirtualBlock*>(rawVBlock_.get());
    header->lastCheckpointKnownToRequester = fetchResPagesMsg->lastCheckpointKnownToRequester;
    header->numberOfUpdatedPages = numberOfUpdatedPages;
    char* elements = rawVBlock_.get() + sizeof(BCStateTran::HeaderOfVirtualBlock);
    uint32_t idx = 0;

    DataStore::ResPagesDescriptor* resPagesDesc =
        mockedDatastore_->getResPagesDescriptor(fetchResPagesMsg->requiredCheckpointNum);

    for (uint32_t pageId{0}; pageId < numberOfUpdatedPages; ++pageId) {
      ConcordAssertLT(idx, numberOfUpdatedPages);
      BCStateTran::ElementOfVirtualBlock* currElement =
          reinterpret_cast<BCStateTran::ElementOfVirtualBlock*>(elements + idx * elementSize);
      currElement->pageId = pageId;
      currElement->checkpointNumber = fetchResPagesMsg->requiredCheckpointNum;
      currElement->pageDigest = resPagesDesc->d[pageId].pageDigest;
      ASSERT_TRUE(!currElement->pageDigest.isZero());
      mockedDatastore_->getResPage(
          pageId, currElement->checkpointNumber, nullptr, currElement->page, targetConfig_.sizeOfReservedPage);
      ASSERT_TRUE(!currElement->pageDigest.isZero());
      idx++;
    }
  }
  ASSERT_TRUE(rawVBlock_.get());
  uint32_t vblockSize = BCStateTran::getSizeOfVirtualBlock(rawVBlock_.get(), targetConfig_.sizeOfReservedPage);
  ASSERT_GE(vblockSize, sizeof(BCStateTran::HeaderOfVirtualBlock));
  ASSERT_TRUE(BCStateTran::checkStructureOfVirtualBlock(
      rawVBlock_.get(), vblockSize, targetConfig_.sizeOfReservedPage, stateTransfer_->logger_));

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
    outMsg->blockNumber = BCStateTran::ID_OF_VBLOCK_RES_PAGES;
    outMsg->totalNumberOfChunksInBlock = numOfChunksInVBlock;
    outMsg->chunkNumber = nextChunk;
    outMsg->dataSize = chunkSize;
    outMsg->lastInBatch =
        ((numOfSentChunks + 1) >= targetConfig_.maxNumberOfChunksInBatch || (nextChunk == numOfChunksInVBlock));
    memcpy(outMsg->data, pRawChunk, chunkSize);

    stateTransfer_->onMessage(outMsg, outMsg->size(), msg.to_, std::chrono::steady_clock::now());
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
  replica_.sent_messages_.pop_front();
}

/////////////////////////////////////////////////////////
// BcStTest - Test fixture for blockchain state transfer tests
/////////////////////////////////////////////////////////
class BcStTest : public ::testing::Test {
  using MetricKeyValPairs = std::vector<std::pair<std::string, uint64_t>>;

 protected:
  // Initializers and finalizers
  void SetUp() override;
  void TearDown() override;
  void initialize();

  // State Transfer proxies (BcStTest is a friend class)
  void onTimerImp() { stateTransfer_->onTimerImp(); }
  SourceSelector& getSourceSelector() { return stateTransfer_->sourceSelector_; }

  // Tests common
  void startStateTransfer();
  void assertAskForCheckpointSummariesSent(uint64_t checkpoint_num);
  void assertFetchBlocksMsgSent(uint64_t firstRequiredBlock, uint64_t lastRequiredBlock);
  void assertFetchResPagesMsgSent();

  // Source Selector metric
  void assertSourceSelectorMetricKeyVal(const std::string& key, uint64_t val);
  void validateSourceSelectorMetricCounters(const MetricKeyValPairs& metric_counters);

 private:
  void printConfiguration();

  // Test Target vars - all the below are used to create a tested replica
 protected:
  Config targetConfig_ = targetConfig();
  TestAppState appState_;
  TestReplica replica_;
  std::shared_ptr<BCStateTran> stateTransfer_;
  DataStore* datastore_ = nullptr;

  // Test body related members
  std::unique_ptr<MockedSources> mockedSrc_;
  TestConfig testConfig_;
  TestState test_state_;
  bool initialized_ = false;
  MsgGenerator msg_generator_;
  bftEngine::test::ReservedPagesMock<EpochManager> res_pages_mock_;
};  // class BcStTest

/////////////////////////////////////////////////////////
// BcStTest - definition
/////////////////////////////////////////////////////////
void BcStTest::SetUp() {
  // Uncomment if needed after setting the required log level
#ifdef USE_LOG4CPP
  log4cplus::LogLevel logLevel = log4cplus::INFO_LOG_LEVEL;
  // logging::Logger::getInstance("serializable").setLogLevel(logLevel);
  // logging::Logger::getInstance("concord.bft.st.dbdatastore").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft.st.dst").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft.st.src").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.util.handoff").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft").setLogLevel(logLevel);
  // logging::Logger::getInstance("rocksdb").setLogLevel(logLevel);
#else
  logging::LogLevel logLevel = logging::LogLevel::info;
  // logging::Logger::getInstance("serializable").setLogLevel(logLevel);
  // logging::Logger::getInstance("concord.bft.st.dbdatastore").setLogLevel(logLevel);
  // logging::Logger::getInstance("rocksdb").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft.st.dst").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft.st.src").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.util.handoff").setLogLevel(logLevel);
  logging::Logger::getInstance("concord.bft").setLogLevel(logLevel);
#endif
}

void BcStTest::TearDown() {
  if (stateTransfer_) {
    // Must stop running before destruction
    if (stateTransfer_->isRunning()) {
      stateTransfer_->stopRunning();
    }
  }
  if (testConfig_.productDbDeleteOnEnd) deleteBcStateTransferDbFolder(testConfig_.BCST_DB);
}

// We should call this function after we made all the needed overrides (if needed) for:
// 1) testConfig_
// 2) targetConfig_
void BcStTest::initialize() {
  printConfiguration();
  if (testConfig_.productDbDeleteOnStart) deleteBcStateTransferDbFolder(testConfig_.BCST_DB);

  // For now we assume no chunking is supported
  ConcordAssertEQ(targetConfig_.maxChunkSize, targetConfig_.maxBlockSize);

  test_state_.expectedFirstRequiredBlockNum = appState_.getGenesisBlockNum() + 1;
  test_state_.expectedLastRequiredBlockNum =
      (testConfig_.lastReachedcheckpointNum + 1) * testConfig_.checkpointWindowSize;

#ifdef USE_ROCKSDB
  auto* db_key_comparator = new concord::kvbc::v1DirectKeyValue::DBKeyComparator();
  concord::storage::IDBClient::ptr dbc(new concord::storage::rocksdb::Client(
      string(testConfig_.BCST_DB), make_unique<KeyComparator>(db_key_comparator)));
  dbc->init();
  datastore_ = new DBDataStore(dbc,
                               targetConfig_.sizeOfReservedPage,
                               make_shared<concord::storage::v1DirectKeyValue::STKeyManipulator>(),
                               targetConfig_.enableReservedPages);
#else
  auto comparator = concord::storage::memorydb::KeyComparator(db_key_comparator);
  concord::storage::IDBClient::ptr dbc1(new concord::storage::memorydb::Client(comparator));
  datastore_ = new InMemoryDataStore(targetConfig_.sizeOfReservedPage);
#endif

  stateTransfer_.reset(new BCStateTran(targetConfig_, &appState_, datastore_));
  stateTransfer_->init(testConfig_.maxNumOfRequiredStoredCheckpoints,
                       testConfig_.numberOfRequiredReservedPages,
                       targetConfig_.sizeOfReservedPage);
  for (uint32_t i{0}; i < testConfig_.numberOfRequiredReservedPages; ++i) {
    stateTransfer_->zeroReservedPage(i);
  }
  mockedSrc_.reset(new MockedSources(targetConfig_, testConfig_, replica_, stateTransfer_));
  ASSERT_FALSE(stateTransfer_->isRunning());
  stateTransfer_->startRunning(&replica_);
  ASSERT_TRUE(stateTransfer_->isRunning());
  ASSERT_EQ(BCStateTran::FetchingState::NotFetching, stateTransfer_->getFetchingState());
  ASSERT_LE(testConfig_.minNumberOfUpdatedReservedPages, testConfig_.maxNumberOfUpdatedReservedPages);
  ASSERT_LE(test_state_.expectedFirstRequiredBlockNum, test_state_.expectedLastRequiredBlockNum);
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  initialized_ = true;
}

void BcStTest::startStateTransfer() {
  ASSERT_TRUE(initialized_);
  stateTransfer_->startCollectingState();
  ASSERT_EQ(BCStateTran::FetchingState::GettingCheckpointSummaries, stateTransfer_->getFetchingState());
  auto min_relevant_checkpoint = 1;
  assertAskForCheckpointSummariesSent(min_relevant_checkpoint);
}

void BcStTest::assertAskForCheckpointSummariesSent(uint64_t checkpoint_num) {
  ASSERT_EQ(replica_.sent_messages_.size(), targetConfig_.numReplicas - 1);

  set<uint16_t> dests;
  for (auto& msg : replica_.sent_messages_) {
    auto p = dests.insert(msg.to_);
    ASSERT_TRUE(p.second);  // destinations must be unique
    assertMsgType(msg, MsgType::AskForCheckpointSummaries);
    auto askMsg = reinterpret_cast<AskForCheckpointSummariesMsg*>(msg.data_.get());
    ASSERT_TRUE(askMsg->msgSeqNum > 0);
    ASSERT_EQ(checkpoint_num, askMsg->minRelevantCheckpointNum);
  }
}

void BcStTest::assertFetchBlocksMsgSent(uint64_t firstRequiredBlock, uint64_t lastRequiredBlock) {
  ASSERT_EQ(BCStateTran::FetchingState::GettingMissingBlocks, stateTransfer_->getFetchingState());
  auto currentSourceId = getSourceSelector().currentReplica();
  ASSERT_NE(currentSourceId, NO_REPLICA);
  ASSERT_EQ(datastore_->getFirstRequiredBlock(), firstRequiredBlock);
  ASSERT_EQ(datastore_->getLastRequiredBlock(), lastRequiredBlock);
  ASSERT_EQ(replica_.sent_messages_.size(), 1);
  assertMsgType(replica_.sent_messages_.front(), MsgType::FetchBlocks);
  ASSERT_EQ(replica_.sent_messages_.front().to_, currentSourceId);
}

void BcStTest::assertFetchResPagesMsgSent() {
  ASSERT_EQ(BCStateTran::FetchingState::GettingMissingResPages, stateTransfer_->getFetchingState());
  auto currentSourceId = getSourceSelector().currentReplica();
  ASSERT_NE(currentSourceId, NO_REPLICA);
  ASSERT_EQ(datastore_->getFirstRequiredBlock(), datastore_->getLastRequiredBlock());
  ASSERT_EQ(replica_.sent_messages_.size(), 1);
  assertMsgType(replica_.sent_messages_.front(), MsgType::FetchResPages);
  ASSERT_EQ(replica_.sent_messages_.front().to_, currentSourceId);
}

void BcStTest::printConfiguration() {
  LOG_INFO(GL, "testConfig_:" << std::boolalpha << testConfig_);
  LOG_INFO(GL, "targetConfig_:" << std::boolalpha << targetConfig_);
}

void BcStTest::assertSourceSelectorMetricKeyVal(const std::string& key, uint64_t val) {
  if (key == "total_replacements_") {
    ASSERT_EQ(getSourceSelector().metrics_.total_replacements_.Get().Get(), val);
  } else if (key == "replacement_due_to_no_source_") {
    ASSERT_EQ(getSourceSelector().metrics_.replacement_due_to_no_source_.Get().Get(), val);
  } else if (key == "replacement_due_to_bad_data_") {
    ASSERT_EQ(getSourceSelector().metrics_.replacement_due_to_bad_data_.Get().Get(), val);
  } else if (key == "replacement_due_to_retransmission_timeout_") {
    ASSERT_EQ(getSourceSelector().metrics_.replacement_due_to_retransmission_timeout_.Get().Get(), val);
  } else if (key == "replacement_due_to_periodic_change_") {
    ASSERT_EQ(getSourceSelector().metrics_.replacement_due_to_periodic_change_.Get().Get(), val);
  } else if (key == "replacement_due_to_source_same_as_primary_") {
    ASSERT_EQ(getSourceSelector().metrics_.replacement_due_to_source_same_as_primary_.Get().Get(), val);
  } else {
    FAIL() << "Unexpected key!";
  }
}

void BcStTest::validateSourceSelectorMetricCounters(const MetricKeyValPairs& metric_counters) {
  for (auto& [key, val] : metric_counters) {
    assertSourceSelectorMetricKeyVal(key, val);
  }
}

/////////////////////////////////////////////////////////
//
//                BcStTest Test Cases
//
/////////////////////////////////////////////////////////

// Validate a full state transfer
TEST_F(BcStTest, dstFullStateTransfer) {
  initialize();
  ASSERT_NO_FATAL_FAILURE(startStateTransfer());
  mockedSrc_->replyAskForCheckpointSummariesMsg();
  while (true) {
    ASSERT_NO_FATAL_FAILURE(
        assertFetchBlocksMsgSent(test_state_.expectedFirstRequiredBlockNum, test_state_.expectedLastRequiredBlockNum));
    mockedSrc_->replyFetchBlocksMsg();
    if (test_state_.expectedLastRequiredBlockNum <= targetConfig_.maxNumberOfChunksInBatch) break;
    test_state_.expectedLastRequiredBlockNum -= targetConfig_.maxNumberOfChunksInBatch;
    // There might be pending jobs for putBlock, we need to wait some time and then finalize them by calling
    // onTimerImp()test
    this_thread::sleep_for(chrono::milliseconds(20));
    onTimerImp();
  }
  ASSERT_NO_FATAL_FAILURE(assertFetchResPagesMsgSent());
  bool doneSending = false;
  while (!doneSending) mockedSrc_->replyResPagesMsg(doneSending);
  // now validate commpletion
  ASSERT_TRUE(replica_.onTransferringCompleteCalled_);
  ASSERT_EQ(BCStateTran::FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

/**
 * Check that only actual resources are inserted into source selector's actualSources_
 * This is done by triggering multiple retransmissions and then  source replacements, and checking that only the sources
 * which replied are in the list, and in the expected order.
 * The check is done only for FetchingMissingblocks state sources.
 */
TEST_F(BcStTest, dstValidateRealSourceListReported) {
  initialize();
  uint16_t currentSrc;
  /**
   * Add callback to ST to be executed when transferring is completed.
   * Here we validate that only one actual source is in the sources list, although we had multiple
   * retransmissions and a few sources were selected.
   */
  stateTransfer_->addOnTransferringCompleteCallback([this, &currentSrc](std::uint64_t) {
    const auto& sources_ = getSourceSelector().getActualSources();
    ASSERT_EQ(sources_.size(), 1);
    ASSERT_EQ(sources_[0], currentSrc);
  });

  ASSERT_NO_FATAL_FAILURE(startStateTransfer());
  mockedSrc_->replyAskForCheckpointSummariesMsg();

  // Trigger multiple retransmissions to 2 sources. none will be answered, then we expect the replica to move into the
  // 3rd source
  auto& sourceSelector = getSourceSelector();
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
      ASSERT_EQ(replica_.sent_messages_.size(), 1);
      ASSERT_EQ(replica_.sent_messages_.front().to_, currentSrc);
      replica_.sent_messages_.clear();
      this_thread::sleep_for(chrono::milliseconds(targetConfig_.fetchRetransmissionTimeoutMs + 10));
      onTimerImp();
    }
  }
  ASSERT_EQ(replica_.sent_messages_.size(), 1);
  currentSrc = sourceSelector.currentReplica();
  while (true) {
    ASSERT_NO_FATAL_FAILURE(
        assertFetchBlocksMsgSent(test_state_.expectedFirstRequiredBlockNum, test_state_.expectedLastRequiredBlockNum));
    mockedSrc_->replyFetchBlocksMsg();
    if (test_state_.expectedLastRequiredBlockNum <= targetConfig_.maxNumberOfChunksInBatch) break;
    test_state_.expectedLastRequiredBlockNum -= targetConfig_.maxNumberOfChunksInBatch;
    // There might be pending jobs for putBlock, we need to wait some time and then finalize them by calling
    // onTimerImp()
    this_thread::sleep_for(chrono::milliseconds(20));
    onTimerImp();
  }
  ASSERT_NO_FATAL_FAILURE(assertFetchResPagesMsgSent());
  bool doneSending = false;
  while (!doneSending) mockedSrc_->replyResPagesMsg(doneSending);
  // now validate completion
  ASSERT_TRUE(replica_.onTransferringCompleteCalled_);
  ASSERT_EQ(BCStateTran::FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

// Validate a recurring source selection, during ongoing state transfer;
TEST_F(BcStTest, validatePeriodicSourceReplacement) {
  targetConfig_.sourceReplicaReplacementTimeoutMs = 1000;
  initialize();
  ASSERT_NO_FATAL_FAILURE(startStateTransfer());
  mockedSrc_->replyAskForCheckpointSummariesMsg();
  uint32_t batch_count{0};
  while (true) {
    // once the source is selected, adding sleep for more than source replacement time duration
    if (batch_count < 2) {
      this_thread::sleep_for(milliseconds(targetConfig_.sourceReplicaReplacementTimeoutMs));
    }
    ASSERT_NO_FATAL_FAILURE(
        assertFetchBlocksMsgSent(test_state_.expectedFirstRequiredBlockNum, test_state_.expectedLastRequiredBlockNum));
    mockedSrc_->replyFetchBlocksMsg();
    if (test_state_.expectedLastRequiredBlockNum <= targetConfig_.maxNumberOfChunksInBatch) break;
    test_state_.expectedLastRequiredBlockNum -= targetConfig_.maxNumberOfChunksInBatch;
    // There might be pending jobs for putBlock, we need to wait some time and then finalize them by calling
    this_thread::sleep_for(chrono::milliseconds(20));
    onTimerImp();
    batch_count++;
  }
  const auto& sources_ = getSourceSelector().getActualSources();
  ASSERT_EQ(sources_.size(), 3);
  validateSourceSelectorMetricCounters({{"total_replacements_", 3},
                                        {"replacement_due_to_periodic_change_", 2},
                                        {"replacement_due_to_retransmission_timeout_", 0},
                                        {"replacement_due_to_bad_data_", 0}});

  ASSERT_NO_FATAL_FAILURE(assertFetchResPagesMsgSent());
  bool doneSending = false;
  while (!doneSending) mockedSrc_->replyResPagesMsg(doneSending);
  ASSERT_TRUE(replica_.onTransferringCompleteCalled_);
  ASSERT_EQ(BCStateTran::FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

// TBD BC-14432
TEST_F(BcStTest, sendPrePrepareMsgsDuringStateTransfer) {
  initialize();
  std::once_flag once_flag;
  ASSERT_NO_FATAL_FAILURE(startStateTransfer());
  mockedSrc_->replyAskForCheckpointSummariesMsg();
  auto ss = getSourceSelector();
  while (true) {
    ASSERT_NO_FATAL_FAILURE(
        assertFetchBlocksMsgSent(test_state_.expectedFirstRequiredBlockNum, test_state_.expectedLastRequiredBlockNum));
    mockedSrc_->replyFetchBlocksMsg();
    if (test_state_.expectedLastRequiredBlockNum <= targetConfig_.maxNumberOfChunksInBatch) break;
    test_state_.expectedLastRequiredBlockNum -= targetConfig_.maxNumberOfChunksInBatch;
    this_thread::sleep_for(chrono::milliseconds(20));
    onTimerImp();

    std::call_once(once_flag, [&] {
      // Generate prePrepare messages to trigger source seletor to change the source to avoid primary.
      auto msg = msg_generator_.generatePrePrepareMsg(ss.currentReplica());
      for (uint16_t i = 1; i <= targetConfig_.minPrePrepareMsgsForPrimaryAwarness; i++) {
        auto cmsg = make_shared<ConsensusMsg>(msg->type(), msg->senderId());
        stateTransfer_->peekConsensusMessage(cmsg);
      }
    });
  }
  ASSERT_NO_FATAL_FAILURE(assertFetchResPagesMsgSent());
  bool doneSending = false;
  const auto& sources = getSourceSelector().getActualSources();
  ASSERT_EQ(sources.size(), 2);
  validateSourceSelectorMetricCounters({{"total_replacements_", 2},
                                        {"replacement_due_to_source_same_as_primary_", 1},
                                        {"replacement_due_to_periodic_change_", 0},
                                        {"replacement_due_to_retransmission_timeout_", 0},
                                        {"replacement_due_to_bad_data_", 0}});

  while (!doneSending) mockedSrc_->replyResPagesMsg(doneSending);
  // validate completion
  ASSERT_TRUE(replica_.onTransferringCompleteCalled_);
  ASSERT_EQ(BCStateTran::FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

TEST_F(BcStTest, preprepareFromMultipleSourcesDuringStateTransfer) {
  initialize();
  std::once_flag once_flag;
  ASSERT_NO_FATAL_FAILURE(startStateTransfer());
  mockedSrc_->replyAskForCheckpointSummariesMsg();
  auto ss = getSourceSelector();
  while (true) {
    ASSERT_NO_FATAL_FAILURE(
        assertFetchBlocksMsgSent(test_state_.expectedFirstRequiredBlockNum, test_state_.expectedLastRequiredBlockNum));
    mockedSrc_->replyFetchBlocksMsg();
    if (test_state_.expectedLastRequiredBlockNum <= targetConfig_.maxNumberOfChunksInBatch) break;
    test_state_.expectedLastRequiredBlockNum -= targetConfig_.maxNumberOfChunksInBatch;
    this_thread::sleep_for(chrono::milliseconds(20));
    onTimerImp();

    std::call_once(once_flag, [&] {
      // Generate enough prePrepare messages but from more than one source so that source does not get changed
      auto msg = msg_generator_.generatePrePrepareMsg(ss.currentReplica());
      for (uint16_t i = 1; i <= targetConfig_.minPrePrepareMsgsForPrimaryAwarness - 1; i++) {
        auto cmsg = make_shared<ConsensusMsg>(msg->type(), msg->senderId());
        stateTransfer_->peekConsensusMessage(cmsg);
      }
      auto cmsg = make_shared<ConsensusMsg>(msg->type(), (msg->senderId() + 1) % targetConfig_.numReplicas);
      stateTransfer_->peekConsensusMessage(cmsg);
    });
  }
  ASSERT_NO_FATAL_FAILURE(assertFetchResPagesMsgSent());
  bool doneSending = false;
  const auto& sources = getSourceSelector().getActualSources();
  ASSERT_EQ(sources.size(), 1);
  validateSourceSelectorMetricCounters({{"total_replacements_", 1},
                                        {"replacement_due_to_periodic_change_", 0},
                                        {"replacement_due_to_retransmission_timeout_", 0},
                                        {"replacement_due_to_bad_data_", 0}});

  while (!doneSending) mockedSrc_->replyResPagesMsg(doneSending);
  // validate completion
  ASSERT_TRUE(replica_.onTransferringCompleteCalled_);
  ASSERT_EQ(BCStateTran::FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

}  // namespace bftEngine::bcst::impl

int main(int argc, char** argv) {
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_death_test_style =
      "threadsafe";  // mitigate the risks of testing in a possibly multithreaded environment
  return RUN_ALL_TESTS();
}
