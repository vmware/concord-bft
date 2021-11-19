// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <chrono>
#include <thread>
#include <set>
#include <string>
#include <vector>
#include <random>
#include <climits>
#include <optional>

#include "gtest/gtest.h"
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

// Create a test config with small blocks and chunks for testing
Config TestConfig() {
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
      300,                // refreshTimerMs
      2500,               // checkpointSummariesRetransmissionTimeoutMs
      60000,              // maxAcceptableMsgDelayMs
      0,                  // sourceReplicaReplacementTimeoutMs
      2000,               // fetchRetransmissionTimeoutMs
      2,                  // maxFetchRetransmissions
      5,                  // metricsDumpIntervalSec
      false,              // runInSeparateThread
      true,               // enableReservedPages
      true                // enableSourceBlocksPreFetch
  };
}

// Test fixture for blockchain state transfer tests
class BcStTest : public ::testing::Test {
 protected:
  class MockedSources;
  /////////////////////////////////////////////////////////
  //                Constants
  /////////////////////////////////////////////////////////
 protected:
  static constexpr char BCST_DB[] = "./bcst_db";
  static constexpr char MOCKED_BCST_DB[] = "./mocked_bcst_db";
  static constexpr uint64_t maxNumOfRequiredStoredCheckpoints = 3;
  static constexpr uint32_t numberOfRequiredReservedPages = 100;
  static constexpr uint32_t checkpointWindowSize = 150;
  static constexpr uint32_t defaultMinBlockDataSize = 300;
  static constexpr uint32_t defaultLastReachedcheckpointNum = 10;
  static constexpr uint32_t minNumberOfUpdatedReservedPages = 3;
  static constexpr uint32_t maxNumberOfUpdatedReservedPages = numberOfRequiredReservedPages;

 protected:
  /////////////////////////////////////////////////////////
  //        Members
  /////////////////////////////////////////////////////////
  // test subject - all the below are used to create a tested replica
  Config config_;
  TestAppState app_state_;
  TestReplica replica_;
  std::shared_ptr<BCStateTran> stateTransfer_;
  DataStore* datastore_ = nullptr;

  std::unique_ptr<MockedSources> mockedSrc_;

  // Test body related members
  struct CommonTestParams {
    uint64_t lastReachedcheckpointNum;
    uint64_t expectedFirstRequiredBlockNum;
    uint64_t expectedLastRequiredBlockNum;
  };
  CommonTestParams testParams_;

  /////////////////////////////////////////////////////////
  //        Ctor / Dtor / SetUp / TearDown
  /////////////////////////////////////////////////////////
  BcStTest() {
    testParams_.lastReachedcheckpointNum = defaultLastReachedcheckpointNum;
    testParams_.expectedFirstRequiredBlockNum = app_state_.getGenesisBlockNum() + 1;
    testParams_.expectedLastRequiredBlockNum = (testParams_.lastReachedcheckpointNum + 1) * checkpointWindowSize;
    DeleteBcStateTransferDbFolder(BCST_DB);
  }
  ~BcStTest(){};

  void SetUp() override {
    // uncomment if needed after setting the required log level
#ifdef USE_LOG4CPP
    log4cplus::LogLevel logLevel = log4cplus::INFO_LOG_LEVEL;
    // logging::Logger::getInstance("serializable").setLogLevel(logLevel);
    // logging::Logger::getInstance("concord.bft.st.dbdatastore").setLogLevel(logLevel);
    logging::Logger::getInstance("concord.bft.st.dst").setLogLevel(logLevel);
    logging::Logger::getInstance("concord.bft.st.src").setLogLevel(logLevel);
    logging::Logger::getInstance("concord.util.handoff").setLogLevel(logLevel);
    // logging::Logger::getInstance("rocksdb").setLogLevel(logLevel);
#else
    logging::LogLevel logLevel = logging::LogLevel::info;
    // logging::Logger::getInstance("serializable").setLogLevel(logLevel);
    // logging::Logger::getInstance("concord.bft.st.dbdatastore").setLogLevel(logLevel);
    logging::getLogger("concord.bft.st.dst").setLogLevel(logLevel);
    logging::getLogger("concord.bft.st.src").setLogLevel(logLevel);
    logging::getLogger("concord.util.handoff").setLogLevel(logLevel);
    // logging::Logger::getInstance("rocksdb").setLogLevel(logLevel);
#endif
    config_ = TestConfig();
    // For now we assume no chunking is supported
    ConcordAssertEQ(config_.maxChunkSize, config_.maxBlockSize);
#ifdef USE_ROCKSDB
    auto* db_key_comparator = new concord::kvbc::v1DirectKeyValue::DBKeyComparator();
    concord::storage::IDBClient::ptr dbc(
        new concord::storage::rocksdb::Client(string(BCST_DB), make_unique<KeyComparator>(db_key_comparator)));
    dbc->init();
    datastore_ = new DBDataStore(dbc,
                                 config_.sizeOfReservedPage,
                                 make_shared<concord::storage::v1DirectKeyValue::STKeyManipulator>(),
                                 config_.enableReservedPages);
#else
    auto comparator = concord::storage::memorydb::KeyComparator(db_key_comparator);
    concord::storage::IDBClient::ptr dbc1(new concord::storage::memorydb::Client(comparator));
    datastore_ = new InMemoryDataStore(config_.sizeOfReservedPage);
#endif

    stateTransfer_.reset(new BCStateTran(config_, &app_state_, datastore_));
    stateTransfer_->init(maxNumOfRequiredStoredCheckpoints, numberOfRequiredReservedPages, config_.sizeOfReservedPage);
    for (uint32_t i{0}; i < numberOfRequiredReservedPages; ++i) {
      stateTransfer_->zeroReservedPage(i);
    }
    mockedSrc_.reset(new MockedSources(config_, testParams_, replica_, stateTransfer_));
    ASSERT_FALSE(stateTransfer_->isRunning());
    stateTransfer_->startRunning(&replica_);
    ASSERT_TRUE(stateTransfer_->isRunning());
    ASSERT_EQ(BCStateTran::FetchingState::NotFetching, stateTransfer_->getFetchingState());
    ASSERT_LE(minNumberOfUpdatedReservedPages, maxNumberOfUpdatedReservedPages);
    ASSERT_LE(testParams_.expectedFirstRequiredBlockNum, testParams_.expectedLastRequiredBlockNum);
  }

  void TearDown() override {
    if (stateTransfer_) {
      // Must stop running before destruction
      if (stateTransfer_->isRunning()) {
        stateTransfer_->stopRunning();
      }
    }
    DeleteBcStateTransferDbFolder(BCST_DB);
  }

  static void DeleteBcStateTransferDbFolder(string&& path) {
    string cmd = string("rm -rf ") + string(path);
    if (system(cmd.c_str())) {
      ASSERT_TRUE(false);
    }
  }

  /////////////////////////////////////////////////////////
  //   State Transfer proxies (BcStTest is a friend class)
  /////////////////////////////////////////////////////////
  void onTimerImp() { stateTransfer_->onTimerImp(); }
  SourceSelector& GetSourceSelector() { return stateTransfer_->sourceSelector_; }

  /////////////////////////////////////////////////////////
  //      Tests common code - send messages
  /////////////////////////////////////////////////////////
  void SendCheckpointSummaries() {
    stateTransfer_->startCollectingState();
    ASSERT_EQ(BCStateTran::FetchingState::GettingCheckpointSummaries, stateTransfer_->getFetchingState());
    auto min_relevant_checkpoint = 1;
    AssertCheckpointSummariesSent(min_relevant_checkpoint);
  }

  /////////////////////////////////////////////////////////
  //      Tests common code - state assertions
  /////////////////////////////////////////////////////////
  static void AssertMsgType(const Msg& msg, uint16_t type) {
    auto header = reinterpret_cast<BCStateTranBaseMsg*>(msg.data_.get());
    ASSERT_EQ(type, header->type);
  }

  void AssertCheckpointSummariesSent(uint64_t checkpoint_num) {
    ASSERT_EQ(replica_.sent_messages_.size(), config_.numReplicas - 1);

    set<uint16_t> dests;
    for (auto& msg : replica_.sent_messages_) {
      auto p = dests.insert(msg.to_);
      ASSERT_TRUE(p.second);  // destinations must be unique
      AssertMsgType(msg, MsgType::AskForCheckpointSummaries);
      auto askMsg = reinterpret_cast<AskForCheckpointSummariesMsg*>(msg.data_.get());
      ASSERT_TRUE(askMsg->msgSeqNum > 0);
      ASSERT_EQ(checkpoint_num, askMsg->minRelevantCheckpointNum);
    }
  }

  void AssertFetchBlocksMsgSent(uint64_t firstRequiredBlock, uint64_t lastRequiredBlock) {
    ASSERT_EQ(BCStateTran::FetchingState::GettingMissingBlocks, stateTransfer_->getFetchingState());
    auto currentSourceId = GetSourceSelector().currentReplica();
    ASSERT_NE(currentSourceId, NO_REPLICA);
    ASSERT_EQ(datastore_->getFirstRequiredBlock(), firstRequiredBlock);
    ASSERT_EQ(datastore_->getLastRequiredBlock(), lastRequiredBlock);
    ASSERT_EQ(replica_.sent_messages_.size(), 1);
    AssertMsgType(replica_.sent_messages_.front(), MsgType::FetchBlocks);
    ASSERT_EQ(replica_.sent_messages_.front().to_, currentSourceId);
  }

  void AssertFetchResPagesMsgSent() {
    ASSERT_EQ(BCStateTran::FetchingState::GettingMissingResPages, stateTransfer_->getFetchingState());
    auto currentSourceId = GetSourceSelector().currentReplica();
    ASSERT_NE(currentSourceId, NO_REPLICA);
    ASSERT_EQ(datastore_->getFirstRequiredBlock(), datastore_->getLastRequiredBlock());
    ASSERT_EQ(replica_.sent_messages_.size(), 1);
    AssertMsgType(replica_.sent_messages_.front(), MsgType::FetchResPages);
    ASSERT_EQ(replica_.sent_messages_.front().to_, currentSourceId);
  }

  // TODO(GL) - consider transforming to gMock? + the mocked interfaces (replica, app)
  class MockedSources {
   protected:
    DataStore* mockedDatastore_ = nullptr;
    std::unique_ptr<char[]> rawVBlock_;
    std::map<uint64_t, std::shared_ptr<Block>> generatedBlocks_;  // map: blockId -> Block
    std::optional<FetchResPagesMsg> lastReceivedFetchResPagesMsg_;
    Config& config_;
    CommonTestParams testParams_;
    TestAppState app_state_;
    TestReplica& replica_;
    const std::shared_ptr<BCStateTran>& stateTransfer_;

    /////////////////////////////////////////////////////////
    //        Ctor / Dtor
    /////////////////////////////////////////////////////////
   public:
    MockedSources(Config& config,
                  CommonTestParams& testParams_,
                  TestReplica& replica,
                  const std::shared_ptr<BCStateTran>& stateTransfer)
        : config_(config), testParams_(testParams_), replica_(replica), stateTransfer_(stateTransfer) {
      DeleteBcStateTransferDbFolder(MOCKED_BCST_DB);
#ifdef USE_ROCKSDB
      // create a mocked data store
      auto* mocked_db_key_comparator = new concord::kvbc::v1DirectKeyValue::DBKeyComparator();
      concord::storage::IDBClient::ptr mockedDbc(new concord::storage::rocksdb::Client(
          string(MOCKED_BCST_DB), make_unique<KeyComparator>(mocked_db_key_comparator)));
      mockedDbc->init();
      mockedDatastore_ = new DBDataStore(mockedDbc,
                                         config_.sizeOfReservedPage,
                                         make_shared<concord::storage::v1DirectKeyValue::STKeyManipulator>(),
                                         config_.enableReservedPages);
      mockedDatastore_->setNumberOfReservedPages(numberOfRequiredReservedPages);
#else
      concord::storage::IDBClient::ptr dbc2(new concord::storage::memorydb::Client(comparator));
      mockedDatastore_ = new InMemoryDataStore(config_.sizeOfReservedPage);
#endif
    }
    ~MockedSources() {
      delete mockedDatastore_;
      DeleteBcStateTransferDbFolder(MOCKED_BCST_DB);
    }
    /////////////////////////////////////////////////////////
    //      Source (Mock) Replies
    /////////////////////////////////////////////////////////
    void ReplyAskForCheckpointSummariesMsg() {
      const uint64_t toCheckpoint = testParams_.lastReachedcheckpointNum;
      const uint64_t fromCheckpoint = testParams_.lastReachedcheckpointNum - maxNumOfRequiredStoredCheckpoints + 1;
      vector<unique_ptr<CheckpointSummaryMsg>> checkpointSummaryReplies;

      // Generate all the blocks until lastBlock of the last checkpoint
      auto lastBlockId = (toCheckpoint + 1) * checkpointWindowSize;
      GenerateBlocks(app_state_.getGenesisBlockNum() + 1, lastBlockId);

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
        reply->lastBlock = (i + 1) * checkpointWindowSize;
        auto digestBytes = reply->digestOfLastBlock.getForUpdate();
        if (i == toCheckpoint)
          memcpy(digestBytes, &lastBlockDigest, sizeof(lastBlockDigest));
        else {
          auto blk = generatedBlocks_[reply->lastBlock + 1];
          memcpy(digestBytes, &blk->digestPrev, sizeof(blk->digestPrev));
        }
        GenerateReservedPages(i);
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
      ASSERT_EQ(ClearSentMessagesByMessageType(MsgType::AskForCheckpointSummaries), config_.numReplicas - 1);
    }

    void ReplyFetchBlocksMsg() {
      ASSERT_EQ(replica_.sent_messages_.size(), 1);
      const auto& msg = replica_.sent_messages_.front();
      AssertMsgType(msg, MsgType::FetchBlocks);
      auto fetchBlocksMsg = reinterpret_cast<FetchBlocksMsg*>(msg.data_.get());
      uint64_t nextBlockId = fetchBlocksMsg->lastRequiredBlock;
      size_t numOfSentChunks = 0;

      // For now we assume no chunking is supported
      ConcordAssertEQ(fetchBlocksMsg->lastKnownChunkInLastRequiredBlock, 0);
      ConcordAssertLE(fetchBlocksMsg->firstRequiredBlock, fetchBlocksMsg->lastRequiredBlock);

      while (true) {
        const auto& blk = generatedBlocks_[nextBlockId];
        ItemDataMsg* itemDataMsg = ItemDataMsg::alloc(blk->totalBlockSize);
        bool lastInBatch = ((numOfSentChunks + 1) >= config_.maxNumberOfChunksInBatch) ||
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
    void ReplyResPagesMsg(bool& outDoneSending) {
      ASSERT_EQ(replica_.sent_messages_.size(), 1);
      const auto& msg = replica_.sent_messages_.front();
      AssertMsgType(msg, MsgType::FetchResPages);
      auto fetchResPagesMsg = reinterpret_cast<FetchResPagesMsg*>(msg.data_.get());

      // check if need to create a vBlock
      if (!lastReceivedFetchResPagesMsg_ ||
          lastReceivedFetchResPagesMsg_.value().lastCheckpointKnownToRequester !=
              fetchResPagesMsg->lastCheckpointKnownToRequester ||
          lastReceivedFetchResPagesMsg_.value().requiredCheckpointNum != fetchResPagesMsg->requiredCheckpointNum) {
        // need to generate pages - for now, lets assume all pages need to be updated
        uint32_t numberOfUpdatedPages = maxNumberOfUpdatedReservedPages;
        const uint32_t elementSize = sizeof(BCStateTran::ElementOfVirtualBlock) + config_.sizeOfReservedPage - 1;
        const uint32_t size = sizeof(BCStateTran::HeaderOfVirtualBlock) + numberOfUpdatedPages * elementSize;
        lastReceivedFetchResPagesMsg_ = *fetchResPagesMsg;

        // allocate and fill vBlock
        rawVBlock_.reset(new char[size]);
        std::fill(rawVBlock_.get(), rawVBlock_.get() + size, 0);
        BCStateTran::HeaderOfVirtualBlock* header =
            reinterpret_cast<BCStateTran::HeaderOfVirtualBlock*>(rawVBlock_.get());
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
              pageId, currElement->checkpointNumber, nullptr, currElement->page, config_.sizeOfReservedPage);
          ASSERT_TRUE(!currElement->pageDigest.isZero());
          idx++;
        }
      }
      ASSERT_TRUE(rawVBlock_.get());
      uint32_t vblockSize = BCStateTran::getSizeOfVirtualBlock(rawVBlock_.get(), config_.sizeOfReservedPage);
      ASSERT_GE(vblockSize, sizeof(BCStateTran::HeaderOfVirtualBlock));
      ASSERT_TRUE(BCStateTran::checkStructureOfVirtualBlock(
          rawVBlock_.get(), vblockSize, config_.sizeOfReservedPage, stateTransfer_->logger_));

      // compute information about next chunk
      uint32_t sizeOfLastChunk = config_.maxChunkSize;
      uint32_t numOfChunksInVBlock = vblockSize / config_.maxChunkSize;
      uint16_t nextChunk = fetchResPagesMsg->lastKnownChunk + 1;
      if (vblockSize % config_.maxChunkSize != 0) {
        sizeOfLastChunk = vblockSize % config_.maxChunkSize;
        numOfChunksInVBlock++;
      }
      // if msg is invalid (because lastKnownChunk+1 does not exist)
      ASSERT_LE(nextChunk, numOfChunksInVBlock);

      // send chunks
      uint16_t numOfSentChunks = 0;
      while (true) {
        uint32_t chunkSize = (nextChunk < numOfChunksInVBlock) ? config_.maxChunkSize : sizeOfLastChunk;
        ASSERT_GT(chunkSize, 0);

        char* pRawChunk = rawVBlock_.get() + (nextChunk - 1) * config_.maxChunkSize;
        ItemDataMsg* outMsg = ItemDataMsg::alloc(chunkSize);

        outMsg->requestMsgSeqNum = fetchResPagesMsg->msgSeqNum;
        outMsg->blockNumber = BCStateTran::ID_OF_VBLOCK_RES_PAGES;
        outMsg->totalNumberOfChunksInBlock = numOfChunksInVBlock;
        outMsg->chunkNumber = nextChunk;
        outMsg->dataSize = chunkSize;
        outMsg->lastInBatch =
            ((numOfSentChunks + 1) >= config_.maxNumberOfChunksInBatch || (nextChunk == numOfChunksInVBlock));
        memcpy(outMsg->data, pRawChunk, chunkSize);

        stateTransfer_->onMessage(outMsg, outMsg->size(), msg.to_, std::chrono::steady_clock::now());
        numOfSentChunks++;

        // if we've already sent enough chunks
        if (numOfSentChunks >= config_.maxNumberOfChunksInBatch) {
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

   protected:
    void GenerateReservedPages(uint64_t checkpointNumber) {
      uint32_t idx = 0;
      std::unique_ptr<char[]> buffer(new char[config_.sizeOfReservedPage]);
      for (uint32_t pageId{0}; pageId < maxNumberOfUpdatedReservedPages; ++pageId) {
        ConcordAssertLT(idx, maxNumberOfUpdatedReservedPages);
        STDigest pageDigest;
        fillRandomBytes(buffer.get(), config_.sizeOfReservedPage);
        BCStateTran::computeDigestOfPage(
            pageId, checkpointNumber, buffer.get(), config_.sizeOfReservedPage, pageDigest);
        ASSERT_TRUE(!pageDigest.isZero());
        mockedDatastore_->setResPage(pageId, checkpointNumber, pageDigest, buffer.get());
        idx++;
      }
    }

    // toBlockId is assumed to be a checkpoint block, we also assume generatedBlocks_ is empty
    void GenerateBlocks(uint64_t fromBlockId, uint64_t toBlockId) {
      char buff[kMaxBlockSize];

      ConcordAssertEQ(toBlockId % checkpointWindowSize, 0);
      ConcordAssert(generatedBlocks_.empty());
      ConcordAssertGT(fromBlockId, 1);

      auto maxBlockDataSize = Block::calcMaxDataSize();
      for (size_t i = fromBlockId; i <= toBlockId; ++i) {
        uint32_t dataSize =
            static_cast<uint32_t>(rand()) % (maxBlockDataSize - defaultMinBlockDataSize + 1) + defaultMinBlockDataSize;
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

    /////////////////////////////////////////////////////////
    //          Helper functions
    /////////////////////////////////////////////////////////
    size_t ClearSentMessagesByMessageType(uint16_t type) { return FilterSentMessagesByMessageType(type, false); }
    size_t KeepSentMessagesByMessageType(uint16_t type) { return FilterSentMessagesByMessageType(type, true); }

    // keep is true: keep only messages with msg->type == type
    // keep is false: keep all message with msg->type != type
    // return number of messages deleted
    size_t FilterSentMessagesByMessageType(uint16_t type, bool keep) {
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
  };  // class MockedSources

  static void fillRandomBytes(char* data, size_t bytesToFill) {
    using random_bytes_engine = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char>;
    random_bytes_engine rbe;
    std::generate(data, data + bytesToFill, std::ref(rbe));
  }
};  // class BcStTest

// Validates that GettingCheckpointSummaries is sent to all replicas
TEST_F(BcStTest, dstSendAskForCheckpointSummariesMsg) { ASSERT_NO_FATAL_FAILURE(SendCheckpointSummaries()); }

// Validates that after GettingCheckpointSummaries is sent to all replicas, dest retrnasmit on timeout
TEST_F(BcStTest, dstRetransmitAskForCheckpointSummariesMsg) {
  ASSERT_NO_FATAL_FAILURE(SendCheckpointSummaries());
  this_thread::sleep_for(milliseconds(config_.checkpointSummariesRetransmissionTimeoutMs + 100));
  onTimerImp();
  ASSERT_EQ(replica_.sent_messages_.size(), (config_.numReplicas - 1) * 2);
}

// Validate that when f+1 identical checkpoint summary message are received within a certain duration from f+1 sources,
// then destinations process them correctly and moves into the next stage to fetch blocks / reserved pages
// Also validate that a single FetchBlocksMsg has been sent to a specific source
TEST_F(BcStTest, dstProcessCheckpointSummariesMsgs) {
  ASSERT_NO_FATAL_FAILURE(SendCheckpointSummaries());
  // in the next call multiple CheckpointSummaries message are sent, and in addition, a single FetchBlocksMsg is sent
  mockedSrc_->ReplyAskForCheckpointSummariesMsg();
  ASSERT_NO_FATAL_FAILURE(
      AssertFetchBlocksMsgSent(testParams_.expectedFirstRequiredBlockNum, testParams_.expectedLastRequiredBlockNum));
}

// Validate correct processing of series of ItemDataMsg messages from a mocked source
TEST_F(BcStTest, dstProcessItemDataMsgs) {
  ASSERT_NO_FATAL_FAILURE(SendCheckpointSummaries());
  mockedSrc_->ReplyAskForCheckpointSummariesMsg();
  while (true) {
    ASSERT_NO_FATAL_FAILURE(
        AssertFetchBlocksMsgSent(testParams_.expectedFirstRequiredBlockNum, testParams_.expectedLastRequiredBlockNum));
    mockedSrc_->ReplyFetchBlocksMsg();
    if (testParams_.expectedLastRequiredBlockNum <= config_.maxNumberOfChunksInBatch) break;
    testParams_.expectedLastRequiredBlockNum -= config_.maxNumberOfChunksInBatch;
    // There might be pending jobs for putBlock, we need to wait some time and then finalize them by calling
    // onTimerImp()
    this_thread::sleep_for(chrono::milliseconds(20));
    onTimerImp();
  }
  ASSERT_NO_FATAL_FAILURE(AssertFetchResPagesMsgSent());
}

// Validate a full state transfer
TEST_F(BcStTest, dstFullStateTransfer) {
  ASSERT_NO_FATAL_FAILURE(SendCheckpointSummaries());
  mockedSrc_->ReplyAskForCheckpointSummariesMsg();
  while (true) {
    ASSERT_NO_FATAL_FAILURE(
        AssertFetchBlocksMsgSent(testParams_.expectedFirstRequiredBlockNum, testParams_.expectedLastRequiredBlockNum));
    mockedSrc_->ReplyFetchBlocksMsg();
    if (testParams_.expectedLastRequiredBlockNum <= config_.maxNumberOfChunksInBatch) break;
    testParams_.expectedLastRequiredBlockNum -= config_.maxNumberOfChunksInBatch;
    // There might be pending jobs for putBlock, we need to wait some time and then finalize them by calling
    // onTimerImp()
    this_thread::sleep_for(chrono::milliseconds(20));
    onTimerImp();
  }
  ASSERT_NO_FATAL_FAILURE(AssertFetchResPagesMsgSent());
  bool doneSending = false;
  while (!doneSending) mockedSrc_->ReplyResPagesMsg(doneSending);
  // now validate commpletion
  ASSERT_TRUE(replica_.onTransferringCompleteCalled_);
  ASSERT_EQ(BCStateTran::FetchingState::NotFetching, stateTransfer_->getFetchingState());
}

// Check that only actual resources are inserted into source selector actualSources_
// This is done by triggering multiple retransmissions and then  source replacments, and checking that only the sources
// which replied are in the list, and in the expected order.
// The check is done only for FetchingMissingblocks state sources.
TEST_F(BcStTest, dstValidateRealSourceListReported) {
  uint16_t currentSrc;
  // Add callback to ST to be executed when transferring is completed.
  // Here we valiate that only one actual source is in the sources list, although we had multiple
  // retransmissions and few sources were selected.
  stateTransfer_->addOnTransferringCompleteCallback([this, &currentSrc](std::uint64_t) {
    const auto& sources_ = GetSourceSelector().getActualSources();
    ASSERT_EQ(sources_.size(), 1);
    ASSERT_EQ(sources_[0], currentSrc);
  });

  ASSERT_NO_FATAL_FAILURE(SendCheckpointSummaries());
  mockedSrc_->ReplyAskForCheckpointSummariesMsg();

  // Trigger multiple retransmissions to 2 sources. none will be answered, then we expect the replica to move into the
  // 3rd source
  auto& sourceSelector = GetSourceSelector();
  set<uint16_t> sources;
  for (uint32_t i{0}; i < 2; ++i) {
    for (uint32_t j{0}; j < config_.maxFetchRetransmissions; ++j) {
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
      this_thread::sleep_for(chrono::milliseconds(config_.fetchRetransmissionTimeoutMs + 10));
      onTimerImp();
    }
  }
  ASSERT_EQ(replica_.sent_messages_.size(), 1);
  currentSrc = sourceSelector.currentReplica();
  while (true) {
    ASSERT_NO_FATAL_FAILURE(
        AssertFetchBlocksMsgSent(testParams_.expectedFirstRequiredBlockNum, testParams_.expectedLastRequiredBlockNum));
    mockedSrc_->ReplyFetchBlocksMsg();
    if (testParams_.expectedLastRequiredBlockNum <= config_.maxNumberOfChunksInBatch) break;
    testParams_.expectedLastRequiredBlockNum -= config_.maxNumberOfChunksInBatch;
    // There might be pending jobs for putBlock, we need to wait some time and then finalize them by calling
    // onTimerImp()
    this_thread::sleep_for(chrono::milliseconds(20));
    onTimerImp();
  }
  ASSERT_NO_FATAL_FAILURE(AssertFetchResPagesMsgSent());
  bool doneSending = false;
  while (!doneSending) mockedSrc_->ReplyResPagesMsg(doneSending);
  // now validate completion
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
