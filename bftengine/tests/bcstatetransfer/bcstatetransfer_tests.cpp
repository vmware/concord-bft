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

#include "gtest/gtest.h"
#include "SimpleBCStateTransfer.hpp"
#include "BCStateTran.hpp"
#include "test_app_state.hpp"
#include "test_replica.hpp"
#include "Logger.hpp"
#include "DBDataStore.hpp"
#include "blockchain/db_adapter.h"
#include "memorydb/client.h"

using concord::storage::ITransaction;
using concord::storage::blockchain::KeyManipulator;

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
using concord::storage::rocksdb::Client;
using concord::storage::rocksdb::KeyComparator;
#endif
namespace bftEngine {
namespace SimpleBlockchainStateTransfer {

using namespace impl;

// Create a test config with small blocks and chunks for testing
Config TestConfig() {
    Config config;
    config.myReplicaId = 1;
    config.fVal = 1;
    config.maxBlockSize = kMaxBlockSize;
    config.maxChunkSize = 128;
    config.maxNumberOfChunksInBatch = 128;
    return config;
}

// Test fixture for blockchain state transfer tests
class BcStTest : public ::testing::Test {
  protected:
    void SetUp() override {
      //uncomment if needed
//      log4cplus::Logger::getInstance( LOG4CPLUS_TEXT("serializable")).setLogLevel(log4cplus::TRACE_LOG_LEVEL);
//      log4cplus::Logger::getInstance( LOG4CPLUS_TEXT("DBDataStore")).setLogLevel(log4cplus::TRACE_LOG_LEVEL);
//      log4cplus::Logger::getInstance( LOG4CPLUS_TEXT("rocksdb")).setLogLevel(log4cplus::TRACE_LOG_LEVEL);

      config_ = TestConfig();
      auto* key_manipulator = new concord::storage::blockchain::KeyManipulator();
#ifdef USE_ROCKSDB
      concord::storage::IDBClient::ptr dbc(new concord::storage::rocksdb::Client("./bcst_db", new KeyComparator(key_manipulator)));
#else
      auto comparator = concord::storage::memorydb::KeyComparator(key_manipulator);
      concord::storage::IDBClient::ptr dbc(new concord::storage::memorydb::Client(comparator));
#endif
      dbc->init();
      DBDataStore* dbds = new DBDataStore(dbc, config_.sizeOfReservedPage);
      st_ = new BCStateTran(config_, &app_state_, dbds);
      ASSERT_FALSE(st_->isRunning());
      st_->startRunning(&replica_);
      ASSERT_TRUE(st_->isRunning());
      ASSERT_EQ(BCStateTran::FetchingState::NotFetching, st_->getFetchingState());


      bftEngine::SimpleBlockchainStateTransfer::impl::DataStoreTransaction::ptr txn(dbds->beginTransaction());
      txn->setFVal(34);
      txn->setMyReplicaId(34);

    }

    void TearDown() override {
      // Must stop running before destruction
      st_->stopRunning();
      delete st_;
    }

    Config config_;
    TestAppState app_state_;
    TestReplica replica_;
    BCStateTran* st_;

};

// Verify that AskForCheckpointSummariesMsg is sent to all other replicas
void assert_checkpoint_summary_requests_sent(
    const TestReplica& replica, uint64_t checkpoint_num) {

  ASSERT_EQ(replica.sent_messages_.size(), 3);
  for (auto& msg: replica.sent_messages_) {
    auto header = reinterpret_cast<BCStateTranBaseMsg*>(msg.msg_.get());
    ASSERT_EQ(MsgType::AskForCheckpointSummaries, header->type);
    auto ask_msg = reinterpret_cast<AskForCheckpointSummariesMsg*>(msg.msg_.get());
    ASSERT_TRUE(ask_msg->msgSeqNum > 0);
    // TODO(AJS): Should this assert work?
    // ASSERT_EQ(checkpoint_num, ask_msg->minRelevantCheckpointNum);
  }

}

// The state transfer module under test here is fetching data that is missing from
// another replica.
TEST_F(BcStTest, FetchMissingData) {
  st_->startCollectingState();

  ASSERT_EQ(BCStateTran::FetchingState::GettingCheckpointSummaries,
            st_->getFetchingState());

  auto min_relevant_checkpoint = 1;
  assert_checkpoint_summary_requests_sent(replica_, min_relevant_checkpoint);

  // TODO: Mock out some checkpoint data and return it from multiple replicas.
  // Make sure that it syncs correctly.

}

} // namespace SimpleBlockchainStateTransfer
} // namespace bftEngine
