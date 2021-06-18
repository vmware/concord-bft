// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"

#include "Logger.hpp"
#include "block_metadata.hpp"
#include "categorization/kv_blockchain.h"
#include "db_interfaces.h"
#include "replica_state_sync_imp.hpp"
#include "rocksdb/native_client.h"
#include "storage/test/storage_test_common.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace {

using concord::kvbc::BlockId;
using concord::kvbc::BlockMetadata;
using concord::kvbc::IReader;
using concord::kvbc::kConcordInternalCategoryId;
using concord::kvbc::ReplicaStateSyncImp;
using concord::kvbc::categorization::CATEGORY_TYPE;
using concord::kvbc::categorization::KeyValueBlockchain;
using concord::kvbc::categorization::TaggedVersion;
using concord::kvbc::categorization::Updates;
using concord::kvbc::categorization::Value;
using concord::kvbc::categorization::VersionedUpdates;
using concord::storage::rocksdb::NativeClient;

using namespace ::testing;
using namespace std::literals;

class replica_state_sync_test : public Test, public IReader {
  void SetUp() override {
    cleanup();
    db = TestRocksDb::createNative();
    const auto link_st_chain = true;
    blockchain.emplace(db,
                       link_st_chain,
                       std::map<std::string, CATEGORY_TYPE>{{kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}});
  }

  void TearDown() override { cleanup(); }

 public:
  std::optional<Value> get(const std::string &category_id, const std::string &key, BlockId block_id) const override {
    throw std::logic_error{"IReader::get() should not be called"};
  }

  std::optional<Value> getLatest(const std::string &category_id, const std::string &key) const override {
    return blockchain->getLatest(kConcordInternalCategoryId, key);
  }

  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<Value>> &values) const override {
    throw std::logic_error{"IReader::multiGet() should not be called"};
  }

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<Value>> &values) const override {
    throw std::logic_error{"IReader::multiGetLatest() should not be called"};
  }

  std::optional<TaggedVersion> getLatestVersion(const std::string &category_id, const std::string &key) const override {
    throw std::logic_error{"IReader::getLatestVersion() should not be called"};
  }

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<TaggedVersion>> &versions) const override {
    throw std::logic_error{"IReader::multiGetLatestVersion() should not be called"};
  }

  std::optional<Updates> getBlockUpdates(BlockId block_id) const override {
    throw std::logic_error{"IReader::getBlockUpdates() should not be called"};
  }

  BlockId getGenesisBlockId() const override {
    throw std::logic_error{"IReader::getGenesisBlockId() should not be called"};
  }

  BlockId getLastBlockId() const override { throw std::logic_error{"IReader::getLastBlockId() should not be called"}; }

 protected:
  void addBlockWithSeqNum(std::uint64_t seq_number) {
    auto updates = Updates{};
    auto ver_updates = VersionedUpdates{};
    ver_updates.addUpdate(std::string{BlockMetadata::kBlockMetadataKeyStr}, metadata.serialize(seq_number));
    updates.add(kConcordInternalCategoryId, std::move(ver_updates));
    blockchain->addBlock(std::move(updates));
  }

 protected:
  BlockMetadata metadata{*this};
  ReplicaStateSyncImp replica_state_sync{new BlockMetadata{*this}};
  logging::Logger logger{logging::Logger::getInstance("com.vmware.replica_state_sync_test")};

  // Test case specific members that are reset on SetUp() and TearDown().
  std::shared_ptr<NativeClient> db;
  std::optional<KeyValueBlockchain> blockchain;
  const std::uint32_t max_num_of_blocks_to_delete = 10;
};

TEST_F(replica_state_sync_test, empty_blockchain_and_0_bft_seq_num) {
  const auto last_executed_bft_seq_num = 0;
  ASSERT_EQ(0, replica_state_sync.execute(logger, *blockchain, last_executed_bft_seq_num, max_num_of_blocks_to_delete));
}

TEST_F(replica_state_sync_test, non_empty_blockchain_and_0_bft_seq_num) {
  addBlockWithSeqNum(1);
  addBlockWithSeqNum(2);
  const auto last_executed_bft_seq_num = 0;
  ASSERT_EQ(0, replica_state_sync.execute(logger, *blockchain, last_executed_bft_seq_num, max_num_of_blocks_to_delete));
  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(2, blockchain->getLastReachableBlockId());
}

TEST_F(replica_state_sync_test, empty_blockchain_and_non_0_bft_seq_num) {
  const auto last_executed_bft_seq_num = 42;
  ASSERT_EQ(0, replica_state_sync.execute(logger, *blockchain, last_executed_bft_seq_num, max_num_of_blocks_to_delete));
}

TEST_F(replica_state_sync_test, bft_seq_num_equal_to_block_seq_num) {
  addBlockWithSeqNum(1);
  addBlockWithSeqNum(2);
  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(2, blockchain->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 2;
  ASSERT_EQ(0, replica_state_sync.execute(logger, *blockchain, last_executed_bft_seq_num, max_num_of_blocks_to_delete));

  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(2, blockchain->getLastReachableBlockId());
}

TEST_F(replica_state_sync_test, bft_seq_num_bigger_than_block_seq_num) {
  addBlockWithSeqNum(1);
  addBlockWithSeqNum(2);
  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(2, blockchain->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 42;
  ASSERT_EQ(0, replica_state_sync.execute(logger, *blockchain, last_executed_bft_seq_num, max_num_of_blocks_to_delete));
  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(2, blockchain->getLastReachableBlockId());
}

TEST_F(replica_state_sync_test, bft_seq_num_less_than_block_seq_num) {
  addBlockWithSeqNum(1);
  addBlockWithSeqNum(2);
  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(2, blockchain->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 1;
  ASSERT_EQ(1, replica_state_sync.execute(logger, *blockchain, last_executed_bft_seq_num, max_num_of_blocks_to_delete));

  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(1, blockchain->getLastReachableBlockId());
}

TEST_F(replica_state_sync_test, cannot_delete_only_block_left) {
  addBlockWithSeqNum(2);  // block 1
  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(1, blockchain->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 1;
  ASSERT_THROW(replica_state_sync.execute(logger, *blockchain, last_executed_bft_seq_num, max_num_of_blocks_to_delete),
               std::exception);
  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(1, blockchain->getLastReachableBlockId());
}

TEST_F(replica_state_sync_test, cannot_delete_only_block_left_with_pruned_block) {
  addBlockWithSeqNum(2);  // block 1
  addBlockWithSeqNum(3);  // block 2
  // Prune block 1.
  blockchain->deleteBlock(1);
  ASSERT_EQ(2, blockchain->getGenesisBlockId());
  ASSERT_EQ(2, blockchain->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 2;
  ASSERT_THROW(replica_state_sync.execute(logger, *blockchain, last_executed_bft_seq_num, max_num_of_blocks_to_delete),
               std::exception);
  ASSERT_EQ(2, blockchain->getGenesisBlockId());
  ASSERT_EQ(2, blockchain->getLastReachableBlockId());
}

TEST_F(replica_state_sync_test, bft_too_many_inconsistent_blocks_detected) {
  addBlockWithSeqNum(1);
  addBlockWithSeqNum(2);
  addBlockWithSeqNum(3);
  addBlockWithSeqNum(4);
  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(4, blockchain->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 2;
  ASSERT_THROW(replica_state_sync.execute(logger, *blockchain, last_executed_bft_seq_num, 1), std::exception);
  // Only one block is expected to be deleted
  ASSERT_EQ(1, blockchain->getGenesisBlockId());
  ASSERT_EQ(3, blockchain->getLastReachableBlockId());
}

}  // namespace
