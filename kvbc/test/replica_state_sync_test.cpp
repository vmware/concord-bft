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

#include "DbMetadataStorage.hpp"
#include "Logger.hpp"
#include "block_metadata.hpp"
#include "categorization/kv_blockchain.h"
#include "db_interfaces.h"
#include "metadata_block_id.h"
#include "PersistentStorageImp.hpp"
#include "replica_state_sync_imp.hpp"
#include "rocksdb/native_client.h"
#include "storage/merkle_tree_key_manipulator.h"
#include "storage/test/storage_test_common.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace {

using bftEngine::impl::PersistentStorageImp;
using concord::kvbc::BlockId;
using concord::kvbc::BlockMetadata;
using concord::kvbc::IReader;
using concord::kvbc::categorization::kConcordInternalCategoryId;
using concord::kvbc::ReplicaStateSyncImp;
using concord::kvbc::categorization::CATEGORY_TYPE;
using concord::kvbc::categorization::KeyValueBlockchain;
using concord::kvbc::categorization::TaggedVersion;
using concord::kvbc::categorization::Updates;
using concord::kvbc::categorization::Value;
using concord::kvbc::categorization::VersionedUpdates;
using concord::kvbc::getLastBlockIdFromMetadata;
using concord::storage::DBMetadataStorage;
using concord::storage::rocksdb::NativeClient;
using concord::storage::v2MerkleTree::MetadataKeyManipulator;

using namespace ::testing;
using namespace std::literals;

class replica_state_sync_test : public Test, public IReader {
  void SetUp() override {
    destroyDb();
    db_ = TestRocksDb::createNative();
    const auto link_st_chain = true;
    blockchain_.emplace(
        db_,
        link_st_chain,
        std::map<std::string, CATEGORY_TYPE>{{kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}});

    metadata_ = std::make_shared<PersistentStorageImp>(kNumOfReplicas, kFVal, kCVal);
    auto num_of_objects = std::uint16_t{0};
    auto obj_descriptors = metadata_->getDefaultMetadataObjectDescriptors(num_of_objects);
    auto db_metadata_storage =
        std::make_unique<DBMetadataStorage>(db_->asIDBClient().get(), std::make_unique<MetadataKeyManipulator>());
    db_metadata_storage->initMaxSizeOfObjects(obj_descriptors.get(), num_of_objects);
    metadata_->init(std::move(db_metadata_storage));
  }

  void TearDown() override { destroyDb(); }

  void destroyDb() {
    blockchain_.reset();
    metadata_.reset();
    db_.reset();
    ASSERT_EQ(0, db_.use_count());
    ASSERT_EQ(0, metadata_.use_count());
    cleanup();
  }

 public:
  std::optional<Value> get(const std::string &category_id, const std::string &key, BlockId block_id) const override {
    throw std::logic_error{"IReader::get() should not be called"};
  }

  std::optional<Value> getLatest(const std::string &category_id, const std::string &key) const override {
    return blockchain_->getLatest(kConcordInternalCategoryId, key);
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
  void addBlockWithBftSeqNum(std::uint64_t seq_number) {
    auto updates = Updates{};
    auto ver_updates = VersionedUpdates{};
    ver_updates.addUpdate(std::string{BlockMetadata::kBlockMetadataKeyStr}, block_metadata_.serialize(seq_number));
    updates.add(kConcordInternalCategoryId, std::move(ver_updates));
    blockchain_->addBlock(std::move(updates));
  }

  void addBlockWithoutBftSeqNum() { blockchain_->addBlock(Updates{}); }

  void persistLastBlockIdInMetadata() {
    constexpr auto in_transaction = false;
    concord::kvbc::persistLastBlockIdInMetadata<in_transaction>(*blockchain_, metadata_);
  }

 protected:
  BlockMetadata block_metadata_{*this};
  ReplicaStateSyncImp replica_state_sync_{new BlockMetadata{*this}};
  logging::Logger logger_{logging::getLogger("com.vmware.replica_state_sync_test")};
  const std::uint32_t kMaxNumOfBlocksToDelete = 10;
  const std::uint16_t kNumOfReplicas = 4;
  const std::uint16_t kFVal = 1;
  const std::uint16_t kCVal = 0;

  // Test case specific members that are reset on SetUp() and TearDown().
  std::shared_ptr<NativeClient> db_;
  std::optional<KeyValueBlockchain> blockchain_;
  std::shared_ptr<PersistentStorageImp> metadata_;
};

// Make GTest display tests differently, but still share the setup code.
using replica_state_sync_on_bft_seq_num_test = replica_state_sync_test;
using replica_state_sync_on_block_id_test = replica_state_sync_test;
using replica_state_sync_test = replica_state_sync_test;

TEST_F(replica_state_sync_on_bft_seq_num_test, empty_blockchain_and_0_bft_seq_num) {
  const auto last_executed_bft_seq_num = 0;
  ASSERT_EQ(0,
            replica_state_sync_.executeBasedOnBftSeqNum(
                logger_, *blockchain_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));
}

TEST_F(replica_state_sync_on_bft_seq_num_test, non_empty_blockchain_and_0_bft_seq_num) {
  addBlockWithBftSeqNum(1);
  addBlockWithBftSeqNum(2);
  const auto last_executed_bft_seq_num = 0;
  ASSERT_EQ(0,
            replica_state_sync_.executeBasedOnBftSeqNum(
                logger_, *blockchain_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_bft_seq_num_test, empty_blockchain_and_non_0_bft_seq_num) {
  const auto last_executed_bft_seq_num = 42;
  ASSERT_EQ(0,
            replica_state_sync_.executeBasedOnBftSeqNum(
                logger_, *blockchain_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));
}

TEST_F(replica_state_sync_on_bft_seq_num_test, bft_seq_num_equal_to_block_seq_num) {
  addBlockWithBftSeqNum(1);
  addBlockWithBftSeqNum(2);
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 2;
  ASSERT_EQ(0,
            replica_state_sync_.executeBasedOnBftSeqNum(
                logger_, *blockchain_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));

  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_bft_seq_num_test, bft_seq_num_bigger_than_block_seq_num) {
  addBlockWithBftSeqNum(1);
  addBlockWithBftSeqNum(2);
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 42;
  ASSERT_EQ(0,
            replica_state_sync_.executeBasedOnBftSeqNum(
                logger_, *blockchain_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_bft_seq_num_test, bft_seq_num_less_than_block_seq_num) {
  addBlockWithBftSeqNum(1);
  addBlockWithBftSeqNum(2);
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 1;
  ASSERT_EQ(1,
            replica_state_sync_.executeBasedOnBftSeqNum(
                logger_, *blockchain_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));

  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(1, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_bft_seq_num_test, cannot_delete_only_block_left) {
  addBlockWithBftSeqNum(2);  // block 1
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(1, blockchain_->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 1;
  ASSERT_THROW(replica_state_sync_.executeBasedOnBftSeqNum(
                   logger_, *blockchain_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete),
               std::exception);
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(1, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_bft_seq_num_test, cannot_delete_only_block_left_with_pruned_block) {
  addBlockWithBftSeqNum(2);  // block 1
  addBlockWithBftSeqNum(3);  // block 2
  // Prune block 1.
  blockchain_->deleteBlock(1);
  ASSERT_EQ(2, blockchain_->getGenesisBlockId());
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 2;
  ASSERT_THROW(replica_state_sync_.executeBasedOnBftSeqNum(
                   logger_, *blockchain_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete),
               std::exception);
  ASSERT_EQ(2, blockchain_->getGenesisBlockId());
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_bft_seq_num_test, bft_too_many_inconsistent_blocks_detected) {
  addBlockWithBftSeqNum(1);
  addBlockWithBftSeqNum(2);
  addBlockWithBftSeqNum(3);
  addBlockWithBftSeqNum(4);
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(4, blockchain_->getLastReachableBlockId());

  const auto last_executed_bft_seq_num = 2;
  ASSERT_THROW(replica_state_sync_.executeBasedOnBftSeqNum(logger_, *blockchain_, last_executed_bft_seq_num, 1),
               std::exception);

  // Only one block is expected to be deleted.
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(3, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_block_id_test, missing_block_id_in_metadata) {
  ASSERT_DEATH(replica_state_sync_.executeBasedOnBlockId(logger_, *blockchain_, metadata_, kMaxNumOfBlocksToDelete),
               "");
}

TEST_F(replica_state_sync_on_block_id_test, empty_db) {
  persistLastBlockIdInMetadata();
  ASSERT_EQ(0, replica_state_sync_.executeBasedOnBlockId(logger_, *blockchain_, metadata_, kMaxNumOfBlocksToDelete));
}

TEST_F(replica_state_sync_on_block_id_test, zero_max_blocks_to_delete) {
  addBlockWithoutBftSeqNum();
  persistLastBlockIdInMetadata();
  addBlockWithoutBftSeqNum();  // Force out of sync by adding a second block without persisting in metadata.
  ASSERT_EQ(0, replica_state_sync_.executeBasedOnBlockId(logger_, *blockchain_, metadata_, 0));
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_block_id_test, metadata_block_id_bigger_than_last_block_id) {
  // Add two blocks, persist last block ID = 2 in metadata and then delete the last block.
  addBlockWithoutBftSeqNum();
  addBlockWithoutBftSeqNum();
  persistLastBlockIdInMetadata();
  blockchain_->deleteLastReachableBlock();
  ASSERT_EQ(0, replica_state_sync_.executeBasedOnBlockId(logger_, *blockchain_, metadata_, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(1, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_block_id_test, max_num_of_blocks_to_delete_is_honoured) {
  addBlockWithoutBftSeqNum();
  addBlockWithoutBftSeqNum();
  persistLastBlockIdInMetadata();
  // Add one more block than kMaxNumOfBlocksToDelete.
  for (auto i = 0u; i < kMaxNumOfBlocksToDelete + 1; ++i) {
    addBlockWithoutBftSeqNum();
  }
  ASSERT_THROW(replica_state_sync_.executeBasedOnBlockId(logger_, *blockchain_, metadata_, kMaxNumOfBlocksToDelete),
               std::runtime_error);
  // 2 blocks added prior to metadata persistence and 1 that is not deleted as it exceeeds `kMaxNumOfBlocksToDelete`.
  ASSERT_EQ(2 + 1, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_block_id_test, in_sync) {
  addBlockWithoutBftSeqNum();
  addBlockWithoutBftSeqNum();
  persistLastBlockIdInMetadata();
  ASSERT_EQ(0, replica_state_sync_.executeBasedOnBlockId(logger_, *blockchain_, metadata_, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_block_id_test, out_of_sync) {
  addBlockWithoutBftSeqNum();
  addBlockWithoutBftSeqNum();
  persistLastBlockIdInMetadata();
  addBlockWithoutBftSeqNum();
  addBlockWithoutBftSeqNum();
  addBlockWithoutBftSeqNum();
  ASSERT_EQ(5, blockchain_->getLastReachableBlockId());
  ASSERT_EQ(3, replica_state_sync_.executeBasedOnBlockId(logger_, *blockchain_, metadata_, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_block_id_test, cannot_delete_only_block_left) {
  persistLastBlockIdInMetadata();
  addBlockWithoutBftSeqNum();
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(1, blockchain_->getLastReachableBlockId());

  ASSERT_THROW(replica_state_sync_.executeBasedOnBlockId(logger_, *blockchain_, metadata_, kMaxNumOfBlocksToDelete),
               std::exception);
  ASSERT_EQ(1, blockchain_->getGenesisBlockId());
  ASSERT_EQ(1, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_on_block_id_test, cannot_delete_only_block_left_with_pruned_block) {
  // Add 3 blocks, but persist metadata at block ID = 1.
  addBlockWithoutBftSeqNum();
  persistLastBlockIdInMetadata();
  addBlockWithoutBftSeqNum();
  addBlockWithoutBftSeqNum();

  blockchain_->deleteBlock(1);

  // Expect a throw from KVBC when trying to delete the only block (genesis, block ID = 2) in the system.
  ASSERT_THROW(replica_state_sync_.executeBasedOnBlockId(logger_, *blockchain_, metadata_, kMaxNumOfBlocksToDelete),
               std::exception);
  ASSERT_EQ(2, blockchain_->getGenesisBlockId());
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_test, out_of_sync_on_upgade) {
  // Simulate out of sync based on BFT seq number on the first startup after software upgrade that supports block ID
  // sync.
  addBlockWithBftSeqNum(1);
  addBlockWithBftSeqNum(2);

  const auto last_executed_bft_seq_num = 1;
  ASSERT_EQ(1,
            replica_state_sync_.execute(
                logger_, *blockchain_, metadata_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(1, blockchain_->getLastReachableBlockId());

  // Last block ID is persisted by execute() in metadata.
  auto last_mtd_block_id = getLastBlockIdFromMetadata(metadata_);
  ASSERT_TRUE(last_mtd_block_id.has_value());
  ASSERT_EQ(1, *last_mtd_block_id);

  // We don't delete anything on a subsequent startup.
  ASSERT_EQ(0,
            replica_state_sync_.execute(
                logger_, *blockchain_, metadata_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(1, blockchain_->getLastReachableBlockId());

  // Last block ID remains the same.
  last_mtd_block_id = getLastBlockIdFromMetadata(metadata_);
  ASSERT_TRUE(last_mtd_block_id.has_value());
  ASSERT_EQ(1, *last_mtd_block_id);
}

TEST_F(replica_state_sync_test, metadata_not_persisted_on_first_block_after_software_upgrade) {
  // Add blocks as if from the old software version.
  addBlockWithBftSeqNum(1);
  addBlockWithBftSeqNum(2);

  const auto last_executed_bft_seq_num = 2;
  ASSERT_EQ(0,
            replica_state_sync_.executeBasedOnBftSeqNum(
                logger_, *blockchain_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(2, blockchain_->getLastReachableBlockId());

  // Add a block after the upgrade from the new software version, without persisting metadata.
  addBlockWithoutBftSeqNum();
  ASSERT_EQ(3, blockchain_->getLastReachableBlockId());

  // Calling execute() will persist the last block ID in metadata. execute() assumes that the first block after upgrade
  // is successfuly added to both KVBC and metadata by the replica. If that is not the case as in this test, we don't
  // have a way of knowing how many blocks to delete and, therefore, we don't delete any as the last block ID
  // in metadata has just been persisted by execute() with the last block ID from KVBC, making them equal.
  ASSERT_EQ(0,
            replica_state_sync_.execute(
                logger_, *blockchain_, metadata_, last_executed_bft_seq_num + 1, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(3, blockchain_->getLastReachableBlockId());

  // Last block ID is persisted by execute() in metadata.
  const auto last_mtd_block_id = getLastBlockIdFromMetadata(metadata_);
  ASSERT_TRUE(last_mtd_block_id.has_value());
  ASSERT_EQ(3, *last_mtd_block_id);
}

TEST_F(replica_state_sync_test, out_of_sync_after_software_upgrade) {
  addBlockWithBftSeqNum(1);
  addBlockWithBftSeqNum(2);
  addBlockWithoutBftSeqNum();
  persistLastBlockIdInMetadata();
  addBlockWithoutBftSeqNum();

  const auto last_executed_bft_seq_num = 3;
  ASSERT_EQ(1,
            replica_state_sync_.execute(
                logger_, *blockchain_, metadata_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(3, blockchain_->getLastReachableBlockId());
}

TEST_F(replica_state_sync_test, in_sync_after_software_upgrade) {
  addBlockWithBftSeqNum(1);
  addBlockWithBftSeqNum(2);
  addBlockWithoutBftSeqNum();
  addBlockWithoutBftSeqNum();
  persistLastBlockIdInMetadata();

  const auto last_executed_bft_seq_num = 4;
  ASSERT_EQ(0,
            replica_state_sync_.execute(
                logger_, *blockchain_, metadata_, last_executed_bft_seq_num, kMaxNumOfBlocksToDelete));
  ASSERT_EQ(4, blockchain_->getLastReachableBlockId());
}

}  // namespace
