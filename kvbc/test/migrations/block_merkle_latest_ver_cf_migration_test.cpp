// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "migrations/block_merkle_latest_ver_cf_migration.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "categorization/column_families.h"
#include "categorization/db_categories.h"
#include "categorization/kv_blockchain.h"
#include "hex_tools.h"
#include "sha_hash.hpp"
#include "storage/test/storage_test_common.h"

#include "util/filesystem.hpp"

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

namespace {

using namespace concord::kvbc::categorization;
using namespace concord::kvbc::migrations;
using namespace concord::storage::rocksdb;
using namespace concord::util;
using namespace ::testing;

using concord::kvbc::categorization::detail::BLOCK_MERKLE_LATEST_KEY_VERSION_CF;

class block_merkle_latest_ver_cf_migration_test : public Test {
  void SetUp() override {
    cleanupTestData();
    createKvbc();
    addBlocks();
  }

  void TearDown() override { cleanupTestData(); }

 protected:
  auto createMigration(size_t batch_size) {
    return BlockMerkleLatestVerCfMigration{rocksDbPath(db_path_id_), rocksDbPath(export_path_id_), batch_size};
  }

  void createKvbc() {
    db_ = TestRocksDb::createNative(db_path_id_);
    const auto link_st_chain = true;
    kvbc_.emplace(db_,
                  link_st_chain,
                  std::map<std::string, CATEGORY_TYPE>{{kExecutionProvableCategory, CATEGORY_TYPE::block_merkle},
                                                       {kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv},
                                                       {kVersionedCategoryId_, CATEGORY_TYPE::versioned_kv}});
  }

  void closeDb() {
    kvbc_.reset();
    db_.reset();
    ASSERT_EQ(0, db_.use_count());
  }

  void cleanupTestData() {
    closeDb();
    cleanup(db_path_id_);
    cleanup(export_path_id_);
  }

  void addBlocks() {
    // Block 1: a -> va1, b -> vb1
    {
      auto updates = Updates{};
      auto merkle_updates = BlockMerkleUpdates{};
      merkle_updates.addUpdate("a", "va1");
      merkle_updates.addUpdate("b", "vb1");
      updates.add(kExecutionProvableCategory, std::move(merkle_updates));
      kvbc_->addBlock(std::move(updates));
    }

    // Block 2: c -> vc2, a -> va2
    {
      auto updates = Updates{};
      auto merkle_updates = BlockMerkleUpdates{};
      merkle_updates.addUpdate("c", "vc2");
      merkle_updates.addUpdate("a", "va2");
      updates.add(kExecutionProvableCategory, std::move(merkle_updates));
      kvbc_->addBlock(std::move(updates));
    }

    // Block 3: no updates
    {
      auto updates = Updates{};
      kvbc_->addBlock(std::move(updates));
    }

    // Block 4: delete a, d -> vd4
    {
      auto updates = Updates{};
      auto merkle_updates = BlockMerkleUpdates{};
      merkle_updates.addDelete("a");
      merkle_updates.addUpdate("d", "vd4");
      updates.add(kExecutionProvableCategory, std::move(merkle_updates));
      kvbc_->addBlock(std::move(updates));
    }

    // Block 5: internal updates only
    {
      auto updates = Updates{};
      auto internal_updates = VersionedUpdates{};
      internal_updates.addUpdate("internal_key", "internal_value");
      updates.add(kVersionedCategoryId_, std::move(internal_updates));
      kvbc_->addBlock(std::move(updates));
    }

    // Block 6: b -> vb6
    {
      auto updates = Updates{};
      auto merkle_updates = BlockMerkleUpdates{};
      merkle_updates.addUpdate("b", "vb6");
      updates.add(kExecutionProvableCategory, std::move(merkle_updates));
      kvbc_->addBlock(std::move(updates));
    }
  }

  // Simulate previous format of BLOCK_MERKLE_LATEST_KEY_VERSION_CF, i.e. key_hash -> latest_version. Migration will
  // then migrate from the previous version to raw_key -> latest_version.
  void downgrade() {
    auto batch = NativeWriteBatch{db_};
    auto it = db_->getIterator(BLOCK_MERKLE_LATEST_KEY_VERSION_CF);

    // Firstly, accumulate the key-values in a write batch, with the key hashes as keys.
    it.first();
    while (it) {
      const auto hash = SHA3_256{}.digest(it.keyView().data(), it.keyView().size());
      batch.put(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, hash, it.valueView());
      it.next();
    }

    // Secondly, remove all keys in the BLOCK_MERKLE_LATEST_KEY_VERSION_CF column family.
    {
      it.first();
      auto del_batch = NativeWriteBatch{db_};
      del_batch.delRange(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, it.keyView(), kKeyAfterLast);
      db_->write(std::move(del_batch));
    }

    // Thirdly, remove the migration key as it is not supposed to be there in the previous DB format.
    batch.del(BlockMerkleLatestVerCfMigration::migrationKey());

    // Finally, write the key-values with the key hashes as keys.
    db_->write(std::move(batch));

    closeDb();
  }

  std::unordered_set<std::string> columnFamilyKeys(const std::string& columnFamily) const {
    auto ret = std::unordered_set<std::string>{};
    auto iter = db_->getIterator(columnFamily);
    iter.first();
    while (iter) {
      ret.insert(iter.key());
      iter.next();
    }
    return ret;
  }

  void printColumnFamilyKeyValues(const std::string& columnFamily) const {
    auto iter = db_->getIterator(columnFamily);
    iter.first();
    while (iter) {
      concordUtils::hexPrint(std::cout, iter.keyView().data(), iter.keyView().size());
      std::cout << ": ";
      concordUtils::hexPrint(std::cout, iter.valueView().data(), iter.valueView().size());
      std::cout << std::endl;
      iter.next();
    }
  }

  std::optional<std::string> currentState(BlockMerkleLatestVerCfMigration& migration) const {
    return migration.db()->get(BlockMerkleLatestVerCfMigration::migrationKey());
  }

  void executeAndVerifyMigration(size_t batch_size) {
    {
      auto migration = createMigration(batch_size);
      const auto status = migration.execute();
      ASSERT_EQ(BlockMerkleLatestVerCfMigration::ExecutionStatus::kExecuted, status);
    }

    createKvbc();

    ASSERT_FALSE(fs::exists(rocksDbPath(export_path_id_)));
    ASSERT_FALSE(db_->hasColumnFamily(BlockMerkleLatestVerCfMigration::temporaryColumnFamily()));
    const auto state = db_->get(BlockMerkleLatestVerCfMigration::migrationKey());
    ASSERT_TRUE(state.has_value());
    ASSERT_EQ(*state, BlockMerkleLatestVerCfMigration::kStateMigrationNotNeededOrCompleted);

    const auto latest_ver_cf_keys = columnFamilyKeys(BLOCK_MERKLE_LATEST_KEY_VERSION_CF);
    ASSERT_THAT(latest_ver_cf_keys, ContainerEq(std::unordered_set<std::string>{"a", "b", "c", "d"}));
    printColumnFamilyKeyValues(BLOCK_MERKLE_LATEST_KEY_VERSION_CF);

    // Key "a".
    {
      const auto a_ver = kvbc_->getLatestVersion(kExecutionProvableCategory, "a");
      ASSERT_TRUE(a_ver.has_value());
      ASSERT_EQ(4, a_ver->version);
      ASSERT_TRUE(a_ver->deleted);

      const auto a_val = kvbc_->getLatest(kExecutionProvableCategory, "a");
      ASSERT_FALSE(a_val.has_value());
    }

    // Key "b".
    {
      const auto b_ver = kvbc_->getLatestVersion(kExecutionProvableCategory, "b");
      ASSERT_TRUE(b_ver.has_value());
      ASSERT_EQ(6, b_ver->version);
      ASSERT_FALSE(b_ver->deleted);

      const auto b_val = kvbc_->getLatest(kExecutionProvableCategory, "b");
      ASSERT_TRUE(b_val.has_value());
      const auto& b_merkle_val = std::get<MerkleValue>(*b_val);
      ASSERT_EQ(6, b_merkle_val.block_id);
      ASSERT_EQ("vb6", b_merkle_val.data);
    }

    // Key "c".
    {
      const auto c_ver = kvbc_->getLatestVersion(kExecutionProvableCategory, "c");
      ASSERT_TRUE(c_ver.has_value());
      ASSERT_EQ(2, c_ver->version);
      ASSERT_FALSE(c_ver->deleted);

      const auto c_val = kvbc_->getLatest(kExecutionProvableCategory, "c");
      ASSERT_TRUE(c_val.has_value());
      const auto& c_merkle_val = std::get<MerkleValue>(*c_val);
      ASSERT_EQ(2, c_merkle_val.block_id);
      ASSERT_EQ("vc2", c_merkle_val.data);
    }

    // Key "d".
    {
      const auto d_ver = kvbc_->getLatestVersion(kExecutionProvableCategory, "d");
      ASSERT_TRUE(d_ver.has_value());
      ASSERT_EQ(4, d_ver->version);
      ASSERT_FALSE(d_ver->deleted);

      const auto d_val = kvbc_->getLatest(kExecutionProvableCategory, "d");
      ASSERT_TRUE(d_val.has_value());
      const auto& d_merkle_val = std::get<MerkleValue>(*d_val);
      ASSERT_EQ(4, d_merkle_val.block_id);
      ASSERT_EQ("vd4", d_merkle_val.data);
    }
  }

 protected:
  const std::string kKeyAfterLast{"e"};  // as last key added is "d"
  const std::string kVersionedCategoryId_{"ver"};
  const std::size_t db_path_id_{0};
  const std::size_t export_path_id_{1};
  std::shared_ptr<NativeClient> db_;
  std::optional<KeyValueBlockchain> kvbc_;
};

TEST_F(block_merkle_latest_ver_cf_migration_test, successful_migration) {
  downgrade();
  executeAndVerifyMigration(1);
}

TEST_F(block_merkle_latest_ver_cf_migration_test, successful_migration_with_different_batch_size) {
  for (size_t batch_size = 0; batch_size <= 50; batch_size++) {
    downgrade();
    executeAndVerifyMigration(batch_size);
  }
}

TEST_F(block_merkle_latest_ver_cf_migration_test, not_needed) {
  closeDb();
  // We try to migrate after adding blocks only, before downgrading the DB.
  auto migration = createMigration(2);
  const auto status = migration.execute();
  ASSERT_EQ(BlockMerkleLatestVerCfMigration::ExecutionStatus::kNotNeededOrAlreadyExecuted, status);
}

TEST_F(block_merkle_latest_ver_cf_migration_test, already_executed) {
  downgrade();
  executeAndVerifyMigration(2);
  closeDb();
  auto migration = createMigration(3);
  const auto status = migration.execute();
  ASSERT_EQ(BlockMerkleLatestVerCfMigration::ExecutionStatus::kNotNeededOrAlreadyExecuted, status);
}

TEST_F(block_merkle_latest_ver_cf_migration_test, stop_before_checkpoint_db) {
  downgrade();
  {
    auto migration = createMigration(4);
    migration.removeExportDir();
    migration.dropTempLatestVerCf();
    const auto current_state = currentState(migration);
    ASSERT_FALSE(current_state.has_value());
  }
  executeAndVerifyMigration(3);
}

TEST_F(block_merkle_latest_ver_cf_migration_test, stop_after_checkpoint_db) {
  downgrade();
  {
    auto migration = createMigration(5);
    migration.removeExportDir();
    migration.dropTempLatestVerCf();
    migration.checkpointDB();
    const auto current_state = currentState(migration);
    ASSERT_FALSE(current_state.has_value());
  }
  executeAndVerifyMigration(4);
}

TEST_F(block_merkle_latest_ver_cf_migration_test, stop_after_imported_temp_latest_ver_cf) {
  downgrade();
  {
    auto migration = createMigration(6);
    migration.removeExportDir();
    migration.dropTempLatestVerCf();
    migration.checkpointDB();
    migration.exportLatestVerCf();
    migration.importTempLatestVerCf();
    const auto current_state = currentState(migration);
    ASSERT_TRUE(current_state.has_value());
    ASSERT_EQ(BlockMerkleLatestVerCfMigration::kStateImportedTempCf, *current_state);
  }
  executeAndVerifyMigration(5);
}

TEST_F(block_merkle_latest_ver_cf_migration_test, stop_after_clear_existing_latest_ver_cf) {
  downgrade();
  {
    auto migration = createMigration(7);
    migration.removeExportDir();
    migration.dropTempLatestVerCf();
    migration.checkpointDB();
    migration.exportLatestVerCf();
    migration.importTempLatestVerCf();
    migration.clearExistingLatestVerCf();
    const auto current_state = currentState(migration);
    ASSERT_TRUE(current_state.has_value());
    ASSERT_EQ(BlockMerkleLatestVerCfMigration::kStateImportedTempCf, *current_state);
  }
  executeAndVerifyMigration(6);
}

TEST_F(block_merkle_latest_ver_cf_migration_test, simulate_stop_in_the_middle_of_iterate_and_migrate) {
  downgrade();
  auto dummy_key = std::string{};
  {
    auto migration = createMigration(8);
    migration.removeExportDir();
    migration.dropTempLatestVerCf();
    migration.checkpointDB();
    migration.exportLatestVerCf();
    migration.importTempLatestVerCf();
    migration.clearExistingLatestVerCf();
    const auto current_state = currentState(migration);
    ASSERT_TRUE(current_state.has_value());
    ASSERT_EQ(BlockMerkleLatestVerCfMigration::kStateImportedTempCf, *current_state);

    // Insert a dummy key in the BLOCK_MERKLE_LATEST_KEY_VERSION_CF column family, simulating a "partial" migration.
    // We expect that this key is not there after migration.
    auto iter = migration.db()->getIterator(BlockMerkleLatestVerCfMigration::temporaryColumnFamily());
    iter.first();
    dummy_key = iter.key();
    migration.db()->put(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, dummy_key, iter.value());
  }
  executeAndVerifyMigration(7);
  const auto dummy_key_val = db_->get(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, dummy_key);
  ASSERT_FALSE(dummy_key_val.has_value());
}

TEST_F(block_merkle_latest_ver_cf_migration_test, stop_after_iterate_and_migrate_latest_ver_cf) {
  downgrade();
  {
    auto migration = createMigration(9);
    migration.removeExportDir();
    migration.dropTempLatestVerCf();
    migration.checkpointDB();
    migration.exportLatestVerCf();
    migration.importTempLatestVerCf();
    migration.clearExistingLatestVerCf();
    migration.iterateAndMigrate();
    const auto current_state = currentState(migration);
    ASSERT_TRUE(current_state.has_value());
    ASSERT_EQ(BlockMerkleLatestVerCfMigration::kStateMigrated, *current_state);
  }
  executeAndVerifyMigration(8);
}

TEST_F(block_merkle_latest_ver_cf_migration_test, stop_after_commit_complete) {
  downgrade();
  {
    auto migration = createMigration(10);
    migration.removeExportDir();
    migration.dropTempLatestVerCf();
    migration.checkpointDB();
    migration.exportLatestVerCf();
    migration.importTempLatestVerCf();
    migration.clearExistingLatestVerCf();
    migration.iterateAndMigrate();
    migration.removeExportDir();
    migration.dropTempLatestVerCf();
    const auto current_state = currentState(migration);
    ASSERT_TRUE(current_state.has_value());
    ASSERT_EQ(BlockMerkleLatestVerCfMigration::kStateMigrated, *current_state);
  }
  executeAndVerifyMigration(9);
}

}  // namespace
