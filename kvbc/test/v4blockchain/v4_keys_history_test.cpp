// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include "v4blockchain/v4_blockchain.h"
#include "storage/test/storage_test_common.h"
#include "bftengine/ReplicaConfig.hpp"
#include <utility>
#include <random>

using concord::storage::rocksdb::NativeClient;
using namespace concord::kvbc;
using namespace ::testing;

namespace {

class v4_kvbc : public Test {
 protected:
  void SetUp() override {
    destroyDb();
    db = TestRocksDb::createNative();
    categories = std::map<std::string, categorization::CATEGORY_TYPE>{
        {"merkle", categorization::CATEGORY_TYPE::block_merkle},
        {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
        {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
        {"immutable", categorization::CATEGORY_TYPE::immutable}};
    category_mapping_ = std::make_unique<v4blockchain::detail::Categories>(db, categories);
  }

  void TearDown() override { destroyDb(); }

  void destroyDb() {
    db.reset();
    ASSERT_EQ(0, db.use_count());
    cleanup();
  }

  typedef struct keyAndVersion_ {
    std::string key;
    BlockId block_id;
    bool operator<(const keyAndVersion_& o) const { return key < o.key || (key == o.key && block_id < o.block_id); }
  } keyAndVersion;

  typedef struct expectedHistory_ {
    std::optional<BlockId> last_modified;
    std::string cat_id;
  } expectedHistory;

  using ExpectedMap = std::map<keyAndVersion, expectedHistory>;

  std::optional<std::pair<std::string, std::string>> add_key_with_random(std::mt19937& gen,
                                                                         const std::string& category,
                                                                         const BlockId block_id,
                                                                         const int key_idx,
                                                                         ExpectedMap& expected_snapshots) {
    // Keys are: <category>_key_<key_idx>
    // Values are: <category>_value_<block_id>_<key_idx>
    std::optional<std::pair<std::string, std::string>> ret_val{std::nullopt};
    std::uniform_int_distribution dist_bool{0, 1};
    const std::string key = category + "_key" + std::to_string(key_idx);
    const std::string val = category + "_val" + std::to_string(block_id) + std::to_string(key_idx);
    const keyAndVersion merkle_key_ver{key, block_id};
    if (dist_bool(gen)) {
      ret_val = std::make_pair(key, val);
      expected_snapshots.emplace(merkle_key_ver, expectedHistory{block_id, category});
    } else {
      auto val_last_block = expected_snapshots.find({key, block_id - 1});
      if (val_last_block != expected_snapshots.end()) {
        expected_snapshots.emplace(merkle_key_ver, val_last_block->second);
      } else {
        expected_snapshots.emplace(merkle_key_ver, expectedHistory{std::nullopt, category});
      }
    }
    return ret_val;
  }

  std::string delete_random_key(std::mt19937& gen,
                                const int num_keys,
                                const std::string& category,
                                const BlockId block_id,
                                ExpectedMap& expected_snapshots) {
    std::uniform_int_distribution dist_for_delete_key{0, num_keys};
    const int key_idx = dist_for_delete_key(gen);
    std::string key = category + "_key" + std::to_string(key_idx);
    const keyAndVersion key_ver_update{key, block_id};
    // a key might be deleted before it was created or be deleted twice. anyway, KeysHistory does not care about that.
    expected_snapshots[key_ver_update] = expectedHistory{block_id, category};
    return key;
  }

  void assert_expected_match_keys_history(const v4blockchain::detail::KeysHistory& keys_history,
                                          const ExpectedMap& expected_snapshots,
                                          const BlockId latest_block) {
    for (const auto& [k, v] : expected_snapshots) {
      auto opt_val = keys_history.getVersionFromHistory(v.cat_id, k.key, k.block_id);
      if (v.last_modified) {
        ASSERT_NE(opt_val, std::nullopt);
        ASSERT_EQ(opt_val.value(), v.last_modified.value());
      } else {
        ASSERT_EQ(opt_val, std::nullopt);
      }
    }
  }

  void add_merkle_keys_to_block(v4blockchain::detail::KeysHistory& keys_history,
                                const BlockId block_id,
                                const int num_keys) {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    for (int j = 0; j < num_keys; j++) {
      const std::string merkle_key = std::to_string(j);
      const std::string merkle_val = merkle_category + "_key_" + std::to_string(block_id);
      merkle_updates.addUpdate(std::string(merkle_key), std::string(merkle_val));
    }
    updates.add(merkle_category, std::move(merkle_updates));
    auto wb = db->getBatch();
    keys_history.addBlockKeys(updates, block_id, wb);
    db->write(std::move(wb));
  }

 protected:
  std::map<std::string, categorization::CATEGORY_TYPE> categories;
  std::unique_ptr<v4blockchain::detail::Categories> category_mapping_;
  std::shared_ptr<NativeClient> db;
  const std::string merkle_category = "merkle";
  const std::string versioned_category = "versioned";
  const std::string immutable_category = "immutable";
};

TEST_F(v4_kvbc, creation) {
  v4blockchain::detail::KeysHistory keys_history{db, categories};
  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::KEYS_HISTORY_CF));
  auto options = db->columnFamilyOptions(v4blockchain::detail::KEYS_HISTORY_CF);
  ASSERT_NE(options.compaction_filter, nullptr);
  ASSERT_EQ(std::string(options.compaction_filter->Name()), std::string("KeysHistoryCompactionFilter"));
}

TEST_F(v4_kvbc, add_and_get_keys_history) {
  v4blockchain::detail::KeysHistory keys_history{db, categories};
  std::random_device seed;
  std::mt19937 gen{seed()};                       // seed the generator
  std::uniform_int_distribution dist{5, 20};      // set min and max
  const BlockId num_blocks = (BlockId)dist(gen);  // generate number
  const int num_keys = dist(gen);                 // generate number
  ExpectedMap expected_snapshots;

  for (BlockId i = 1; i <= num_blocks; i++) {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    categorization::VersionedUpdates ver_updates;
    categorization::ImmutableUpdates immutable_updates;
    // add or update keys with random
    for (int j = 0; j < num_keys; j++) {
      auto opt_key_val = add_key_with_random(gen, merkle_category, i, j, expected_snapshots);
      if (opt_key_val) {
        merkle_updates.addUpdate(std::string(opt_key_val.value().first), std::string(opt_key_val.value().second));
      }
      opt_key_val = add_key_with_random(gen, versioned_category, i, j, expected_snapshots);
      if (opt_key_val) {
        ver_updates.addUpdate(std::string(opt_key_val.value().first), std::string(opt_key_val.value().second));
      }
      opt_key_val = add_key_with_random(gen, immutable_category, i, j, expected_snapshots);
      if (opt_key_val) {
        immutable_updates.addUpdate(std::string(opt_key_val.value().first),
                                    {std::string(opt_key_val.value().second), {"1"}});
      }
    }
    // delete a random key
    if (i > 1) {
      merkle_updates.addDelete(delete_random_key(gen, num_keys, merkle_category, i, expected_snapshots));
    }
    updates.add(merkle_category, std::move(merkle_updates));
    updates.add(versioned_category, std::move(ver_updates));
    updates.add(immutable_category, std::move(immutable_updates));
    auto wb = db->getBatch();
    keys_history.addBlockKeys(updates, i, wb);
    db->write(std::move(wb));
  }
  ///////////// Checking getVersionFromHistory /////////////
  assert_expected_match_keys_history(keys_history, expected_snapshots, num_blocks);
}

TEST_F(v4_kvbc, revertLastBlockKeys) {
  v4blockchain::detail::KeysHistory keys_history{db, categories};
  std::random_device seed;
  std::mt19937 gen{seed()};                       // seed the generator
  std::uniform_int_distribution dist{5, 20};      // set min and max
  const BlockId num_blocks = (BlockId)dist(gen);  // generate number
  ExpectedMap expected_snapshots;
  categorization::Updates updates_to_revert;

  // Keys are: <category_name>_key_<blockId>_<key_id>
  // Values are: <category_name>_value_<blockId>_<key_id>
  for (BlockId i = 1; i <= num_blocks; i++) {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    categorization::VersionedUpdates ver_updates;
    categorization::ImmutableUpdates immutable_updates;

    const int num_keys = dist(gen);  // generate number
    // add or update keys
    // don't add updates of the last block to expected_snapshots since it will be reverted
    for (int j = 0; j < num_keys; j++) {
      const std::string merkle_key = merkle_category + "_key_" + std::to_string(i) + "_" + std::to_string(j);
      const std::string merkle_val = merkle_category + "_val_" + std::to_string(i) + "_" + std::to_string(j);
      merkle_updates.addUpdate(std::string(merkle_key), std::string(merkle_val));
      const keyAndVersion merkle_key_ver{merkle_key, i};
      if (i < num_blocks) expected_snapshots.emplace(merkle_key_ver, expectedHistory{i, merkle_category});

      const std::string ver_key = versioned_category + "_key_" + std::to_string(i) + "_" + std::to_string(j);
      const std::string ver_val = versioned_category + "_val_" + std::to_string(i) + "_" + std::to_string(j);
      ver_updates.addUpdate(std::string(ver_key), std::string(ver_val));
      const keyAndVersion ver_key_ver{ver_key, i};
      if (i < num_blocks) expected_snapshots.emplace(ver_key_ver, expectedHistory{i, versioned_category});

      const std::string immutable_key = immutable_category + "_key_" + std::to_string(i) + "_" + std::to_string(j);
      const std::string immutable_val = immutable_category + "_val_" + std::to_string(i) + "_" + std::to_string(j);
      immutable_updates.addUpdate(std::string(immutable_key), {std::string(immutable_val), {"1"}});
      const keyAndVersion immutable_key_ver{immutable_key, i};
      if (i < num_blocks) expected_snapshots.emplace(immutable_key_ver, expectedHistory{i, immutable_category});
    }
    // delete
    if (i > 1) {
      std::uniform_int_distribution dist_old_block{1, int(i - 1)};
      const int key_id = 3;
      BlockId block_id = (BlockId)dist_old_block(gen);
      const auto merkle_key = merkle_category + "_key_" + std::to_string(block_id) + "_" + std::to_string(key_id);
      merkle_updates.addDelete(std::string(merkle_key));
      if (i < num_blocks) expected_snapshots[{merkle_key, i}] = expectedHistory{i, merkle_category};

      block_id = (BlockId)dist_old_block(gen);
      const auto ver_key = versioned_category + "_key_" + std::to_string(block_id) + "_" + std::to_string(key_id);
      ver_updates.addDelete(std::string(ver_key));
      if (i < num_blocks) expected_snapshots[{ver_key, i}] = expectedHistory{i, versioned_category};
    }
    updates.add(merkle_category, std::move(merkle_updates));
    updates.add(versioned_category, std::move(ver_updates));
    updates.add(immutable_category, std::move(immutable_updates));
    auto wb = db->getBatch();
    keys_history.addBlockKeys(updates, i, wb);
    db->write(std::move(wb));

    // keep the updates of the last block which is about to be reverted
    if (i == num_blocks) {
      updates_to_revert = updates;
    }
  }

  auto revert_wb = db->getBatch();
  keys_history.revertLastBlockKeys(updates_to_revert, num_blocks, revert_wb);
  db->write(std::move(revert_wb));

  assert_expected_match_keys_history(keys_history, expected_snapshots, num_blocks);
}

TEST_F(v4_kvbc, filter_keys) {
  v4blockchain::detail::KeysHistory keys_history{db, categories};
  std::random_device seed;
  std::mt19937 gen{seed()};                       // seed the generator
  std::uniform_int_distribution dist{20, 100};    // set min and max
  const BlockId num_blocks = (BlockId)dist(gen);  // generate number
  const int num_keys = dist(gen);
  std::uniform_int_distribution dist2{5, 19};
  const BlockId num_blocks_to_save_in_history = (BlockId)dist2(gen);

  auto orig_max_blocks_to_save = bftEngine::ReplicaConfig::instance().keysHistoryMaxBlocksNum;
  bftEngine::ReplicaConfig::instance().setkeysHistoryMaxBlocksNum(num_blocks_to_save_in_history);

  // add blocks
  for (BlockId i = 1; i <= num_blocks; i++) {
    add_merkle_keys_to_block(keys_history, i, num_keys);
  }
  v4blockchain::detail::Blockchain::global_latest_block_id = num_blocks;

  // compaction triggers filter
  auto prefix = category_mapping_->categoryPrefix(merkle_category);
  auto first_key = prefix + std::to_string(1) + v4blockchain::detail::Blockchain::generateKey(1);
  auto last_key = prefix + std::to_string(num_blocks) + v4blockchain::detail::Blockchain::generateKey(num_blocks);
  auto start_key = ::rocksdb::Slice(concord::storage::rocksdb::detail::toSlice(first_key));
  auto end_key = ::rocksdb::Slice(concord::storage::rocksdb::detail::toSlice(last_key));

  db->rawDB().CompactRange(::rocksdb::CompactRangeOptions{},
                           db->columnFamilyHandle(v4blockchain::detail::KEYS_HISTORY_CF),
                           &start_key,
                           &end_key);

  // make sure that keys of old blocks were filtered out from keys history
  for (BlockId i = 1; i <= num_blocks; i++) {
    for (int j = 0; j < num_keys; j++) {
      auto key = std::to_string(j);
      auto opt_val = keys_history.getVersionFromHistory(merkle_category, key, i);
      if (i > num_blocks - num_blocks_to_save_in_history) {
        ASSERT_TRUE(opt_val);
        ASSERT_EQ(opt_val.value(), i);
      } else {
        ASSERT_FALSE(opt_val);
      }
    }
  }

  // check the case when keysHistoryMaxBlocksNum == 0
  bftEngine::ReplicaConfig::instance().setkeysHistoryMaxBlocksNum(0);

  for (BlockId i = num_blocks + 1; i <= 2 * num_blocks; i++) {
    add_merkle_keys_to_block(keys_history, i, num_keys);
  }
  v4blockchain::detail::Blockchain::global_genesis_block_id = num_blocks + (BlockId)dist2(gen);

  // compaction triggers filter
  first_key = prefix + std::to_string(1) + v4blockchain::detail::Blockchain::generateKey(1);
  last_key = prefix + std::to_string(2 * num_blocks) + v4blockchain::detail::Blockchain::generateKey(2 * num_blocks);
  start_key = ::rocksdb::Slice(concord::storage::rocksdb::detail::toSlice(first_key));
  end_key = ::rocksdb::Slice(concord::storage::rocksdb::detail::toSlice(last_key));

  db->rawDB().CompactRange(::rocksdb::CompactRangeOptions{},
                           db->columnFamilyHandle(v4blockchain::detail::KEYS_HISTORY_CF),
                           &start_key,
                           &end_key);

  // make sure that keys of blocks below the genesis were filtered out from keys history
  for (BlockId i = num_blocks + 1; i <= 2 * num_blocks; i++) {
    for (int j = 0; j < num_keys; j++) {
      auto key = std::to_string(j);
      auto opt_val = keys_history.getVersionFromHistory(merkle_category, key, i);
      if (i >= v4blockchain::detail::Blockchain::global_genesis_block_id) {
        ASSERT_TRUE(opt_val);
        ASSERT_EQ(opt_val.value(), i);
      } else {
        ASSERT_FALSE(opt_val);
      }
    }
  }

  // cleanup test changes
  bftEngine::ReplicaConfig::instance().setkeysHistoryMaxBlocksNum(orig_max_blocks_to_save);
  v4blockchain::detail::Blockchain::global_latest_block_id = 0;
  v4blockchain::detail::Blockchain::global_genesis_block_id = 0;
}

TEST_F(v4_kvbc, follow_key_history) {
  v4blockchain::detail::KeysHistory keys_history{db, categories};
  std::random_device seed;
  std::mt19937 gen{seed()};                       // seed the generator
  std::uniform_int_distribution dist{10, 40};     // set min and max
  const BlockId num_blocks = (BlockId)dist(gen);  // generate number
  const int num_keys = dist(gen);
  std::optional<BlockId> opt_val;
  const auto key1 = "key1";

  // keys history CF is empty
  opt_val = keys_history.getVersionFromHistory(merkle_category, key1, num_blocks);
  ASSERT_EQ(opt_val, std::nullopt);

  // add blocks [1, num_blocks]
  for (BlockId i = 1; i <= num_blocks; i++) {
    add_merkle_keys_to_block(keys_history, i, num_keys);
    // key1 has not added yet
    opt_val = keys_history.getVersionFromHistory(merkle_category, key1, i);
    ASSERT_EQ(opt_val, std::nullopt);
  }

  // add key1 to block [num_blocks + 1]
  categorization::Updates updates;
  categorization::BlockMerkleUpdates merkle_updates;
  merkle_updates.addUpdate(key1, "initial_val");
  updates.add(merkle_category, std::move(merkle_updates));
  auto wb = db->getBatch();
  keys_history.addBlockKeys(updates, num_blocks + 1, wb);
  db->write(std::move(wb));

  // make sure that key1's history is correct
  opt_val = keys_history.getVersionFromHistory(merkle_category, key1, num_blocks);
  ASSERT_EQ(opt_val, std::nullopt);
  opt_val = keys_history.getVersionFromHistory(merkle_category, key1, num_blocks + 1);
  ASSERT_NE(opt_val, std::nullopt);
  ASSERT_EQ(opt_val.value(), num_blocks + 1);

  // add some additional blocks [num_blocks + 2, additional_blocks]
  const BlockId additional_blocks = num_blocks + (BlockId)dist(gen);  // generate number
  for (BlockId i = num_blocks + 2; i <= additional_blocks; i++) {
    add_merkle_keys_to_block(keys_history, i, num_keys);
    // key1 was last updated in block [num_blocks + 1]
    opt_val = keys_history.getVersionFromHistory(merkle_category, key1, i);
    ASSERT_NE(opt_val, std::nullopt);
    ASSERT_EQ(opt_val.value(), num_blocks + 1);
  }

  // update key1 at block [additional_blocks + 1]
  categorization::Updates updates1;
  categorization::BlockMerkleUpdates merkle_updates1;
  merkle_updates1.addUpdate(key1, "updated_val");
  updates1.add(merkle_category, std::move(merkle_updates1));
  wb = db->getBatch();
  keys_history.addBlockKeys(updates1, additional_blocks + 1, wb);
  db->write(std::move(wb));

  // make sure that key1's history is correct
  opt_val = keys_history.getVersionFromHistory(merkle_category, key1, additional_blocks);
  ASSERT_NE(opt_val, std::nullopt);
  ASSERT_EQ(opt_val.value(), num_blocks + 1);
  opt_val = keys_history.getVersionFromHistory(merkle_category, key1, additional_blocks + 1);
  ASSERT_NE(opt_val, std::nullopt);
  ASSERT_EQ(opt_val.value(), additional_blocks + 1);

  // add some more blocks [additional_blocks + 2, more_blocks]
  const BlockId more_blocks = additional_blocks + (BlockId)dist(gen);  // generate number
  for (BlockId i = additional_blocks + 2; i <= more_blocks; i++) {
    add_merkle_keys_to_block(keys_history, i, num_keys);
    // key1 was last updated on num_blocks + 1
    opt_val = keys_history.getVersionFromHistory(merkle_category, key1, i);
    ASSERT_NE(opt_val, std::nullopt);
    ASSERT_EQ(opt_val.value(), additional_blocks + 1);
  }

  // delete key1 in block [more_blocks + 1]
  categorization::Updates updates2;
  categorization::BlockMerkleUpdates merkle_updates2;
  merkle_updates2.addDelete(key1);
  updates2.add(merkle_category, std::move(merkle_updates2));
  wb = db->getBatch();
  keys_history.addBlockKeys(updates2, more_blocks + 1, wb);
  db->write(std::move(wb));

  // make sure that key1's history is correct
  opt_val = keys_history.getVersionFromHistory(merkle_category, key1, more_blocks);
  ASSERT_NE(opt_val, std::nullopt);
  ASSERT_EQ(opt_val.value(), additional_blocks + 1);
  // deletion of key is saved to keys history as well
  opt_val = keys_history.getVersionFromHistory(merkle_category, key1, more_blocks + 1);
  ASSERT_NE(opt_val, std::nullopt);
  ASSERT_EQ(opt_val.value(), more_blocks + 1);

  // add some extra blocks [more_blocks + 2, extra_blocks]
  const BlockId extra_blocks = more_blocks + (BlockId)dist(gen);  // generate number
  for (BlockId i = more_blocks + 2; i <= extra_blocks; i++) {
    add_merkle_keys_to_block(keys_history, i, num_keys);
    // key1 was last updated on num_blocks + 1
    opt_val = keys_history.getVersionFromHistory(merkle_category, key1, i);
    ASSERT_NE(opt_val, std::nullopt);
    ASSERT_EQ(opt_val.value(), more_blocks + 1);
  }

  // make sure also after we are done adding blocks
  const BlockId total_blocks_number = num_blocks + additional_blocks + more_blocks + extra_blocks;
  for (BlockId i = 1; i <= total_blocks_number; i++) {
    opt_val = keys_history.getVersionFromHistory(merkle_category, key1, i);
    if (i <= num_blocks) {
      ASSERT_EQ(opt_val, std::nullopt);
    } else if (i <= additional_blocks) {
      ASSERT_NE(opt_val, std::nullopt);
      ASSERT_EQ(opt_val.value(), num_blocks + 1);
    } else if (i <= more_blocks) {
      ASSERT_NE(opt_val, std::nullopt);
      ASSERT_EQ(opt_val.value(), additional_blocks + 1);
    } else if (i <= extra_blocks) {
      ASSERT_NE(opt_val, std::nullopt);
      ASSERT_EQ(opt_val.value(), more_blocks + 1);
    }
  }
}

}  // end namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
