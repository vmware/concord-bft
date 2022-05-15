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
#include "gmock/gmock.h"
#include "v4blockchain/v4_blockchain.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <set>
#include <random>
#include <functional>
#include <limits>
#include "thread_pool.hpp"
#include "storage/test/storage_test_common.h"
#include "endianness.hpp"
#include "v4blockchain/detail/column_families.h"
#include "categorization/db_categories.h"
#include "block_metadata.hpp"
#include "kvbc_key_types.hpp"

using concord::storage::rocksdb::NativeClient;
using namespace concord::kvbc;
using namespace ::testing;

namespace {

class v4_kvbc : public Test {
 protected:
  void SetUp() override {
    destroyDb();
    db = TestRocksDb::createNative();
  }

  void TearDown() override { destroyDb(); }

  void destroyDb() {
    db.reset();
    ASSERT_EQ(0, db.use_count());
    cleanup();
  }
  void create_blocks(uint64_t num_blocks,
                     uint32_t& num_merkle_each,
                     uint32_t& num_versioned_each,
                     uint32_t& num_immutable_each) {
    std::map<std::string, categorization::CATEGORY_TYPE> cat_map{
        {"merkle", categorization::CATEGORY_TYPE::block_merkle},
        {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
        {"immutable", categorization::CATEGORY_TYPE::immutable}};
    v4blockchain::KeyValueBlockchain blockchain{db, true, cat_map};
    std::mt19937 rgen;
    std::uniform_int_distribution<uint32_t> dist(10, 100);
    num_merkle_each = dist(rgen);
    num_versioned_each = dist(rgen);
    num_immutable_each = dist(rgen);
    // Keys are:
    // <category_name>_key_<blockId>_<key_id>
    // Values are:
    // <category_name>_value_<blockId>_<key_id>
    for (uint64_t blk = 1; blk <= num_blocks; ++blk) {
      categorization::Updates updates;

      categorization::BlockMerkleUpdates merkle_updates;
      for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
        std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
        std::string val = "merkle_value_" + std::to_string(blk) + "_" + std::to_string(kid);
        merkle_updates.addUpdate(std::move(key), std::move(val));
      }
      updates.add("merkle", std::move(merkle_updates));

      categorization::VersionedUpdates ver_updates;
      ver_updates.calculateRootHash(false);
      for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
        std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
        std::string val = "versioned_value_" + std::to_string(blk) + "_" + std::to_string(kid);
        ver_updates.addUpdate(std::move(key), std::move(val));
      }
      updates.add("versioned", std::move(ver_updates));

      categorization::ImmutableUpdates immutable_updates;
      for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
        std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
        std::string val = "immutable_value_" + std::to_string(blk) + "_" + std::to_string(kid);
        immutable_updates.addUpdate(std::move(key), {std::move(val), {std::to_string(blk), std::to_string(kid)}});
      }
      updates.add("immutable", std::move(immutable_updates));
      ASSERT_EQ(blockchain.add(std::move(updates)), (BlockId)blk);
    }
  }

  void create_next_block_in_parallel(uint64_t num_new_block,
                                     uint32_t num_merkle_each,
                                     uint32_t num_versioned_each,
                                     uint32_t num_immutable_each,
                                     v4blockchain::KeyValueBlockchain& blockchain,
                                     std::vector<std::future<void>>& tasks) {
    auto last_block = blockchain.getLastReachableBlockId();
    concord::util::ThreadPool tp;

    // Keys are:
    // <category_name>_key_<blockId>_<key_id>
    // Values are:
    // <category_name>_value_<blockId>_<key_id>

    tasks.push_back(
        tp.async([last_block, num_new_block, num_merkle_each, num_versioned_each, num_immutable_each, &blockchain]() {
          for (uint64_t blk = last_block + 1; blk <= num_new_block; ++blk) {
            categorization::Updates updates;

            categorization::BlockMerkleUpdates merkle_updates;
            for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
              std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
              std::string val = "merkle_value_" + std::to_string(blk) + "_" + std::to_string(kid);
              merkle_updates.addUpdate(std::move(key), std::move(val));
            }
            updates.add("merkle", std::move(merkle_updates));

            categorization::VersionedUpdates ver_updates;
            ver_updates.calculateRootHash(false);
            for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
              std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
              std::string val = "versioned_value_" + std::to_string(blk) + "_" + std::to_string(kid);
              ver_updates.addUpdate(std::move(key), std::move(val));
            }
            updates.add("versioned", std::move(ver_updates));

            categorization::ImmutableUpdates immutable_updates;
            for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
              std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
              std::string val = "immutable_value_" + std::to_string(blk) + "_" + std::to_string(kid);
              immutable_updates.addUpdate(std::move(key), {std::move(val), {std::to_string(blk), std::to_string(kid)}});
            }
            updates.add("immutable", std::move(immutable_updates));
            ASSERT_EQ(blockchain.add(std::move(updates)), (BlockId)blk);
          }
        }));
  }

  void wait_for_block_adds(const std::vector<std::future<void>>& tasks) {
    for (const auto& task : tasks) {
      task.wait();
    }
  }

  void add_deletes_to_blocks(
      uint64_t& tot_num_blocks,
      uint32_t num_merkle_each,
      uint32_t num_versioned_each,
      uint32_t num_immutable_each,
      std::function<bool(const std::string&, uint64_t, uint32_t, uint32_t)> const& key_deletion_filter) {
    std::map<std::string, categorization::CATEGORY_TYPE> cat_map{
        {"merkle", categorization::CATEGORY_TYPE::block_merkle},
        {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
        {"immutable", categorization::CATEGORY_TYPE::immutable}};
    v4blockchain::KeyValueBlockchain blockchain{db, true, cat_map};
    // Keys are:
    // <category_name>_key_<blockId>_<key_id>
    // Values are:
    // <category_name>_value_<blockId>_<key_id>
    uint64_t new_tot_num_blocks = tot_num_blocks;
    for (uint64_t blk = 1; blk <= tot_num_blocks; ++blk) {
      categorization::Updates updates;

      categorization::BlockMerkleUpdates merkle_updates;
      bool is_delete_added = false;
      for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
        std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
        if (key_deletion_filter(key, blk, kid, num_merkle_each)) {
          merkle_updates.addDelete(std::move(key));
          is_delete_added = true;
        }
      }
      if (is_delete_added) {
        updates.add("merkle", std::move(merkle_updates));
      }
      is_delete_added = false;

      categorization::VersionedUpdates ver_updates;
      ver_updates.calculateRootHash(false);
      for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
        std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
        if (key_deletion_filter(key, blk, kid, num_versioned_each)) {
          ver_updates.addDelete(std::move(key));
          is_delete_added = true;
        }
      }
      if (is_delete_added) {
        updates.add("versioned", std::move(ver_updates));
      }
      if (!updates.empty()) {
        ASSERT_EQ(blockchain.add(std::move(updates)), (BlockId)(new_tot_num_blocks + 1));
        new_tot_num_blocks++;
      }
    }
    tot_num_blocks = new_tot_num_blocks;
  }

 protected:
  std::shared_ptr<NativeClient> db;
};

TEST_F(v4_kvbc, simulation) {
  GTEST_SKIP() << "Skipping simulation as its added only for critical debugging purpose";
  uint64_t max_block = 20;
  uint64_t num_blocks_to_add_inparallel = 50000;
  uint32_t num_merkle_each = 0;
  uint32_t num_versioned_each = 0;
  uint32_t num_immutable_each = 0;
  create_blocks(max_block, num_merkle_each, num_versioned_each, num_immutable_each);

  std::map<std::string, categorization::CATEGORY_TYPE> cat_map{
      {"merkle", categorization::CATEGORY_TYPE::block_merkle},
      {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
      {"immutable", categorization::CATEGORY_TYPE::immutable}};
  v4blockchain::KeyValueBlockchain blockchain{db, true, cat_map};
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), max_block);
  std::vector<std::future<void>> tasks;
  create_next_block_in_parallel(
      num_blocks_to_add_inparallel, num_merkle_each, num_versioned_each, num_immutable_each, blockchain, tasks);
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_GE(blockchain.getLastReachableBlockId(), max_block);

  ///////Checking the get///////////////////////
  for (int times = 0; times < 50000; ++times) {
    for (uint64_t blk = 1; blk <= max_block; ++blk) {
      for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
        std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
        std::string val_str = "merkle_value_" + std::to_string(blk) + "_" + std::to_string(kid);
        auto val = blockchain.getLatest("merkle", key);
        ASSERT_TRUE(val.has_value());
        auto merkle_val = std::get<categorization::MerkleValue>(*val);
        ASSERT_EQ(merkle_val.block_id, blk);
        ASSERT_EQ(merkle_val.data, val_str);
      }

      for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
        std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
        std::string val_str = "versioned_value_" + std::to_string(blk) + "_" + std::to_string(kid);
        auto val = blockchain.getLatest("versioned", key);
        ASSERT_TRUE(val.has_value());
        auto ver_val = std::get<categorization::VersionedValue>(*val);
        ASSERT_EQ(ver_val.block_id, blk);
        ASSERT_EQ(ver_val.data, val_str);
      }
      for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
        std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
        std::string val_str = "immutable_value_" + std::to_string(blk) + "_" + std::to_string(kid);
        auto val = blockchain.getLatest("immutable", key);
        ASSERT_TRUE(val.has_value());
        auto immutable_val = std::get<categorization::ImmutableValue>(*val);
        ASSERT_EQ(immutable_val.block_id, blk);
        ASSERT_EQ(immutable_val.data, val_str);
      }
    }
  }
  wait_for_block_adds(tasks);
}

// Add a block which contains updates per category.
// Each category handles its updates and returs an output which goes to the block structure.
// The block structure is then inserted into the DB.
// we test that the block that is written to DB contains the expected data.
TEST_F(v4_kvbc, creation) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{{"merkle", categorization::CATEGORY_TYPE::block_merkle},
                                                           {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
                                                           {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
                                                           {"immutable", categorization::CATEGORY_TYPE::immutable}}};

  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::BLOCKS_CF));
  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::ST_CHAIN_CF));
  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::LATEST_KEYS_CF));
}

TEST_F(v4_kvbc, add_blocks) {
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }
  {
    v4blockchain::KeyValueBlockchain blockchain{
        db,
        true,
        std::map<std::string, categorization::CATEGORY_TYPE>{
            {"merkle", categorization::CATEGORY_TYPE::block_merkle},
            {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
            {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
            {"immutable", categorization::CATEGORY_TYPE::immutable},
            {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};
    // Add block1
    {
      categorization::Updates updates;
      categorization::BlockMerkleUpdates merkle_updates;
      merkle_updates.addUpdate("merkle_key1", "merkle_value1");
      merkle_updates.addUpdate("merkle_key2", "merkle_value2");
      updates.add("merkle", std::move(merkle_updates));

      categorization::VersionedUpdates ver_updates;
      ver_updates.calculateRootHash(true);
      ver_updates.addUpdate("ver_key1", "ver_val1");
      ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
      updates.add("versioned", std::move(ver_updates));
      ASSERT_EQ(blockchain.add(std::move(updates)), (BlockId)1);
    }
    // Add block2
    {
      categorization::Updates updates;
      categorization::BlockMerkleUpdates merkle_updates;
      merkle_updates.addUpdate("merkle_key3", "merkle_value3");
      merkle_updates.addDelete("merkle_key1");
      updates.add("merkle", std::move(merkle_updates));

      categorization::VersionedUpdates ver_updates;
      ver_updates.calculateRootHash(false);
      ver_updates.addUpdate("ver_key3", "ver_val3");
      ver_updates.addDelete("ver_key2");
      updates.add("versioned", std::move(ver_updates));

      categorization::VersionedUpdates ver_updates_2;
      ver_updates_2.calculateRootHash(false);
      ver_updates_2.addUpdate("ver_key4", "ver_val4");
      updates.add("versioned_2", std::move(ver_updates_2));

      categorization::ImmutableUpdates immutable_updates;
      immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
      updates.add("immutable", std::move(immutable_updates));
      ASSERT_EQ(blockchain.add(std::move(updates)), (BlockId)2);
    }
  }

  {
    v4blockchain::KeyValueBlockchain blockchain{
        db,
        true,
        std::map<std::string, categorization::CATEGORY_TYPE>{
            {"merkle", categorization::CATEGORY_TYPE::block_merkle},
            {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
            {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
            {"immutable", categorization::CATEGORY_TYPE::immutable},
            {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};

    // Add block3
    {
      categorization::Updates updates;
      categorization::BlockMerkleUpdates merkle_updates;
      merkle_updates.addUpdate("merkle_key4", "merkle_value4");
      merkle_updates.addDelete("merkle_key4");
      updates.add("merkle", std::move(merkle_updates));

      categorization::VersionedUpdates ver_updates;
      ver_updates.calculateRootHash(false);
      ver_updates.addUpdate("ver_key4", "ver_val4");
      ver_updates.addDelete("ver_key4");
      updates.add("versioned", std::move(ver_updates));

      categorization::VersionedUpdates ver_updates_2;
      ver_updates_2.calculateRootHash(false);
      ver_updates_2.addUpdate("ver_key5", "ver_val5");
      updates.add("versioned_2", std::move(ver_updates_2));

      categorization::ImmutableUpdates immutable_updates;
      immutable_updates.addUpdate("immutable_key20", {"immutable_val20", {"1", "2"}});
      updates.add("immutable", std::move(immutable_updates));
      ASSERT_EQ(blockchain.add(std::move(updates)), (BlockId)3);
    }

    // get block 1 from DB and test it
    {
      auto block1_db_val = blockchain.getBlockchain().getBlockData(1);
      ASSERT_TRUE(block1_db_val.has_value());

      auto block1 = v4blockchain::detail::Block(*block1_db_val);
      ASSERT_EQ(block1.parentDigest(), empty_digest);
      auto updates_info = block1.getUpdates().categoryUpdates();
      // Get the merkle updates output of block1
      auto merkle_variant = updates_info.kv["merkle"];
      auto merkle_update = std::get<categorization::BlockMerkleInput>(merkle_variant);
      ASSERT_EQ(merkle_update.kv.size(), 2);
      ASSERT_EQ(merkle_update.kv["merkle_key1"], "merkle_value1");
      ASSERT_EQ(merkle_update.kv["merkle_key2"], "merkle_value2");
      ASSERT_EQ(merkle_update.deletes.size(), 0);

      auto ver_variant = updates_info.kv["versioned"];
      auto ver_out1 = std::get<categorization::VersionedInput>(ver_variant);
      ASSERT_EQ(ver_out1.kv.size(), 2);
      ASSERT_EQ(ver_out1.kv["ver_key1"].data, "ver_val1");
      ASSERT_EQ(ver_out1.kv["ver_key1"].stale_on_update, false);
      ASSERT_EQ(ver_out1.kv["ver_key2"].data, "ver_val2");
      ASSERT_EQ(ver_out1.kv["ver_key2"].stale_on_update, true);
    }
  }
}

TEST_F(v4_kvbc, add_and_read_blocks) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};
  std::string no_flags = {0};
  std::string stale_on_update_flag = {1};
  // Add block1 and read
  auto imm_val1 = categorization::ImmutableValueUpdate{"immutable_val20", {"1", "2"}};
  auto ver_val = categorization::ValueWithFlags{"ver_val", true};
  {
    std::string out_ts;
    uint64_t block_version = 1;
    auto block_version_str = v4blockchain::detail::Blockchain::generateKey(block_version);
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    updates.add("merkle", std::move(merkle_updates));

    categorization::ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key20", {"immutable_val20", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(true);
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val", true});
    updates.add("versioned", std::move(ver_updates));

    // Save serialization of the updates
    std::vector<uint8_t> updates_buffer;
    concord::kvbc::categorization::serialize(updates_buffer, updates.categoryUpdates());

    ASSERT_EQ(blockchain.add(std::move(updates)), (BlockId)1);
    auto opt_block = blockchain.getBlockchain().getBlockData(1);
    ASSERT_TRUE(opt_block.has_value());

    std::vector<uint8_t> updates_from_storage{(*opt_block).begin() + v4blockchain::detail::Block::HEADER_SIZE,
                                              (*opt_block).end()};

    ASSERT_EQ(updates_buffer, updates_from_storage);

    // Get keys from latest
    // Merkle
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       blockchain.getLatestKeys().getCategoryPrefix("merkle") + "merkle_key1",
                       block_version_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(*val, "merkle_value1" + no_flags);
    ASSERT_EQ(out_ts, block_version_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(1, iout_ts);

    // Immutable
    // without category prefix
    out_ts.clear();
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF, std::string("immutable_key20"), block_version_str, &out_ts);
    ASSERT_FALSE(val.has_value());

    // get key1 updated value of this timestamp
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("immutable") + "immutable_key20",
                  block_version_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());

    ASSERT_EQ(*val, imm_val1.data + stale_on_update_flag);
    ASSERT_EQ(out_ts, block_version_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(1, iout_ts);
    out_ts.clear();

    // Versioned
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("versioned") + "ver_key2",
                  block_version_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());

    ASSERT_EQ(*val, ver_val.data + std::string(1, v4blockchain::detail::LatestKeys::STALE_ON_UPDATE[0]));
    ASSERT_EQ(out_ts, block_version_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(1, iout_ts);
    out_ts.clear();
  }
}

TEST_F(v4_kvbc, trim_history_get_block_sequence_number) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};
  {
    categorization::Updates updates;
    ASSERT_EQ(blockchain.getBlockSequenceNumber(updates), 0);
  }
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val", true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));
    ASSERT_EQ(blockchain.getBlockSequenceNumber(updates), 0);
  }

  {
    uint64_t id = 10;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, concord::kvbc::IBlockMetadata::kBlockMetadataKey),
                          categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));
    ASSERT_EQ(blockchain.getBlockSequenceNumber(updates), id);
  }
}

TEST_F(v4_kvbc, check_if_trim_history_is_needed) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};
  {
    categorization::Updates updates;
    ASSERT_EQ(blockchain.markHistoryForGarbageCollectionIfNeeded(updates), 0);
    ASSERT_EQ(blockchain.gc_counter, 0);
  }
  {
    // No meta data sn key
    uint64_t id = 10;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, 0x1), categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));
    ASSERT_EQ(blockchain.markHistoryForGarbageCollectionIfNeeded(updates), 0);
    ASSERT_EQ(blockchain.gc_counter, 0);
  }

  // First real set, sn should be 10 after this
  {
    uint64_t id = 10;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, concord::kvbc::IBlockMetadata::kBlockMetadataKey),
                          categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));

    ASSERT_EQ(blockchain.markHistoryForGarbageCollectionIfNeeded(updates), id);
    blockchain.setLastBlockSequenceNumber(id);
    ASSERT_EQ(blockchain.gc_counter, 0);
  }

  // try now with lower sn
  {
    uint64_t id = 9;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, concord::kvbc::IBlockMetadata::kBlockMetadataKey),
                          categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));

    ASSERT_EQ(blockchain.markHistoryForGarbageCollectionIfNeeded(updates), 0);
    ASSERT_EQ(blockchain.gc_counter, 0);
  }

  // sn was incremented
  {
    uint64_t id = 11;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, concord::kvbc::IBlockMetadata::kBlockMetadataKey),
                          categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));

    ASSERT_EQ(blockchain.markHistoryForGarbageCollectionIfNeeded(updates), id);
    blockchain.setLastBlockSequenceNumber(id);
    ASSERT_EQ(blockchain.gc_counter, 1);
  }
  blockchain.checkpointInProcess(true);

  // sn was incremented but checkpoint in process
  {
    uint64_t id = 15;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, concord::kvbc::IBlockMetadata::kBlockMetadataKey),
                          categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));

    ASSERT_EQ(blockchain.markHistoryForGarbageCollectionIfNeeded(updates), 0);
    ASSERT_EQ(blockchain.gc_counter, 1);
  }

  blockchain.checkpointInProcess(false);

  // sn was incremented and no checkpoint in process
  {
    uint64_t id = 16;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, concord::kvbc::IBlockMetadata::kBlockMetadataKey),
                          categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));

    ASSERT_EQ(blockchain.markHistoryForGarbageCollectionIfNeeded(updates), id);
    blockchain.setLastBlockSequenceNumber(id);
    ASSERT_EQ(blockchain.gc_counter, 2);
  }
}

TEST_F(v4_kvbc, delete_last_reachable) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};
  // Can't delete from empty blockchain
  ASSERT_DEATH(blockchain.deleteLastReachableBlock(), "");
  auto key1 = std::string("merkle_key1");
  auto key2 = std::string("merkle_key2");
  auto val1 = std::string("merkle_value1");
  auto val2 = std::string("merkle_value2");
  auto val_updated = std::string("merkle_value_updated");

  auto ver_key1 = std::string("ver_key1");
  auto ver_key2 = std::string("ver_key2");
  auto ver_val1 = std::string("ver_val1");
  auto ver_val2 = std::string("ver_val2");
  auto ver_val_updated = std::string("ver_value_updated");

  // Add block 1 with 2 keys
  {
    categorization::Updates updates;
    // Merkle
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate(std::string("merkle_key1"), std::string("merkle_value1"));
    merkle_updates.addUpdate(std::string("merkle_key2"), std::string("merkle_value2"));
    updates.add("merkle", std::move(merkle_updates));
    // Versioned
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));

    auto id = blockchain.add(std::move(updates));
    ASSERT_EQ(id, 1);

    std::string out_ts;
    auto block_id1_str = v4blockchain::detail::Blockchain::generateKey(id);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       blockchain.getLatestKeys().getCategoryPrefix("merkle") + key1,
                       block_id1_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    std::string val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
    ASSERT_EQ(val_db, val1);

    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(id, iout_ts);
    out_ts.clear();

    // check that the value of the versioned category i.e. ValueWithFlags was read from DB succuessfully.
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("versioned") + ver_key1,
                  block_id1_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_FALSE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));
    {
      std::string ver_val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
      concord::kvbc::categorization::ValueWithFlags db_val_wf = concord::kvbc::categorization::ValueWithFlags{
          ver_val_db, v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val)};
      auto val_wf = concord::kvbc::categorization::ValueWithFlags{"ver_val1", false};
      ASSERT_EQ(val_wf, db_val_wf);
      ASSERT_EQ(out_ts, block_id1_str);
      iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
      ASSERT_EQ(id, iout_ts);
      out_ts.clear();
    }

    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("versioned") + ver_key2,
                  block_id1_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_TRUE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));

    std::string ver_val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
    concord::kvbc::categorization::ValueWithFlags db_val_wf = concord::kvbc::categorization::ValueWithFlags{
        ver_val_db, v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val)};
    auto val_wf = concord::kvbc::categorization::ValueWithFlags{"ver_val2", true};
    ASSERT_EQ(val_wf, db_val_wf);
    ASSERT_EQ(out_ts, block_id1_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(id, iout_ts);
    out_ts.clear();
  }
  // Can't delete single block
  ASSERT_DEATH(blockchain.deleteLastReachableBlock(), "");

  // Add block 2 where key1 is updated and key2 is deleted.
  {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate(std::string("merkle_key1"), std::string("merkle_value_updated"));
    merkle_updates.addDelete(std::string("merkle_key2"));
    updates.add("merkle", std::move(merkle_updates));

    // Versioned
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key1", "ver_value_updated");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    auto id = blockchain.add(std::move(updates));
    ASSERT_EQ(id, 2);

    // Validate Merkle
    std::string out_ts;
    auto block_id2_str = v4blockchain::detail::Blockchain::generateKey(id);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       blockchain.getLatestKeys().getCategoryPrefix("merkle") + key1,
                       block_id2_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    std::string val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
    ASSERT_EQ(val_db, val_updated);
    ASSERT_EQ(out_ts, block_id2_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(id, iout_ts);
    out_ts.clear();
    // Key2 was deleted
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("merkle") + key2,
                  block_id2_str,
                  &out_ts);
    ASSERT_FALSE(val.has_value());

    // Validate versioned
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("versioned") + ver_key1,
                  block_id2_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_FALSE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));
    std::string ver_val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
    concord::kvbc::categorization::ValueWithFlags db_val_wf = concord::kvbc::categorization::ValueWithFlags{
        ver_val_db, v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val)};
    auto val_wf = concord::kvbc::categorization::ValueWithFlags{"ver_value_updated", false};

    ASSERT_EQ(val_wf, db_val_wf);
    ASSERT_EQ(out_ts, block_id2_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(id, iout_ts);
    out_ts.clear();
    // Key2 was deleted
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("versioned") + ver_key2,
                  block_id2_str,
                  &out_ts);
    ASSERT_FALSE(val.has_value());
  }

  // Delete block 2

  ASSERT_EQ(blockchain.getBlockchain().getLastReachable(), 2);
  blockchain.deleteLastReachableBlock();
  ASSERT_EQ(blockchain.getBlockchain().getLastReachable(), 1);
  // After deletion, lets try to read the keys using version 2
  // key1 - should get the value from block 1.
  // key2 - should be recovered from deletion and get the value from block 1.
  {
    auto id = 2;
    std::string out_ts;
    auto block_id2_str = v4blockchain::detail::Blockchain::generateKey(id);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       blockchain.getLatestKeys().getCategoryPrefix("merkle") + key1,
                       block_id2_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    std::string val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
    // we reverted to old value
    ASSERT_EQ(val_db, val1);

    ASSERT_EQ(out_ts, block_id2_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(id, iout_ts);

    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("merkle") + key2,
                  block_id2_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    std::string val_db_2((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
    ASSERT_EQ(val_db_2, val2);

    // Validate versioned category
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("versioned") + ver_key1,
                  block_id2_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_FALSE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));
    {
      std::string ver_val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
      concord::kvbc::categorization::ValueWithFlags db_val_wf = concord::kvbc::categorization::ValueWithFlags{
          ver_val_db, v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val)};
      auto val_wf = concord::kvbc::categorization::ValueWithFlags{"ver_val1", false};
      ASSERT_EQ(val_wf, db_val_wf);
      ASSERT_EQ(out_ts, block_id2_str);
      iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
      ASSERT_EQ(id, iout_ts);
      out_ts.clear();
    }

    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("versioned") + ver_key2,
                  block_id2_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_TRUE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));

    std::string ver_val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
    concord::kvbc::categorization::ValueWithFlags db_val_wf = concord::kvbc::categorization::ValueWithFlags{
        ver_val_db, v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val)};
    auto val_wf = concord::kvbc::categorization::ValueWithFlags{"ver_val2", true};
    ASSERT_EQ(val_wf, db_val_wf);
    ASSERT_EQ(out_ts, block_id2_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(id, iout_ts);
    out_ts.clear();
  }

  // Let's add block 2 again and validate the key1 and key2 are updated correctly
  // Add block 2 where key1 is updated and key2 is deleted.
  {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate(std::string("merkle_key1"), std::string("merkle_value_updated"));
    merkle_updates.addDelete(std::string("merkle_key2"));
    updates.add("merkle", std::move(merkle_updates));

    // Versioned
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key1", "ver_value_updated");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    auto id = blockchain.add(std::move(updates));
    ASSERT_EQ(id, 2);

    // Validate Merkle
    std::string out_ts;
    auto block_id2_str = v4blockchain::detail::Blockchain::generateKey(id);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       blockchain.getLatestKeys().getCategoryPrefix("merkle") + key1,
                       block_id2_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    std::string val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
    ASSERT_EQ(val_db, val_updated);
    ASSERT_EQ(out_ts, block_id2_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(id, iout_ts);
    out_ts.clear();
    // Key2 was deleted
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("merkle") + key2,
                  block_id2_str,
                  &out_ts);
    ASSERT_FALSE(val.has_value());

    // Validate versioned
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("versioned") + ver_key1,
                  block_id2_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_FALSE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));
    std::string ver_val_db((*val).begin(), (*val).end() - v4blockchain::detail::LatestKeys::FLAGS_SIZE);
    concord::kvbc::categorization::ValueWithFlags db_val_wf = concord::kvbc::categorization::ValueWithFlags{
        ver_val_db, v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val)};
    auto val_wf = concord::kvbc::categorization::ValueWithFlags{"ver_value_updated", false};

    ASSERT_EQ(val_wf, db_val_wf);
    ASSERT_EQ(out_ts, block_id2_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(id, iout_ts);
    out_ts.clear();
    // Key2 was deleted
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  blockchain.getLatestKeys().getCategoryPrefix("versioned") + ver_key2,
                  block_id2_str,
                  &out_ts);
    ASSERT_FALSE(val.has_value());
  }
  ASSERT_EQ(blockchain.getBlockchain().getLastReachable(), 2);
}

/////////////////STATE TRANSFER//////////////////////
TEST_F(v4_kvbc, has_blocks) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};

  // Add block1
  {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    updates.add("merkle", std::move(merkle_updates));

    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(true);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));
    ASSERT_EQ(blockchain.add(std::move(updates)), (BlockId)1);
  }

  ASSERT_TRUE(blockchain.hasBlock(1));
  ASSERT_TRUE(blockchain.getBlockData(1).has_value());
  ASSERT_FALSE(blockchain.hasBlock(2));
  ASSERT_FALSE(blockchain.getBlockData(2).has_value());

  // add block the the ST chain
  auto block = std::string("block");
  blockchain.addBlockToSTChain(2, block.c_str(), block.size(), false);
  ASSERT_TRUE(blockchain.hasBlock(2));
  auto st_block = blockchain.getBlockData(2);
  ASSERT_TRUE(st_block.has_value());
  ASSERT_EQ(*st_block, block);
}

TEST_F(v4_kvbc, add_blocks_to_st_chain) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};

  std::string block_data;
  // Add block1
  {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    updates.add("merkle", std::move(merkle_updates));

    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(true);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));
    ASSERT_EQ(blockchain.add(std::move(updates)), (BlockId)1);
  }

  // Add st block with id 3 i.e. block 2 is missing.
  block_data = *(blockchain.getBlockData(1));
  blockchain.addBlockToSTChain(3, block_data.c_str(), block_data.size(), false);
  blockchain.addBlockToSTChain(5, block_data.c_str(), block_data.size(), false);
  ASSERT_FALSE(blockchain.hasBlock(2));
  ASSERT_TRUE(blockchain.hasBlock(3));
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 1);
  blockchain.linkSTChain();
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 1);
  // now add block 2 and link will add until block 3
  blockchain.addBlockToSTChain(2, block_data.c_str(), block_data.size(), false);
  blockchain.linkSTChain();
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 3);
  auto block3 = blockchain.getBlockchain().getBlockData(3);
  ASSERT_TRUE(block3.has_value());
}

TEST_F(v4_kvbc, add_altot_of_blocks_to_st_chain) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};

  std::string block_data;
  // Add blocks
  for (BlockId i = 1; i < 100; ++i) {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    updates.add("merkle", std::move(merkle_updates));

    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(true);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));
    ASSERT_EQ(blockchain.add(std::move(updates)), i);
  }
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 99);

  // Add st block with id 3 i.e. block 2 is missing.
  block_data = *(blockchain.getBlockData(1));

  // try to add with block id less than the last reachable
  ASSERT_THROW(blockchain.addBlockToSTChain(3, block_data.c_str(), block_data.size(), false), std::invalid_argument);
  for (BlockId i = 100; i < 1000; ++i) {
    blockchain.addBlockToSTChain(i, block_data.c_str(), block_data.size(), false);
  }

  ASSERT_TRUE(blockchain.hasBlock(999));
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 99);
  ASSERT_EQ(blockchain.getStChain().getLastBlockId(), 999);
  blockchain.addBlockToSTChain(1000, block_data.c_str(), block_data.size(), true);
  ASSERT_EQ(blockchain.getStChain().getLastBlockId(), 0);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 1000);
}

TEST_F(v4_kvbc, st_link_until) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};

  std::string block_data;
  // Add blocks
  for (BlockId i = 1; i < 10; ++i) {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    updates.add("merkle", std::move(merkle_updates));

    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(true);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));
    ASSERT_EQ(blockchain.add(std::move(updates)), i);
  }
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 9);

  // Add st block with id 3 i.e. block 2 is missing.
  block_data = *(blockchain.getBlockData(1));

  for (BlockId i = 10; i < 20; ++i) {
    blockchain.addBlockToSTChain(i, block_data.c_str(), block_data.size(), false);
  }

  ASSERT_FALSE(blockchain.hasBlock(999));
  ASSERT_TRUE(blockchain.hasBlock(15));
  ASSERT_EQ(blockchain.getStChain().getLastBlockId(), 19);

  // we can add until block 19
  ASSERT_EQ(blockchain.linkUntilBlockId(30), 10);
  // Chain should be reset
  ASSERT_EQ(blockchain.getStChain().getLastBlockId(), 0);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 19);

  for (BlockId i = 20; i < 200; ++i) {
    blockchain.addBlockToSTChain(i, block_data.c_str(), block_data.size(), false);
  }
  ASSERT_TRUE(blockchain.hasBlock(199));
  ASSERT_EQ(blockchain.getStChain().getLastBlockId(), 199);
  // Add partial range
  ASSERT_EQ(blockchain.linkUntilBlockId(30), 11);
  ASSERT_EQ(blockchain.linkUntilBlockId(100), 70);
  ASSERT_EQ(blockchain.linkUntilBlockId(199), 99);
  // St chain should be empty
  ASSERT_EQ(blockchain.getStChain().getLastBlockId(), 0);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 199);
}

TEST_F(v4_kvbc, parent_digest) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};

  std::string block_data;
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }
  // block1
  {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    updates.add("merkle", std::move(merkle_updates));
    ASSERT_EQ(blockchain.add(std::move(updates)), 1);
    ASSERT_EQ(blockchain.parentDigest(0), empty_digest);
    // also geneis has empty parent digest
    ASSERT_EQ(blockchain.parentDigest(1), empty_digest);
  }

  // block2
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(true);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));
    ASSERT_EQ(blockchain.add(std::move(updates)), 2);
    auto block_data = blockchain.getBlockData(1);
    ASSERT_TRUE(block_data.has_value());
    auto digest = v4blockchain::detail::Block::calculateDigest(1, block_data->c_str(), block_data->size());
    ASSERT_EQ(blockchain.parentDigest(2), digest);

    auto block_data2 = blockchain.getBlockData(2);
    blockchain.addBlockToSTChain(8, block_data2->c_str(), block_data2->size(), false);
    ASSERT_DEATH(blockchain.parentDigest(3), "");
    ASSERT_EQ(blockchain.parentDigest(8), digest);
  }
}

TEST_F(v4_kvbc, prun_on_st) {
  v4blockchain::KeyValueBlockchain blockchain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};

  // Add blocks
  BlockId until = 100;
  for (BlockId i = 1; i <= until; ++i) {
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    updates.add("merkle", std::move(merkle_updates));

    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(true);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));
    // Add genesis for pruning
    if (i % 20 == 0) {
      const auto stale_on_update = true;
      updates.addCategoryIfNotExisting<categorization::VersionedInput>(categorization::kConcordInternalCategoryId);
      updates.appendKeyValue<categorization::VersionedUpdates>(
          categorization::kConcordInternalCategoryId,
          std::string{concord::kvbc::keyTypes::genesis_block_key},
          categorization::VersionedUpdates::ValueType{concordUtils::toBigEndianStringBuffer(i - 10), stale_on_update});
    }
    ASSERT_EQ(blockchain.add(std::move(updates)), i);
    auto block_data = *(blockchain.getBlockData(i));
    blockchain.addBlockToSTChain(until + i, block_data.c_str(), block_data.size(), false);
  }
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 100);
  ASSERT_EQ(blockchain.getStChain().getLastBlockId(), 200);

  // range with the an update that contains genesis = 10;
  ASSERT_EQ(blockchain.linkUntilBlockId(130), 30);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 130);
  ASSERT_EQ(blockchain.getGenesisBlockId(), 10);
  ASSERT_EQ(blockchain.linkUntilBlockId(160), 30);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 160);
  ASSERT_EQ(blockchain.getGenesisBlockId(), 50);
  blockchain.linkSTChain();
  ASSERT_EQ(blockchain.getStChain().getLastBlockId(), 0);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), 200);
  ASSERT_EQ(blockchain.getGenesisBlockId(), 90);
}

TEST_F(v4_kvbc, all_gets) {
  uint64_t max_block = 100;
  uint32_t num_merkle_each = 0;
  uint32_t num_versioned_each = 0;
  uint32_t num_immutable_each = 0;
  create_blocks(max_block, num_merkle_each, num_versioned_each, num_immutable_each);

  std::map<std::string, categorization::CATEGORY_TYPE> cat_map{
      {"merkle", categorization::CATEGORY_TYPE::block_merkle},
      {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
      {"immutable", categorization::CATEGORY_TYPE::immutable}};
  v4blockchain::KeyValueBlockchain blockchain{db, true, cat_map};
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), max_block);

  std::vector<std::string> merkle_keys;
  std::vector<BlockId> merkle_versions;
  std::vector<std::string> versioned_keys;
  std::vector<BlockId> versioned_versions;
  std::vector<std::string> immutable_keys;
  std::vector<BlockId> immutable_versions;

  ///////Checking the get///////////////////////
  for (uint64_t blk = 1; blk <= max_block; ++blk) {
    for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
      std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "merkle_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      merkle_keys.push_back(key);
      merkle_versions.push_back(blk);
      auto val = blockchain.get("merkle", key, blk);
      ASSERT_TRUE(val.has_value());
      auto merkle_val = std::get<categorization::MerkleValue>(*val);
      ASSERT_EQ(merkle_val.block_id, blk);
      ASSERT_EQ(merkle_val.data, val_str);
    }

    for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
      std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "versioned_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      versioned_keys.push_back(key);
      versioned_versions.push_back(blk);
      auto val = blockchain.get("versioned", key, blk);
      ASSERT_TRUE(val.has_value());
      auto ver_val = std::get<categorization::VersionedValue>(*val);
      ASSERT_EQ(ver_val.block_id, blk);
      ASSERT_EQ(ver_val.data, val_str);
    }
    for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
      std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "immutable_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      immutable_keys.push_back(key);
      immutable_versions.push_back(blk);
      auto val = blockchain.get("immutable", key, blk);
      ASSERT_TRUE(val.has_value());
      auto immutable_val = std::get<categorization::ImmutableValue>(*val);
      ASSERT_EQ(immutable_val.block_id, blk);
      ASSERT_EQ(immutable_val.data, val_str);
    }
  }

  ///////Checking the multiGet///////////////////////
  std::vector<std::optional<categorization::Value>> values;
  blockchain.multiGet("merkle", merkle_keys, merkle_versions, values);
  size_t key_start = std::string("merkle_key_").size();
  size_t val_start = std::string("merkle_value_").size();
  for (size_t i = 0; i < merkle_keys.size(); ++i) {
    ASSERT_TRUE(values[i].has_value());
    auto merkle_val = std::get<categorization::MerkleValue>(*(values[i]));
    ASSERT_EQ(merkle_keys[i].substr(key_start), (merkle_val.data).substr(val_start));
  }

  blockchain.multiGet("versioned", versioned_keys, versioned_versions, values);
  key_start = std::string("versioned_key_").size();
  val_start = std::string("versioned_value_").size();
  for (size_t i = 0; i < versioned_keys.size(); ++i) {
    ASSERT_TRUE(values[i].has_value());
    auto versioned_val = std::get<categorization::VersionedValue>(*(values[i]));
    ASSERT_EQ(versioned_keys[i].substr(key_start), (versioned_val.data).substr(val_start));
  }

  blockchain.multiGet("immutable", immutable_keys, immutable_versions, values);
  key_start = std::string("immutable_key_").size();
  val_start = std::string("immutable_value_").size();
  for (size_t i = 0; i < immutable_keys.size(); ++i) {
    ASSERT_TRUE(values[i].has_value());
    auto immutable_val = std::get<categorization::ImmutableValue>(*(values[i]));
    ASSERT_EQ(immutable_keys[i].substr(key_start), (immutable_val.data).substr(val_start));
  }

  merkle_keys.clear();
  merkle_versions.clear();
  versioned_keys.clear();
  versioned_versions.clear();
  immutable_keys.clear();
  immutable_versions.clear();

  std::set<std::string> deleted_keys;
  uint64_t new_max_block = max_block;
  add_deletes_to_blocks(
      new_max_block,
      num_merkle_each,
      num_versioned_each,
      num_immutable_each,
      [&deleted_keys](const std::string& key, uint64_t blkid, uint32_t kid, uint32_t kid_max) -> bool {
        std::mt19937 rgen;
        std::uniform_int_distribution<uint32_t> dist(2, std::numeric_limits<uint32_t>::max());
        if (((blkid + kid) * dist(rgen)) % 2 == 0) {
          deleted_keys.insert(key);
          return true;
        }
        return false;
      });
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), new_max_block);
  // Checking again
  for (uint64_t blk = 1; blk <= max_block; ++blk) {
    for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
      std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "merkle_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      merkle_keys.push_back(key);
      merkle_versions.push_back(blk);
      auto val = blockchain.get("merkle", key, blk);
      ASSERT_TRUE(val.has_value());
      auto merkle_val = std::get<categorization::MerkleValue>(*val);
      ASSERT_EQ(merkle_val.block_id, blk);
      ASSERT_EQ(merkle_val.data, val_str);
    }

    for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
      std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "versioned_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      versioned_keys.push_back(key);
      versioned_versions.push_back(blk);
      auto val = blockchain.get("versioned", key, blk);
      ASSERT_TRUE(val.has_value());
      auto ver_val = std::get<categorization::VersionedValue>(*val);
      ASSERT_EQ(ver_val.block_id, blk);
      ASSERT_EQ(ver_val.data, val_str);
    }
    for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
      std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "immutable_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      immutable_keys.push_back(key);
      immutable_versions.push_back(blk);
      auto val = blockchain.get("immutable", key, blk);
      ASSERT_TRUE(val.has_value());
      auto immutable_val = std::get<categorization::ImmutableValue>(*val);
      ASSERT_EQ(immutable_val.block_id, blk);
      ASSERT_EQ(immutable_val.data, val_str);
    }
  }

  ///////Checking the multiGet///////////////////////
  blockchain.multiGet("merkle", merkle_keys, merkle_versions, values);
  key_start = std::string("merkle_key_").size();
  val_start = std::string("merkle_value_").size();
  for (size_t i = 0; i < merkle_keys.size(); ++i) {
    ASSERT_TRUE(values[i].has_value());
    auto merkle_val = std::get<categorization::MerkleValue>(*(values[i]));
    ASSERT_EQ(merkle_keys[i].substr(key_start), (merkle_val.data).substr(val_start));
  }

  blockchain.multiGet("versioned", versioned_keys, versioned_versions, values);
  key_start = std::string("versioned_key_").size();
  val_start = std::string("versioned_value_").size();
  for (size_t i = 0; i < versioned_keys.size(); ++i) {
    ASSERT_TRUE(values[i].has_value());
    auto versioned_val = std::get<categorization::VersionedValue>(*(values[i]));
    ASSERT_EQ(versioned_keys[i].substr(key_start), (versioned_val.data).substr(val_start));
  }

  blockchain.multiGet("immutable", immutable_keys, immutable_versions, values);
  key_start = std::string("immutable_key_").size();
  val_start = std::string("immutable_value_").size();
  for (size_t i = 0; i < immutable_keys.size(); ++i) {
    ASSERT_TRUE(values[i].has_value());
    auto immutable_val = std::get<categorization::ImmutableValue>(*(values[i]));
    ASSERT_EQ(immutable_keys[i].substr(key_start), (immutable_val.data).substr(val_start));
  }

  merkle_keys.clear();
  merkle_versions.clear();
  versioned_keys.clear();
  versioned_versions.clear();
  immutable_keys.clear();
  immutable_versions.clear();
  // Checking again
  for (uint64_t blk = max_block + 1; blk <= new_max_block; ++blk) {
    for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
      std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "merkle_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      merkle_keys.push_back(key);
      merkle_versions.push_back(blk);
      auto val = blockchain.get("merkle", key, blk);
      if (deleted_keys.count(key)) {
        ASSERT_FALSE(val.has_value());
      } else {
        ASSERT_TRUE(val.has_value());
        auto merkle_val = std::get<categorization::MerkleValue>(*val);
        ASSERT_EQ(merkle_val.block_id, blk);
        ASSERT_EQ(merkle_val.data, val_str);
      }
    }

    // This range includes the deletes.
    for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
      std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "versioned_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      versioned_keys.push_back(key);
      versioned_versions.push_back(blk);
      auto val = blockchain.get("versioned", key, blk);
      if (deleted_keys.count(key)) {
        ASSERT_FALSE(val.has_value());
      } else {
        ASSERT_TRUE(val.has_value());
        auto ver_val = std::get<categorization::VersionedValue>(*val);
        ASSERT_EQ(ver_val.block_id, blk);
        ASSERT_EQ(ver_val.data, val_str);
      }
    }
    for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
      std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "immutable_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      immutable_keys.push_back(key);
      immutable_versions.push_back(blk);
      auto val = blockchain.get("immutable", key, blk);
      if (deleted_keys.count(key)) {
        ASSERT_FALSE(val.has_value());
      } else {
        ASSERT_TRUE(val.has_value());
        auto immutable_val = std::get<categorization::ImmutableValue>(*val);
        ASSERT_EQ(immutable_val.block_id, blk);
        ASSERT_EQ(immutable_val.data, val_str);
      }
    }
  }

  ///////Checking the multiGet///////////////////////
  blockchain.multiGet("merkle", merkle_keys, merkle_versions, values);
  key_start = std::string("merkle_key_").size();
  val_start = std::string("merkle_value_").size();
  for (size_t i = 0; i < merkle_keys.size(); ++i) {
    if (deleted_keys.count(merkle_keys[i])) {
      ASSERT_FALSE(values[i].has_value());
    } else {
      ASSERT_TRUE(values[i].has_value());
      auto merkle_val = std::get<categorization::MerkleValue>(*(values[i]));
      ASSERT_EQ(merkle_keys[i].substr(key_start), (merkle_val.data).substr(val_start));
    }
  }

  blockchain.multiGet("versioned", versioned_keys, versioned_versions, values);
  key_start = std::string("versioned_key_").size();
  val_start = std::string("versioned_value_").size();
  for (size_t i = 0; i < versioned_keys.size(); ++i) {
    if (deleted_keys.count(versioned_keys[i])) {
      ASSERT_FALSE(values[i].has_value());
    } else {
      ASSERT_TRUE(values[i].has_value());
      auto versioned_val = std::get<categorization::VersionedValue>(*(values[i]));
      ASSERT_EQ(versioned_keys[i].substr(key_start), (versioned_val.data).substr(val_start));
    }
  }

  blockchain.multiGet("immutable", immutable_keys, immutable_versions, values);
  key_start = std::string("immutable_key_").size();
  val_start = std::string("immutable_value_").size();
  for (size_t i = 0; i < immutable_keys.size(); ++i) {
    if (deleted_keys.count(immutable_keys[i])) {
      ASSERT_FALSE(values[i].has_value());
    } else {
      ASSERT_TRUE(values[i].has_value());
      auto immutable_val = std::get<categorization::ImmutableValue>(*(values[i]));
      ASSERT_EQ(immutable_keys[i].substr(key_start), (immutable_val.data).substr(val_start));
    }
  }
}

TEST_F(v4_kvbc, all_get_latest) {
  uint64_t max_block = 100;
  uint32_t num_merkle_each = 0;
  uint32_t num_versioned_each = 0;
  uint32_t num_immutable_each = 0;
  create_blocks(max_block, num_merkle_each, num_versioned_each, num_immutable_each);

  std::map<std::string, categorization::CATEGORY_TYPE> cat_map{
      {"merkle", categorization::CATEGORY_TYPE::block_merkle},
      {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
      {"immutable", categorization::CATEGORY_TYPE::immutable}};
  v4blockchain::KeyValueBlockchain blockchain{db, true, cat_map};
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), max_block);

  std::vector<std::string> merkle_keys;
  std::vector<std::string> versioned_keys;
  std::vector<std::string> immutable_keys;

  ///////Checking the get///////////////////////
  for (uint64_t blk = 1; blk <= max_block; ++blk) {
    for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
      std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "merkle_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      merkle_keys.push_back(key);
      auto val = blockchain.getLatest("merkle", key);
      ASSERT_TRUE(val.has_value());
      auto merkle_val = std::get<categorization::MerkleValue>(*val);
      ASSERT_EQ(merkle_val.block_id, blk);
      ASSERT_EQ(merkle_val.data, val_str);
    }

    for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
      std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "versioned_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      versioned_keys.push_back(key);
      auto val = blockchain.getLatest("versioned", key);
      ASSERT_TRUE(val.has_value());
      auto ver_val = std::get<categorization::VersionedValue>(*val);
      ASSERT_EQ(ver_val.block_id, blk);
      ASSERT_EQ(ver_val.data, val_str);
    }
    for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
      std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "immutable_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      immutable_keys.push_back(key);
      auto val = blockchain.getLatest("immutable", key);
      ASSERT_TRUE(val.has_value());
      auto immutable_val = std::get<categorization::ImmutableValue>(*val);
      ASSERT_EQ(immutable_val.block_id, blk);
      ASSERT_EQ(immutable_val.data, val_str);
    }
  }

  ///////Checking the multiGet///////////////////////
  std::vector<std::optional<categorization::Value>> values;
  blockchain.multiGetLatest("merkle", merkle_keys, values);
  size_t key_start = std::string("merkle_key_").size();
  size_t val_start = std::string("merkle_value_").size();
  for (size_t i = 0; i < merkle_keys.size(); ++i) {
    ASSERT_TRUE(values[i].has_value());
    auto merkle_val = std::get<categorization::MerkleValue>(*(values[i]));
    ASSERT_EQ(merkle_keys[i].substr(key_start), (merkle_val.data).substr(val_start));
  }

  blockchain.multiGetLatest("versioned", versioned_keys, values);
  key_start = std::string("versioned_key_").size();
  val_start = std::string("versioned_value_").size();
  for (size_t i = 0; i < versioned_keys.size(); ++i) {
    ASSERT_TRUE(values[i].has_value());
    auto versioned_val = std::get<categorization::VersionedValue>(*(values[i]));
    ASSERT_EQ(versioned_keys[i].substr(key_start), (versioned_val.data).substr(val_start));
  }

  blockchain.multiGetLatest("immutable", immutable_keys, values);
  key_start = std::string("immutable_key_").size();
  val_start = std::string("immutable_value_").size();
  for (size_t i = 0; i < immutable_keys.size(); ++i) {
    ASSERT_TRUE(values[i].has_value());
    auto immutable_val = std::get<categorization::ImmutableValue>(*(values[i]));
    ASSERT_EQ(immutable_keys[i].substr(key_start), (immutable_val.data).substr(val_start));
  }

  merkle_keys.clear();
  versioned_keys.clear();
  immutable_keys.clear();

  std::set<std::string> deleted_keys;
  uint64_t new_max_block = max_block;
  add_deletes_to_blocks(
      new_max_block,
      num_merkle_each,
      num_versioned_each,
      num_immutable_each,
      [&deleted_keys](const std::string& key, uint64_t blkid, uint32_t kid, uint32_t kid_max) -> bool {
        std::mt19937 rgen;
        std::uniform_int_distribution<uint32_t> dist(2, std::numeric_limits<uint32_t>::max());
        if (((blkid + kid) * dist(rgen)) % 2 == 0) {
          deleted_keys.insert(key);
          return true;
        }
        return false;
      });
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), new_max_block);
  // Checking again
  for (uint64_t blk = max_block + 1; blk <= new_max_block; ++blk) {
    for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
      std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "merkle_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      merkle_keys.push_back(key);
      auto val = blockchain.getLatest("merkle", key);
      if (deleted_keys.count(key)) {
        ASSERT_FALSE(val.has_value());
      } else {
        ASSERT_TRUE(val.has_value());
        auto merkle_val = std::get<categorization::MerkleValue>(*val);
        ASSERT_EQ(merkle_val.block_id, blk);
        ASSERT_EQ(merkle_val.data, val_str);
      }
    }

    for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
      std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "versioned_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      versioned_keys.push_back(key);
      auto val = blockchain.getLatest("versioned", key);
      if (deleted_keys.count(key)) {
        ASSERT_FALSE(val.has_value());
      } else {
        ASSERT_TRUE(val.has_value());
        auto ver_val = std::get<categorization::VersionedValue>(*val);
        ASSERT_EQ(ver_val.block_id, blk);
        ASSERT_EQ(ver_val.data, val_str);
      }
    }
    for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
      std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      std::string val_str = "immutable_value_" + std::to_string(blk) + "_" + std::to_string(kid);
      immutable_keys.push_back(key);
      auto val = blockchain.getLatest("immutable", key);
      if (deleted_keys.count(key)) {
        ASSERT_FALSE(val.has_value());
      } else {
        ASSERT_TRUE(val.has_value());
        auto immutable_val = std::get<categorization::ImmutableValue>(*val);
        ASSERT_EQ(immutable_val.block_id, blk);
        ASSERT_EQ(immutable_val.data, val_str);
      }
    }
  }

  ///////Checking the multiGet///////////////////////
  blockchain.multiGetLatest("merkle", merkle_keys, values);
  key_start = std::string("merkle_key_").size();
  val_start = std::string("merkle_value_").size();
  for (size_t i = 0; i < merkle_keys.size(); ++i) {
    if (deleted_keys.count(merkle_keys[i])) {
      ASSERT_FALSE(values[i].has_value());
    } else {
      ASSERT_TRUE(values[i].has_value());
      auto merkle_val = std::get<categorization::MerkleValue>(*(values[i]));
      ASSERT_EQ(merkle_keys[i].substr(key_start), (merkle_val.data).substr(val_start));
    }
  }

  blockchain.multiGetLatest("versioned", versioned_keys, values);
  key_start = std::string("versioned_key_").size();
  val_start = std::string("versioned_value_").size();
  for (size_t i = 0; i < versioned_keys.size(); ++i) {
    if (deleted_keys.count(versioned_keys[i])) {
      ASSERT_FALSE(values[i].has_value());
    } else {
      ASSERT_TRUE(values[i].has_value());
      auto versioned_val = std::get<categorization::VersionedValue>(*(values[i]));
      ASSERT_EQ(versioned_keys[i].substr(key_start), (versioned_val.data).substr(val_start));
    }
  }

  blockchain.multiGetLatest("immutable", immutable_keys, values);
  key_start = std::string("immutable_key_").size();
  val_start = std::string("immutable_value_").size();
  for (size_t i = 0; i < immutable_keys.size(); ++i) {
    if (deleted_keys.count(immutable_keys[i])) {
      ASSERT_FALSE(values[i].has_value());
    } else {
      ASSERT_TRUE(values[i].has_value());
      auto immutable_val = std::get<categorization::ImmutableValue>(*(values[i]));
      ASSERT_EQ(immutable_keys[i].substr(key_start), (immutable_val.data).substr(val_start));
    }
  }
}

TEST_F(v4_kvbc, get_block_updates_test) {
  uint64_t max_block = 100;
  uint32_t num_merkle_each = 0;
  uint32_t num_versioned_each = 0;
  uint32_t num_immutable_each = 0;
  create_blocks(max_block, num_merkle_each, num_versioned_each, num_immutable_each);

  std::map<std::string, categorization::CATEGORY_TYPE> cat_map{
      {"merkle", categorization::CATEGORY_TYPE::block_merkle},
      {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
      {"immutable", categorization::CATEGORY_TYPE::immutable}};
  v4blockchain::KeyValueBlockchain blockchain{db, true, cat_map};
  for (uint64_t blk = 1; blk <= max_block; ++blk) {
    auto updates = blockchain.getBlockUpdates(blk);
    ASSERT_TRUE(updates.has_value());
    auto merkle_cat_inputs = updates->categoryUpdates("merkle");
    ASSERT_TRUE(merkle_cat_inputs.has_value());
    ASSERT_EQ(std::get<categorization::BlockMerkleInput>(merkle_cat_inputs->get()).kv.size(), num_merkle_each);
    auto versioned_cat_inputs = updates->categoryUpdates("versioned");
    ASSERT_TRUE(versioned_cat_inputs.has_value());
    ASSERT_EQ(std::get<categorization::VersionedInput>(versioned_cat_inputs->get()).kv.size(), num_versioned_each);
    auto immutable_cat_inputs = updates->categoryUpdates("immutable");
    ASSERT_TRUE(immutable_cat_inputs.has_value());
    ASSERT_EQ(std::get<categorization::ImmutableInput>(immutable_cat_inputs->get()).kv.size(), num_immutable_each);
  }
  for (uint64_t blk = max_block + 1; blk <= max_block + 10; ++blk) {
    auto updates = blockchain.getBlockUpdates(blk);
    ASSERT_FALSE(updates.has_value());
  }
  uint64_t new_max_block = max_block;
  add_deletes_to_blocks(new_max_block,
                        num_merkle_each,
                        num_versioned_each,
                        num_immutable_each,
                        [](const std::string& key, uint64_t blkid, uint32_t kid, uint32_t kid_max) -> bool {
                          std::mt19937 rgen;
                          std::uniform_int_distribution<uint32_t> dist(2, std::numeric_limits<uint32_t>::max());
                          if (((blkid + kid) * dist(rgen)) % 2 == 0) {
                            return true;
                          }
                          return false;
                        });
  for (uint64_t blk = 1; blk <= max_block; ++blk) {
    auto updates = blockchain.getBlockUpdates(blk);
    ASSERT_TRUE(updates.has_value());
    auto merkle_cat_inputs = updates->categoryUpdates("merkle");
    ASSERT_TRUE(merkle_cat_inputs.has_value());
    ASSERT_EQ(std::get<categorization::BlockMerkleInput>(merkle_cat_inputs->get()).kv.size(), num_merkle_each);
    auto versioned_cat_inputs = updates->categoryUpdates("versioned");
    ASSERT_TRUE(versioned_cat_inputs.has_value());
    ASSERT_EQ(std::get<categorization::VersionedInput>(versioned_cat_inputs->get()).kv.size(), num_versioned_each);
    auto immutable_cat_inputs = updates->categoryUpdates("immutable");
    ASSERT_TRUE(immutable_cat_inputs.has_value());
    ASSERT_EQ(std::get<categorization::ImmutableInput>(immutable_cat_inputs->get()).kv.size(), num_immutable_each);
  }
  for (uint64_t blk = max_block + 1; blk <= new_max_block; ++blk) {
    auto updates = blockchain.getBlockUpdates(blk);
    ASSERT_TRUE(updates.has_value());
    auto merkle_cat_inputs = updates->categoryUpdates("merkle");
    ASSERT_TRUE(merkle_cat_inputs.has_value());
    ASSERT_LT(std::get<categorization::BlockMerkleInput>(merkle_cat_inputs->get()).kv.size(), num_merkle_each);
    auto versioned_cat_inputs = updates->categoryUpdates("versioned");
    ASSERT_TRUE(versioned_cat_inputs.has_value());
    ASSERT_LT(std::get<categorization::VersionedInput>(versioned_cat_inputs->get()).kv.size(), num_versioned_each);
    auto immutable_cat_inputs = updates->categoryUpdates("immutable");
    ASSERT_TRUE(immutable_cat_inputs.has_value());
    ASSERT_EQ(std::get<categorization::ImmutableInput>(immutable_cat_inputs->get()).kv.size(), num_immutable_each);
  }
  for (uint64_t blk = new_max_block + 1; blk <= new_max_block + 10; ++blk) {
    auto updates = blockchain.getBlockUpdates(blk);
    ASSERT_FALSE(updates.has_value());
  }
}

TEST_F(v4_kvbc, all_get_latest_versions) {
  uint64_t max_block = 100;
  uint32_t num_merkle_each = 0;
  uint32_t num_versioned_each = 0;
  uint32_t num_immutable_each = 0;
  create_blocks(max_block, num_merkle_each, num_versioned_each, num_immutable_each);

  std::map<std::string, categorization::CATEGORY_TYPE> cat_map{
      {"merkle", categorization::CATEGORY_TYPE::block_merkle},
      {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
      {"immutable", categorization::CATEGORY_TYPE::immutable}};
  v4blockchain::KeyValueBlockchain blockchain{db, true, cat_map};
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), max_block);

  std::vector<std::string> merkle_keys;
  std::vector<BlockId> merkle_versions;
  std::vector<std::string> versioned_keys;
  std::vector<BlockId> versioned_versions;
  std::vector<std::string> immutable_keys;
  std::vector<BlockId> immutable_versions;

  ///////Checking the get///////////////////////
  for (uint64_t blk = 1; blk <= max_block; ++blk) {
    for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
      std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      merkle_keys.push_back(key);
      merkle_versions.push_back(blk);
      auto latest_version = blockchain.getLatestVersion("merkle", key);
      ASSERT_TRUE(latest_version.has_value());
      ASSERT_EQ(latest_version->version, blk);
    }

    for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
      std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      versioned_keys.push_back(key);
      versioned_versions.push_back(blk);
      auto latest_version = blockchain.getLatestVersion("versioned", key);
      ASSERT_TRUE(latest_version.has_value());
      ASSERT_EQ(latest_version->version, blk);
    }
    for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
      std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      immutable_keys.push_back(key);
      immutable_versions.push_back(blk);
      auto latest_version = blockchain.getLatestVersion("immutable", key);
      ASSERT_TRUE(latest_version.has_value());
      ASSERT_EQ(latest_version->version, blk);
    }
  }

  ///////Checking the multiGetLatestVersion///////////////////////
  std::vector<std::optional<categorization::TaggedVersion>> latest_versions;
  blockchain.multiGetLatestVersion("merkle", merkle_keys, latest_versions);
  for (size_t i = 0; i < merkle_keys.size(); ++i) {
    ASSERT_TRUE(latest_versions[i].has_value());
    ASSERT_EQ(merkle_versions[i], (latest_versions[i])->version);
  }

  blockchain.multiGetLatestVersion("versioned", versioned_keys, latest_versions);
  for (size_t i = 0; i < versioned_keys.size(); ++i) {
    ASSERT_TRUE(latest_versions[i].has_value());
    ASSERT_EQ(versioned_versions[i], (latest_versions[i])->version);
  }

  blockchain.multiGetLatestVersion("immutable", immutable_keys, latest_versions);
  for (size_t i = 0; i < immutable_keys.size(); ++i) {
    ASSERT_TRUE(latest_versions[i].has_value());
    ASSERT_EQ(immutable_versions[i], (latest_versions[i])->version);
  }

  merkle_keys.clear();
  merkle_versions.clear();
  versioned_keys.clear();
  versioned_versions.clear();
  immutable_keys.clear();
  immutable_versions.clear();

  std::set<std::string> deleted_keys;
  uint64_t new_max_block = max_block;
  add_deletes_to_blocks(
      new_max_block,
      num_merkle_each,
      num_versioned_each,
      num_immutable_each,
      [&deleted_keys](const std::string& key, uint64_t blkid, uint32_t kid, uint32_t kid_max) -> bool {
        std::mt19937 rgen;
        std::uniform_int_distribution<uint32_t> dist(2, std::numeric_limits<uint32_t>::max());
        if (((blkid + kid) * dist(rgen)) % 2 == 0) {
          deleted_keys.insert(key);
          return true;
        }
        return false;
      });
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), new_max_block);
  // Checking again
  for (uint64_t blk = 1; blk <= max_block; ++blk) {
    for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
      std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      merkle_keys.push_back(key);
      merkle_versions.push_back(blk);
      auto latest_version = blockchain.getLatestVersion("merkle", key);
      ASSERT_TRUE(latest_version.has_value());
      ASSERT_EQ(latest_version->version, blk);
    }

    for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
      std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      versioned_keys.push_back(key);
      versioned_versions.push_back(blk);
      auto latest_version = blockchain.getLatestVersion("versioned", key);
      ASSERT_TRUE(latest_version.has_value());
      ASSERT_EQ(latest_version->version, blk);
    }
    for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
      std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      immutable_keys.push_back(key);
      immutable_versions.push_back(blk);
      auto latest_version = blockchain.getLatestVersion("immutable", key);
      ASSERT_TRUE(latest_version.has_value());
      ASSERT_EQ(latest_version->version, blk);
    }
  }

  ///////Checking the multiGetLatestVersion again after deletion///////////////////////
  blockchain.multiGetLatestVersion("merkle", merkle_keys, latest_versions);
  for (size_t i = 0; i < merkle_keys.size(); ++i) {
    ASSERT_TRUE(latest_versions[i].has_value());
    ASSERT_EQ(merkle_versions[i], (latest_versions[i])->version);
  }

  blockchain.multiGetLatestVersion("versioned", versioned_keys, latest_versions);
  for (size_t i = 0; i < versioned_keys.size(); ++i) {
    ASSERT_TRUE(latest_versions[i].has_value());
    ASSERT_EQ(versioned_versions[i], (latest_versions[i])->version);
  }

  blockchain.multiGetLatestVersion("immutable", immutable_keys, latest_versions);
  for (size_t i = 0; i < immutable_keys.size(); ++i) {
    ASSERT_TRUE(latest_versions[i].has_value());
    ASSERT_EQ(immutable_versions[i], (latest_versions[i])->version);
  }

  merkle_keys.clear();
  merkle_versions.clear();
  versioned_keys.clear();
  versioned_versions.clear();
  immutable_keys.clear();
  immutable_versions.clear();
  // Checking again
  for (uint64_t blk = max_block + 1; blk <= new_max_block; ++blk) {
    for (uint32_t kid = 1; kid <= num_merkle_each; ++kid) {
      std::string key = "merkle_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      merkle_keys.push_back(key);
      merkle_versions.push_back(blk);
      auto latest_version = blockchain.getLatestVersion("merkle", key);
      if (deleted_keys.count(key)) {
        ASSERT_FALSE(latest_version.has_value());
      } else {
        ASSERT_TRUE(latest_version.has_value());
        ASSERT_EQ(latest_version->version, blk);
      }
    }

    for (uint32_t kid = 1; kid <= num_versioned_each; ++kid) {
      std::string key = "versioned_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      versioned_keys.push_back(key);
      versioned_versions.push_back(blk);
      auto latest_version = blockchain.getLatestVersion("versioned", key);
      if (deleted_keys.count(key)) {
        ASSERT_FALSE(latest_version.has_value());
      } else {
        ASSERT_TRUE(latest_version.has_value());
        ASSERT_EQ(latest_version->version, blk);
      }
    }
    for (uint32_t kid = 1; kid <= num_immutable_each; ++kid) {
      std::string key = "immutable_key_" + std::to_string(blk) + "_" + std::to_string(kid);
      immutable_keys.push_back(key);
      immutable_versions.push_back(blk);
      auto latest_version = blockchain.getLatestVersion("immutable", key);
      if (deleted_keys.count(key)) {
        ASSERT_FALSE(latest_version.has_value());
      } else {
        ASSERT_TRUE(latest_version.has_value());
        ASSERT_EQ(latest_version->version, blk);
      }
    }
  }

  ///////Checking the multiGet///////////////////////
  blockchain.multiGetLatestVersion("merkle", merkle_keys, latest_versions);
  for (size_t i = 0; i < merkle_keys.size(); ++i) {
    if (deleted_keys.count(merkle_keys[i])) {
      ASSERT_FALSE(latest_versions[i].has_value());
    } else {
      ASSERT_TRUE(latest_versions[i].has_value());
      ASSERT_EQ(merkle_versions[i], (latest_versions[i])->version);
    }
  }

  blockchain.multiGetLatestVersion("versioned", versioned_keys, latest_versions);
  for (size_t i = 0; i < versioned_keys.size(); ++i) {
    if (deleted_keys.count(versioned_keys[i])) {
      ASSERT_FALSE(latest_versions[i].has_value());
    } else {
      ASSERT_TRUE(latest_versions[i].has_value());
      ASSERT_EQ(versioned_versions[i], (latest_versions[i])->version);
    }
  }

  blockchain.multiGetLatestVersion("immutable", immutable_keys, latest_versions);
  for (size_t i = 0; i < immutable_keys.size(); ++i) {
    if (deleted_keys.count(immutable_keys[i])) {
      ASSERT_FALSE(latest_versions[i].has_value());
    } else {
      ASSERT_TRUE(latest_versions[i].has_value());
      ASSERT_EQ(immutable_versions[i], (latest_versions[i])->version);
    }
  }
}

TEST_F(v4_kvbc, trim_blocks) {
  uint64_t max_block = 100;
  uint32_t num_merkle_each = 0;
  uint32_t num_versioned_each = 0;
  uint32_t num_immutable_each = 0;
  create_blocks(max_block, num_merkle_each, num_versioned_each, num_immutable_each);

  std::map<std::string, categorization::CATEGORY_TYPE> cat_map{
      {"merkle", categorization::CATEGORY_TYPE::block_merkle},
      {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
      {"immutable", categorization::CATEGORY_TYPE::immutable}};
  v4blockchain::KeyValueBlockchain blockchain{db, true, cat_map};
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), max_block);
  ASSERT_NO_THROW(blockchain.trimBlocksFromSnapshot(max_block / 2));
  ASSERT_EQ(blockchain.getGenesisBlockId(), 1);
  ASSERT_EQ(blockchain.getLastReachableBlockId(), max_block / 2);
  ASSERT_DEATH(blockchain.trimBlocksFromSnapshot(v4blockchain::detail::Blockchain::INVALID_BLOCK_ID), "");
  ASSERT_DEATH(blockchain.trimBlocksFromSnapshot(max_block + 10), "");
}

}  // end namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
