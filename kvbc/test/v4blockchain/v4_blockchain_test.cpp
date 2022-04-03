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
#include <random>
#include "storage/test/storage_test_common.h"
#include "endianness.hpp"
#include "v4blockchain/detail/column_families.h"
#include "categorization/db_categories.h"
#include "block_metadata.hpp"

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

 protected:
  std::shared_ptr<NativeClient> db;
};

// Add a block which contains updates per category.
// Each category handles its updates and returs an output which goes to the block structure.
// The block structure is then inserted into the DB.
// we test that the block that is written to DB contains the expected data.
TEST_F(v4_kvbc, creation) {
  v4blockchain::KeyValueBlockchain block_chain{
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
    v4blockchain::KeyValueBlockchain block_chain{
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
      ASSERT_EQ(block_chain.add(std::move(updates)), (BlockId)1);
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
      ASSERT_EQ(block_chain.add(std::move(updates)), (BlockId)2);
    }
  }

  {
    v4blockchain::KeyValueBlockchain block_chain{
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
      ASSERT_EQ(block_chain.add(std::move(updates)), (BlockId)3);
    }

    // get block 1 from DB and test it
    {
      auto block1_db_val = block_chain.getBlockchain().getBlockData(1);
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
  v4blockchain::KeyValueBlockchain block_chain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {"merkle", categorization::CATEGORY_TYPE::block_merkle},
          {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
          {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
          {"immutable", categorization::CATEGORY_TYPE::immutable},
          {categorization::kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv}}};
  std::string no_flags = {0};
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

    ASSERT_EQ(block_chain.add(std::move(updates)), (BlockId)1);
    auto opt_block = block_chain.getBlockchain().getBlockData(1);
    ASSERT_TRUE(opt_block.has_value());

    std::vector<uint8_t> updates_from_storage{(*opt_block).begin() + v4blockchain::detail::Block::HEADER_SIZE,
                                              (*opt_block).end()};

    ASSERT_EQ(updates_buffer, updates_from_storage);

    // Get keys from latest
    // Merkle
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       block_chain.getLatestKeys().getCategoryPrefix("merkle") + "merkle_key1",
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
                  block_chain.getLatestKeys().getCategoryPrefix("immutable") + "immutable_key20",
                  block_version_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());

    std::vector<uint8_t> serialized_value;
    concord::kvbc::categorization::serialize(serialized_value, imm_val1);
    std::string str_val(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val + no_flags);
    ASSERT_EQ(out_ts, block_version_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(1, iout_ts);
    out_ts.clear();
    serialized_value.clear();

    // Versioned
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  block_chain.getLatestKeys().getCategoryPrefix("versioned") + "ver_key2",
                  block_version_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());

    concord::kvbc::categorization::serialize(serialized_value, ver_val);
    std::string str_val2(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val2 + std::string(1, v4blockchain::detail::LatestKeys::STALE_ON_UPDATE[0]));
    ASSERT_EQ(out_ts, block_version_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(1, iout_ts);
    out_ts.clear();
    serialized_value.clear();
  }
}

TEST_F(v4_kvbc, trim_history_get_block_sequence_number) {
  v4blockchain::KeyValueBlockchain block_chain{
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
    ASSERT_EQ(block_chain.getBlockSequenceNumber(updates), 0);
  }
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val", true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));
    ASSERT_EQ(block_chain.getBlockSequenceNumber(updates), 0);
  }

  {
    uint64_t id = 10;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, concord::kvbc::IBlockMetadata::kBlockMetadataKey),
                          categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));
    ASSERT_EQ(block_chain.getBlockSequenceNumber(updates), id);
  }
}

TEST_F(v4_kvbc, check_if_trim_history_is_needed) {
  v4blockchain::KeyValueBlockchain block_chain{
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
    ASSERT_EQ(block_chain.markHistoryForGarbageCollectionIfNeeded(updates), 0);
    ASSERT_EQ(block_chain.gc_counter, 0);
  }
  {
    // No meta data sn key
    uint64_t id = 10;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, 0x1), categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));
    ASSERT_EQ(block_chain.markHistoryForGarbageCollectionIfNeeded(updates), 0);
    ASSERT_EQ(block_chain.gc_counter, 0);
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

    ASSERT_EQ(block_chain.markHistoryForGarbageCollectionIfNeeded(updates), id);
    block_chain.setLastBlockSequenceNumber(id);
    ASSERT_EQ(block_chain.gc_counter, 0);
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

    ASSERT_EQ(block_chain.markHistoryForGarbageCollectionIfNeeded(updates), 0);
    ASSERT_EQ(block_chain.gc_counter, 0);
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

    ASSERT_EQ(block_chain.markHistoryForGarbageCollectionIfNeeded(updates), id);
    block_chain.setLastBlockSequenceNumber(id);
    ASSERT_EQ(block_chain.gc_counter, 1);
  }
  block_chain.checkpointInProcess(true);

  // sn was incremented but checkpoint in process
  {
    uint64_t id = 15;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, concord::kvbc::IBlockMetadata::kBlockMetadataKey),
                          categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));

    ASSERT_EQ(block_chain.markHistoryForGarbageCollectionIfNeeded(updates), 0);
    ASSERT_EQ(block_chain.gc_counter, 1);
  }

  block_chain.checkpointInProcess(false);

  // sn was incremented and no checkpoint in process
  {
    uint64_t id = 16;
    auto sid = concordUtils::toBigEndianStringBuffer(id);
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string(1, concord::kvbc::IBlockMetadata::kBlockMetadataKey),
                          categorization::VersionedUpdates::Value{sid, true});
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));

    ASSERT_EQ(block_chain.markHistoryForGarbageCollectionIfNeeded(updates), id);
    block_chain.setLastBlockSequenceNumber(id);
    ASSERT_EQ(block_chain.gc_counter, 2);
  }
}

}  // end namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
