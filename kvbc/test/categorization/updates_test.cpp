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

/**
 * The following test suite tests ordering of KeyValuePairs
 */

#include "gtest/gtest.h"
#include "categorization/updates.h"
#include "categorization/kv_blockchain.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <random>
#include "storage/test/storage_test_common.h"

using namespace concord::kvbc::categorization;
using namespace concord::kvbc;

namespace {

class categorized_kvbc : public ::testing::Test {
  void SetUp() override {
    cleanup();
    db = TestRocksDb::createNative();
  }
  void TearDown() override { cleanup(); }

 protected:
  std::shared_ptr<NativeClient> db;
};

TEST_F(categorized_kvbc, merkle_update) {
  std::string key{"key"};
  std::string val{"val"};
  MerkleUpdates mu;
  mu.addUpdate(std::move(key), std::move(val));
  ASSERT_TRUE(mu.getData().kv.size() == 1);
}

TEST_F(categorized_kvbc, add_blocks) {
  db->createColumnFamily(Block::CATEGORY_ID);
  KeyValueBlockchain block_chain{db};
  // Add block1
  {
    Updates updates;
    MerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    KeyValueUpdates keyval_updates;
    keyval_updates.addUpdate("kv_key1", "key_val1");
    keyval_updates.addDelete("kv_deleted");
    updates.add("kv_hash", std::move(keyval_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }
  // Add block2
  {
    Updates updates;
    MerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    KeyValueUpdates keyval_updates;
    keyval_updates.addUpdate("kv_key2", "key_val2");
    keyval_updates.addDelete("kv_deleted");
    updates.add("kv_hash", std::move(keyval_updates));

    SharedKeyValueUpdates shared_updates;
    shared_updates.addUpdate("shared_key2", {"shared_Val2", {"1", "2"}});
    updates.add(std::move(shared_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }
  // get block 1 from DB and test it
  {
    const detail::Buffer& block_db_key = Block::generateKey(1);
    auto block1_db_val = db->get(Block::CATEGORY_ID, block_db_key);
    ASSERT_TRUE(block1_db_val.has_value());
    // E.L need to add support for strings in cmf
    detail::Buffer in{block1_db_val.value().begin(), block1_db_val.value().end()};
    auto block1_from_db = Block::deserialize(in);
    auto merkle_variant = block1_from_db.data.categories_updates_info["merkle"];
    auto merkle_update_info1 = std::get<MerkleUpdatesInfo>(merkle_variant);
    ASSERT_EQ(merkle_update_info1.keys["merkle_deleted"].deleted, true);
    auto kv_hash_variant = block1_from_db.data.categories_updates_info["kv_hash"];
    auto kv_hash_update_info1 = std::get<KeyValueUpdatesInfo>(kv_hash_variant);
    ASSERT_EQ(kv_hash_update_info1.keys["kv_deleted"].deleted, true);
    ASSERT_FALSE(block1_from_db.data.shared_updates_info.has_value());
  }
  // get block 2 from DB and test it
  {
    const detail::Buffer& block_db_key = Block::generateKey(2);
    auto block2_db_val = db->get(Block::CATEGORY_ID, block_db_key);
    ASSERT_TRUE(block2_db_val.has_value());
    // E.L need to add support for strings in cmf
    detail::Buffer in{block2_db_val.value().begin(), block2_db_val.value().end()};
    auto block2_from_db = Block::deserialize(in);
    ASSERT_TRUE(block2_from_db.data.shared_updates_info.has_value());
    std::vector<std::string> v{"1", "2"};
    ASSERT_EQ(block2_from_db.data.shared_updates_info.value().keys["shared_key2"].categories, v);
  }
}

TEST_F(categorized_kvbc, serialization_and_desirialization_of_block) {
  BlockDigest pHash;
  std::random_device rd;
  for (int i = 0; i < (int)pHash.size(); i++) {
    pHash[i] = (uint8_t)(rd() % 255);
  }
  Block block{8};
  KeyValueUpdatesInfo kvui;
  kvui.keys["key1"] = {false, false};
  kvui.keys["key2"] = {true, false};
  block.add("KeyValueUpdatesInfo", std::move(kvui));
  block.setParentHash(pHash);
  auto ser = Block::serialize(block);
  auto des_block = Block::deserialize(ser);

  // Test the deserialized Block
  ASSERT_TRUE(des_block.id() == 8);
  auto variant = des_block.data.categories_updates_info["KeyValueUpdatesInfo"];
  KeyValueUpdatesInfo kv_updates_info = std::get<KeyValueUpdatesInfo>(variant);
  ASSERT_TRUE(kv_updates_info.keys.size() == 2);
  ASSERT_TRUE(kv_updates_info.keys["key2"].deleted == true);
  ASSERT_EQ(des_block.data.parent_digest, block.data.parent_digest);
}

TEST_F(categorized_kvbc, reconstruct_merkle_updates) {
  BlockDigest pHash;
  std::random_device rd;
  for (int i = 0; i < (int)pHash.size(); i++) {
    pHash[i] = (uint8_t)(rd() % 255);
  }

  auto cf = std::string("merkle");
  auto key = std::string("key");
  auto value = std::string("val");

  uint64_t state_root_version = 886;
  MerkleUpdatesInfo mui;
  mui.keys[key] = MerkleKeyFlag{false};
  mui.root_hash = pHash;
  mui.state_root_version = state_root_version;

  Block block{888};
  block.add(cf, std::move(mui));
  block.setParentHash(pHash);

  db->createColumnFamily(cf);
  auto db_key = v2MerkleTree::detail::DBKeyManipulator::genDataDbKey(std::string(key), state_root_version);
  auto db_val = v2MerkleTree::detail::serialize(
      v2MerkleTree::detail::DatabaseLeafValue{block.id(), sparse_merkle::LeafNode{std::string(value)}});
  db->put(cf, db_key, db_val);
  auto db_get_val = db->get(cf, db_key);
  ASSERT_EQ(db_get_val.value(), db_val);

  concord::kvbc::categorization::RawBlock rw(block, db.get());
  ASSERT_EQ(rw.data.parent_digest, block.data.parent_digest);
  auto variant = rw.data.category_updates[cf];
  auto raw_merkle_updates = std::get<RawBlockMerkleUpdates>(variant);
  // check reconstruction of original kv
  ASSERT_EQ(raw_merkle_updates.updates.kv[key], value);
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
