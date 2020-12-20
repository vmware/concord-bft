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
#include "categorization/column_families.h"
#include "categorization/updates.h"
#include "categorization/kv_blockchain.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <random>
#include "storage/test/storage_test_common.h"

using concord::storage::rocksdb::NativeClient;
using namespace concord::kvbc::categorization;
using namespace concord::kvbc::categorization::detail;
using namespace concord::kvbc;

namespace {

class categorized_kvbc : public ::testing::Test {
  void SetUp() override {
    cleanup();
    db = TestRocksDb::createNative();
    db->createColumnFamily(BLOCKS_CF);
    db->createColumnFamily(ST_CHAIN_CF);
  }
  void TearDown() override { cleanup(); }

 protected:
  std::shared_ptr<NativeClient> db;
};

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

  categorization::RawBlock rw(block, *db.get());
  ASSERT_EQ(rw.data.parent_digest, block.data.parent_digest);
  auto variant = rw.data.category_updates[cf];
  auto merkle_updates = std::get<MerkleUpdatesData>(variant);
  // check reconstruction of original kv
  ASSERT_EQ(merkle_updates.kv[key], value);
}

TEST_F(categorized_kvbc, fail_reconstruct_merkle_updates) {
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

  ASSERT_THROW(categorization::RawBlock rw(block, *db.get()), std::logic_error);
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
