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
#include "categorization/db_categories.h"
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
    destroyDb();
    db = TestRocksDb::createNative();
    db->createColumnFamily(BLOCKS_CF);
    db->createColumnFamily(ST_CHAIN_CF);
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

TEST_F(categorized_kvbc, serialization_and_desirialization_of_block) {
  BlockDigest pHash;
  std::random_device rd;
  for (int i = 0; i < (int)pHash.size(); i++) {
    pHash[i] = (uint8_t)(rd() % 255);
  }
  Block block{8};
  VersionedOutput out;
  out.keys["key1"] = {false, false};
  out.keys["key2"] = {true, false};
  block.add("VersionedOutput", std::move(out));
  block.setParentHash(pHash);
  auto ser = Block::serialize(block);
  auto des_block = Block::deserialize(ser);

  // Test the deserialized Block
  ASSERT_TRUE(des_block.id() == 8);
  auto variant = des_block.data.categories_updates_info["VersionedOutput"];
  VersionedOutput ver_out = std::get<VersionedOutput>(variant);
  ASSERT_TRUE(ver_out.keys.size() == 2);
  ASSERT_TRUE(ver_out.keys["key2"].deleted == true);
  ASSERT_EQ(des_block.data.parent_digest, block.data.parent_digest);
}

TEST_F(categorized_kvbc, reconstruct_merkle_updates) {
  KeyValueBlockchain block_chain{
      db,
      true,
      std::map<std::string, CATEGORY_TYPE>{{"merkle", CATEGORY_TYPE::block_merkle},
                                           {kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}}};
  KeyValueBlockchain::KeyValueBlockchain_tester tester{};

  // Add block1
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    updates.add("merkle", std::move(merkle_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }

  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "merkle_value3");
    merkle_updates.addDelete("merkle_key1");
    updates.add("merkle", std::move(merkle_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  {
    const detail::Buffer& block_db_key = Block::generateKey(1);
    auto block1_db_val = db->get(BLOCKS_CF, block_db_key);
    ASSERT_TRUE(block1_db_val.has_value());

    detail::Buffer in{block1_db_val.value().begin(), block1_db_val.value().end()};
    auto block1_from_db = Block::deserialize(in);

    categorization::RawBlock rw(block1_from_db, db, tester.getCategories(block_chain));
    auto variant = rw.data.updates.kv["merkle"];
    auto merkle_updates = std::get<BlockMerkleInput>(variant);
    // check reconstruction of original kv
    ASSERT_EQ(merkle_updates.kv.size(), 2);
    ASSERT_EQ(merkle_updates.deletes.size(), 0);
    ASSERT_EQ(merkle_updates.kv["merkle_key1"], "merkle_value1");
    ASSERT_EQ(merkle_updates.kv["merkle_key2"], "merkle_value2");
  }

  {
    const detail::Buffer& block_db_key = Block::generateKey(2);
    auto block2_db_val = db->get(BLOCKS_CF, block_db_key);
    ASSERT_TRUE(block2_db_val.has_value());

    detail::Buffer in{block2_db_val.value().begin(), block2_db_val.value().end()};
    auto block2_from_db = Block::deserialize(in);

    categorization::RawBlock rw(block2_from_db, db, tester.getCategories(block_chain));
    auto variant = rw.data.updates.kv["merkle"];
    auto merkle_updates = std::get<BlockMerkleInput>(variant);
    // check reconstruction of original kv
    ASSERT_EQ(merkle_updates.kv.size(), 1);
    ASSERT_EQ(merkle_updates.deletes.size(), 1);
    ASSERT_EQ(merkle_updates.kv["merkle_key3"], "merkle_value3");
    ASSERT_EQ(merkle_updates.deletes[0], "merkle_key1");
  }
}

TEST_F(categorized_kvbc, fail_reconstruct_merkle_updates) {
  // BlockDigest pHash;
  // std::random_device rd;
  // for (int i = 0; i < (int)pHash.size(); i++) {
  //   pHash[i] = (uint8_t)(rd() % 255);
  // }

  // auto cf = std::string("merkle");
  // auto key = std::string("key");
  // auto value = std::string("val");

  // uint64_t state_root_version = 886;
  // BlockMerkleOutput mui;
  // mui.keys[key] = MerkleKeyFlag{false};
  // mui.root_hash = pHash;
  // mui.state_root_version = state_root_version;

  // Block block{888};
  // block.add(cf, std::move(mui));
  // block.setParentHash(pHash);

  // db->createColumnFamily(cf);

  // ASSERT_THROW(categorization::RawBlock rw(block, db), std::runtime_error);
}

TEST_F(categorized_kvbc, reconstruct_immutable_updates) {
  KeyValueBlockchain block_chain{
      db,
      true,
      std::map<std::string, CATEGORY_TYPE>{{"imm", CATEGORY_TYPE::immutable},
                                           {"imm2", CATEGORY_TYPE::immutable},
                                           {kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}}};
  KeyValueBlockchain::KeyValueBlockchain_tester tester{};

  // Add block1
  {
    Updates updates;
    ImmutableUpdates imm_updates;
    imm_updates.addUpdate("imm_key1", ImmutableUpdates::ImmutableValue{"imm_value1", {"1", "2"}});
    imm_updates.addUpdate("imm_key2", ImmutableUpdates::ImmutableValue{"imm_value2", {"12", "22"}});
    updates.add("imm", std::move(imm_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }

  // Add block2
  {
    Updates updates;
    ImmutableUpdates imm_updates;
    imm_updates.addUpdate("imm_key3", ImmutableUpdates::ImmutableValue{"imm_value3", {}});
    updates.add("imm", std::move(imm_updates));

    ImmutableUpdates imm_updates2;
    imm_updates2.addUpdate("imm_key12", ImmutableUpdates::ImmutableValue{"imm_value32", {"32"}});
    updates.add("imm2", std::move(imm_updates2));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  {
    const detail::Buffer& block_db_key = Block::generateKey(1);
    auto block1_db_val = db->get(BLOCKS_CF, block_db_key);
    ASSERT_TRUE(block1_db_val.has_value());

    detail::Buffer in{block1_db_val.value().begin(), block1_db_val.value().end()};
    auto block1_from_db = Block::deserialize(in);

    categorization::RawBlock rw(block1_from_db, db, tester.getCategories(block_chain));
    auto variant = rw.data.updates.kv["imm"];
    auto imm_updates = std::get<ImmutableInput>(variant);
    // check reconstruction of original kv
    ASSERT_EQ(imm_updates.kv.size(), 2);
    auto ex1 = ImmutableValueUpdate{"imm_value1", {"1", "2"}};
    auto ex2 = ImmutableValueUpdate{"imm_value2", {"12", "22"}};
    ASSERT_EQ(imm_updates.kv["imm_key1"], ex1);
    ASSERT_EQ(imm_updates.kv["imm_key2"], ex2);
  }

  {
    const detail::Buffer& block_db_key = Block::generateKey(2);
    auto block2_db_val = db->get(BLOCKS_CF, block_db_key);
    ASSERT_TRUE(block2_db_val.has_value());

    detail::Buffer in{block2_db_val.value().begin(), block2_db_val.value().end()};
    auto block2_from_db = Block::deserialize(in);

    categorization::RawBlock rw(block2_from_db, db, tester.getCategories(block_chain));
    auto variant = rw.data.updates.kv["imm"];
    auto imm_updates = std::get<ImmutableInput>(variant);
    // check reconstruction of original kv
    ASSERT_EQ(imm_updates.kv.size(), 1);
    auto ex1 = ImmutableValueUpdate{"imm_value3", {}};
    ASSERT_EQ(imm_updates.kv["imm_key3"], ex1);

    auto variant2 = rw.data.updates.kv["imm2"];
    auto imm_updates2 = std::get<ImmutableInput>(variant2);
    // check reconstruction of original kv
    ASSERT_EQ(imm_updates2.kv.size(), 1);
    auto ex2 = ImmutableValueUpdate{"imm_value32", {"32"}};
    ASSERT_EQ(imm_updates2.kv["imm_key12"], ex2);
  }
}

TEST_F(categorized_kvbc, fail_reconstruct_immutable_updates) {
  KeyValueBlockchain block_chain{
      db,
      true,
      std::map<std::string, CATEGORY_TYPE>{{"imm", CATEGORY_TYPE::immutable},
                                           {kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}}};
  KeyValueBlockchain::KeyValueBlockchain_tester tester{};

  // Add block1
  {
    Updates updates;
    ImmutableUpdates imm_updates;
    imm_updates.addUpdate("imm_key1", ImmutableUpdates::ImmutableValue{"imm_value1", {"1", "2"}});
    imm_updates.addUpdate("imm_key2", ImmutableUpdates::ImmutableValue{"imm_value2", {"12", "22"}});
    updates.add("imm", std::move(imm_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }
  const detail::Buffer& block_db_key = Block::generateKey(1);
  auto block1_db_val = db->get(BLOCKS_CF, block_db_key);
  ASSERT_TRUE(block1_db_val.has_value());

  detail::Buffer in{block1_db_val.value().begin(), block1_db_val.value().end()};
  auto block1_from_db = Block::deserialize(in);

  detail::ImmutableKeyValueCategory cat{"imm", db};
  auto wb = db->getBatch();
  auto out = std::get<ImmutableOutput>(block1_from_db.data.categories_updates_info["imm"]);
  cat.deleteBlock(out, wb);
  db->write(std::move(wb));

  ASSERT_DEATH(categorization::RawBlock rw(block1_from_db, db, tester.getCategories(block_chain)), "");
}

TEST_F(categorized_kvbc, reconstruct_versioned_kv_updates) {
  KeyValueBlockchain block_chain{
      db,
      true,
      std::map<std::string, CATEGORY_TYPE>{{"ver", CATEGORY_TYPE::versioned_kv},
                                           {"ver2", CATEGORY_TYPE::versioned_kv},
                                           {kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}}};
  KeyValueBlockchain::KeyValueBlockchain_tester tester{};

  // Add block1
  {
    Updates updates;
    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key1", "ver_value1");
    ver_updates.addUpdate("ver_key2", VersionedUpdates::Value{"ver_value2", true});
    updates.add("ver", std::move(ver_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }

  // Add block2
  {
    Updates updates;
    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key3", "ver_value3");
    ver_updates.addDelete("ver_key2");
    updates.add("ver", std::move(ver_updates));

    VersionedUpdates ver_updates2;
    ver_updates2.addUpdate("ver_key2_3", "ver_value3");
    updates.add("ver2", std::move(ver_updates2));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  {
    const detail::Buffer& block_db_key = Block::generateKey(1);
    auto block1_db_val = db->get(BLOCKS_CF, block_db_key);
    ASSERT_TRUE(block1_db_val.has_value());

    detail::Buffer in{block1_db_val.value().begin(), block1_db_val.value().end()};
    auto block1_from_db = Block::deserialize(in);

    categorization::RawBlock rw(block1_from_db, db, tester.getCategories(block_chain));
    auto variant = rw.data.updates.kv["ver"];
    auto ver_updates = std::get<VersionedInput>(variant);
    // check reconstruction of original kv
    ASSERT_EQ(ver_updates.kv.size(), 2);
    auto ex1 = ValueWithFlags{"ver_value1", false};
    auto ex2 = ValueWithFlags{"ver_value2", true};
    ASSERT_EQ(ver_updates.kv["ver_key1"], ex1);
    ASSERT_EQ(ver_updates.kv["ver_key2"], ex2);
  }

  {
    const detail::Buffer& block_db_key = Block::generateKey(2);
    auto block1_db_val = db->get(BLOCKS_CF, block_db_key);
    ASSERT_TRUE(block1_db_val.has_value());

    detail::Buffer in{block1_db_val.value().begin(), block1_db_val.value().end()};
    auto block1_from_db = Block::deserialize(in);

    categorization::RawBlock rw(block1_from_db, db, tester.getCategories(block_chain));
    auto variant = rw.data.updates.kv["ver"];
    auto ver_updates = std::get<VersionedInput>(variant);
    // check reconstruction of original kv
    ASSERT_EQ(ver_updates.kv.size(), 1);
    ASSERT_EQ(ver_updates.deletes.size(), 1);
    auto ex1 = ValueWithFlags{"ver_value3", false};
    ASSERT_EQ(ver_updates.kv["ver_key3"], ex1);
    ASSERT_EQ(ver_updates.deletes[0], "ver_key2");

    auto variant2 = rw.data.updates.kv["ver2"];
    auto ver_updates2 = std::get<VersionedInput>(variant2);
    // check reconstruction of original kv
    ASSERT_EQ(ver_updates2.kv.size(), 1);
    auto ex2 = ValueWithFlags{"ver_value3", false};
    ASSERT_EQ(ver_updates2.kv["ver_key2_3"], ex2);
  }

}  // namespace

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
