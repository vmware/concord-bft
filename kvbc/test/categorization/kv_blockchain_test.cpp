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
  }
  void TearDown() override { cleanup(); }

 protected:
  std::shared_ptr<NativeClient> db;
};

TEST_F(categorized_kvbc, merkle_update) {
  std::string key{"key"};
  std::string val{"val"};
  BlockMerkleUpdates mu;
  mu.addUpdate(std::move(key), std::move(val));
  ASSERT_TRUE(mu.getData().kv.size() == 1);
}

// Add a block which contains updates per category.
// Each category handles its updates and returs an output which goes to the block structure.
// The block structure is then inserted into the DB.
// we test that the block that is written to DB contains the expected data.
TEST_F(categorized_kvbc, add_blocks) {
  KeyValueBlockchain block_chain{db, true};
  // Add block1
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(true);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }
  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "merkle_value3");
    merkle_updates.addDelete("merkle_key1");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key3", "ver_val3");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    VersionedUpdates ver_updates_2;
    // E.L --> some UT fails check if related to CMF
    ver_updates_2.calculateRootHash(false);
    ver_updates_2.addUpdate("ver_key4", "ver_val4");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }
  // get block 1 from DB and test it
  {
    const detail::Buffer& block_db_key = Block::generateKey(1);
    auto block1_db_val = db->get(BLOCKS_CF, block_db_key);
    ASSERT_TRUE(block1_db_val.has_value());

    detail::Buffer in{block1_db_val.value().begin(), block1_db_val.value().end()};
    auto block1_from_db = Block::deserialize(in);

    // First block has zeroed parent digest
    for (auto i : block1_from_db.data.parent_digest) {
      ASSERT_EQ(i, 0);
    }

    // Get the merkle updates output of block1
    auto merkle_variant = block1_from_db.data.categories_updates_info["merkle"];
    auto merkle_update_info1 = std::get<BlockMerkleOutput>(merkle_variant);
    ASSERT_EQ(merkle_update_info1.keys.size(), 2);
    ASSERT_EQ(merkle_update_info1.keys.count("merkle_key1"), 1);
    ASSERT_EQ(merkle_update_info1.keys.count("merkle_key2"), 1);
    ASSERT_EQ(merkle_update_info1.keys["merkle_key1"].deleted, false);
    ASSERT_EQ(merkle_update_info1.keys.count("not_exist"), 0);
    ASSERT_EQ(merkle_update_info1.state_root_version, 1);

    auto ver_variant = block1_from_db.data.categories_updates_info["versioned"];
    auto ver_out1 = std::get<VersionedOutput>(ver_variant);
    ASSERT_EQ(ver_out1.keys.size(), 2);
    ASSERT_EQ(ver_out1.keys.count("ver_key1"), 1);
    ASSERT_EQ(ver_out1.keys.count("ver_key2"), 1);
    ASSERT_EQ(ver_out1.keys["ver_key1"].deleted, false);
    ASSERT_EQ(ver_out1.keys["ver_key1"].stale_on_update, false);
    ASSERT_EQ(ver_out1.keys.count("not_exist"), 0);
    ASSERT_EQ(ver_out1.keys["ver_key2"].deleted, false);
    ASSERT_EQ(ver_out1.keys["ver_key2"].stale_on_update, true);
    ASSERT_EQ(ver_out1.root_hash.has_value(), true);
  }
  // get block 2 from DB and test it
  {
    const detail::Buffer& block_db_key = Block::generateKey(2);
    auto block2_db_val = db->get(BLOCKS_CF, block_db_key);
    ASSERT_TRUE(block2_db_val.has_value());
    detail::Buffer in{block2_db_val.value().begin(), block2_db_val.value().end()};
    auto block2_from_db = Block::deserialize(in);
    const auto expected_tags = std::vector<std::string>{"1", "2"};
    auto immutable_variant = block2_from_db.data.categories_updates_info["immutable"];
    auto immutable_update_info2 = std::get<ImmutableOutput>(immutable_variant);
    ASSERT_EQ(immutable_update_info2.tagged_keys["immutable_key2"], expected_tags);

    // test parent digest is not zeroed
    auto contains_data = false;
    for (auto i : block2_from_db.data.parent_digest) {
      if ((contains_data = (i != 0))) break;
    }
    ASSERT_TRUE(contains_data);

    // Get the merkle updates output of block2
    auto merkle_variant = block2_from_db.data.categories_updates_info["merkle"];
    auto merkle_update_info2 = std::get<BlockMerkleOutput>(merkle_variant);
    ASSERT_EQ(merkle_update_info2.keys.size(), 2);
    ASSERT_EQ(merkle_update_info2.keys.count("merkle_key1"), 1);
    ASSERT_EQ(merkle_update_info2.keys.count("merkle_key3"), 1);
    ASSERT_EQ(merkle_update_info2.keys["merkle_key1"].deleted, true);
    ASSERT_EQ(merkle_update_info2.keys.count("not_exist"), 0);
    ASSERT_EQ(merkle_update_info2.state_root_version, 2);

    auto ver_variant = block2_from_db.data.categories_updates_info["versioned"];
    auto ver_out2 = std::get<VersionedOutput>(ver_variant);
    ASSERT_EQ(ver_out2.keys.size(), 2);
    ASSERT_EQ(ver_out2.keys.count("ver_key3"), 1);
    ASSERT_EQ(ver_out2.keys.count("ver_key2"), 1);
    ASSERT_EQ(ver_out2.keys["ver_key3"].deleted, false);
    ASSERT_EQ(ver_out2.keys["ver_key2"].deleted, true);
    ASSERT_EQ(ver_out2.root_hash.has_value(), false);

    auto ver_variant_2 = block2_from_db.data.categories_updates_info["versioned_2"];
    auto ver_out3 = std::get<VersionedOutput>(ver_variant_2);
    ASSERT_EQ(ver_out3.keys.size(), 1);
    ASSERT_EQ(ver_out3.keys.count("ver_key4"), 1);
    ASSERT_EQ(ver_out3.keys["ver_key4"].deleted, false);
    // E.L --> some UT fails
    ASSERT_EQ(ver_out3.root_hash.has_value(), false);
  }
}

TEST_F(categorized_kvbc, delete_block) {
  KeyValueBlockchain block_chain{db, true};
  // Add block1
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addDelete("ver_deleted");
    updates.add("versioned_kv", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }
  // Can't delete only block
  ASSERT_THROW(block_chain.deleteBlock(1), std::logic_error);

  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key2", "ver_val2");
    ver_updates.addDelete("ver_deleted");
    updates.add("versioned", std::move(ver_updates));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  // Add block3
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "merkle_value3");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key3", "ver_val3");
    ver_updates.addDelete("ver_deleted");
    updates.add("versioned", std::move(ver_updates));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key3", {"immutable_val3", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)3);
  }

  // can't delete block in middle
  ASSERT_THROW(block_chain.deleteBlock(2), std::invalid_argument);

  // delete genesis
  ASSERT_TRUE(block_chain.deleteBlock(1));
  ASSERT_FALSE(block_chain.deleteBlock(1));
  ASSERT_EQ(block_chain.getGenesisBlockId(), 2);
  ASSERT_EQ(block_chain.getLastReachableBlockId(), 3);

  const detail::Buffer& block_db_key = Block::generateKey(1);
  auto block1_db_val = db->get(BLOCKS_CF, block_db_key);
  ASSERT_FALSE(block1_db_val.has_value());

  // Add block4
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key4", "merkle_value4");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key4", "ver_val4");
    ver_updates.addDelete("ver_deleted");
    updates.add("versioned", std::move(ver_updates));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key4", {"immutable_val4", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)4);
  }
  ASSERT_EQ(block_chain.getLastReachableBlockId(), 4);

  // delete last block
  {
    ASSERT_TRUE(block_chain.deleteBlock(4));
    const detail::Buffer& block4_db_key = Block::generateKey(4);
    auto block4_db_val = db->get(BLOCKS_CF, block4_db_key);
    ASSERT_FALSE(block4_db_val.has_value());

    // delete last block
    ASSERT_TRUE(block_chain.deleteBlock(3));
    const detail::Buffer& block3_db_key = Block::generateKey(3);
    auto block3_db_val = db->get(BLOCKS_CF, block3_db_key);
    ASSERT_FALSE(block3_db_val.has_value());

    // only block 2 left
    ASSERT_THROW(block_chain.deleteBlock(2), std::logic_error);
  }

  // call delete last block directly
  block_chain.deleteLastReachableBlock();
  ASSERT_EQ(block_chain.getLastReachableBlockId(), 1);
  ASSERT_EQ(block_chain.getGenesisBlockId(), 1);
  // Block chain is empty
  ASSERT_THROW(block_chain.deleteLastReachableBlock(), std::runtime_error);
}

TEST_F(categorized_kvbc, get_last_and_genesis_block) {
  KeyValueBlockchain block_chain{db, true};
  detail::Blockchain block_chain_imp{db};
  // Add block1
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key1", "key_val1");
    ver_updates.addDelete("ver_deleted");
    updates.add("ver", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }
  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key2", "key_val2");
    ver_updates.addDelete("ver_deleted");
    updates.add("ver", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }
  ASSERT_TRUE(block_chain_imp.loadLastReachableBlockId().has_value());
  ASSERT_EQ(block_chain_imp.loadLastReachableBlockId().value(), 2);
  ASSERT_TRUE(block_chain_imp.loadGenesisBlockId().has_value());
  ASSERT_EQ(block_chain_imp.loadGenesisBlockId().value(), 1);
}

TEST_F(categorized_kvbc, add_raw_block) {
  KeyValueBlockchain block_chain{db, true};
  ASSERT_FALSE(block_chain.getLastStatetransferBlockId().has_value());

  // Add block1
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key1", "key_val1");
    ver_updates.addDelete("ver_deleted");
    updates.add("versioned_kv", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
    ASSERT_EQ(block_chain.getGenesisBlockId(), 1);
  }
  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key2", "key_val2");
    ver_updates.addDelete("ver_deleted");
    updates.add("ver", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
    ASSERT_EQ(block_chain.getLastReachableBlockId(), 2);
  }

  // Add raw block to an invalid id
  {
    categorization::RawBlock rb;
    ASSERT_THROW(block_chain.addRawBlock(rb, 2), std::invalid_argument);
  }

  {
    categorization::RawBlock rb;
    ASSERT_THROW(block_chain.addRawBlock(rb, 1), std::invalid_argument);
  }

  {
    categorization::RawBlock rb;
    block_chain.addRawBlock(rb, 5);
    ASSERT_TRUE(block_chain.getLastStatetransferBlockId().has_value());
    ASSERT_EQ(block_chain.getLastStatetransferBlockId().value(), 5);

    ASSERT_EQ(block_chain.getLastReachableBlockId(), 2);
  }

  {
    categorization::RawBlock rb;
    block_chain.addRawBlock(rb, 4);
    ASSERT_TRUE(block_chain.getLastStatetransferBlockId().has_value());
    ASSERT_EQ(block_chain.getLastStatetransferBlockId().value(), 5);

    ASSERT_EQ(block_chain.getLastReachableBlockId(), 2);
  }

  {
    categorization::RawBlock rb;
    block_chain.addRawBlock(rb, 10);
    ASSERT_TRUE(block_chain.getLastStatetransferBlockId().has_value());
    ASSERT_EQ(block_chain.getLastStatetransferBlockId().value(), 10);

    ASSERT_EQ(block_chain.getLastReachableBlockId(), 2);
  }
}

TEST_F(categorized_kvbc, get_raw_block) {
  KeyValueBlockchain block_chain{db, true};

  ASSERT_FALSE(block_chain.getLastStatetransferBlockId().has_value());

  // Add block1
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key1", "key_val1");
    ver_updates.addDelete("ver_deleted");
    updates.add("versioned_kv", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
    ASSERT_EQ(block_chain.getGenesisBlockId(), 1);
  }

  {
    categorization::RawBlock rb;
    BlockMerkleInput merkle_updates;
    merkle_updates.kv["merkle_key3"] = "merkle_value3";
    merkle_updates.deletes.push_back("merkle_deleted3");
    rb.data.updates.kv["merkle"] = merkle_updates;
    block_chain.addRawBlock(rb, 5);
    ASSERT_TRUE(block_chain.getLastStatetransferBlockId().has_value());
    ASSERT_EQ(block_chain.getLastStatetransferBlockId().value(), 5);

    ASSERT_THROW(block_chain.getRawBlock(3), std::runtime_error);
    ASSERT_THROW(block_chain.getRawBlock(6), std::runtime_error);
  }

  {
    auto rb = block_chain.getRawBlock(5);
    ASSERT_EQ(std::get<BlockMerkleInput>(rb.data.updates.kv["merkle"]).kv["merkle_key3"], "merkle_value3");
  }

  // E.L not yet possible
  // {
  //   auto rb = block_chain.getRawBlock(1);
  //   ASSERT_EQ(std::get<BlockMerkleInput>(rb.data.category_updates["merkle"]).kv["merkle_key1"],
  //   "merkle_value1");
  // }

  { ASSERT_THROW(block_chain.getRawBlock(0), std::runtime_error); }
}

TEST_F(categorized_kvbc, link_state_transfer_chain) {
  KeyValueBlockchain block_chain{db, true};
  detail::Blockchain block_chain_imp{db};

  ASSERT_FALSE(block_chain_imp.loadLastReachableBlockId().has_value());
  ASSERT_EQ(block_chain.getLastReachableBlockId(), 0);
  // Add block1
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key1", "key_val1");
    ver_updates.addDelete("ver_deleted");
    updates.add("ver", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
    ASSERT_EQ(block_chain.getGenesisBlockId(), 1);
  }
  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key2", "key_val2");
    ver_updates.addDelete("ver_deleted");
    updates.add("versioned_kv", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
    ASSERT_EQ(block_chain.getLastReachableBlockId(), 2);
    ASSERT_EQ(block_chain_imp.loadLastReachableBlockId().value(), 2);
  }

  // Add raw block to an invalid id
  {
    categorization::RawBlock rb;
    ASSERT_THROW(block_chain.addRawBlock(rb, 1), std::invalid_argument);
  }

  // Add raw block with a gap form the last reachable
  {
    categorization::RawBlock rb;
    block_chain.addRawBlock(rb, 5);
    ASSERT_TRUE(block_chain.getLastStatetransferBlockId().has_value());
    ASSERT_EQ(block_chain.getLastStatetransferBlockId().value(), 5);

    ASSERT_EQ(block_chain.getLastReachableBlockId(), 2);
    ASSERT_EQ(block_chain_imp.loadLastReachableBlockId().value(), 2);
  }

  {
    categorization::RawBlock rb;
    block_chain.addRawBlock(rb, 4);
    ASSERT_TRUE(block_chain.getLastStatetransferBlockId().has_value());
    ASSERT_EQ(block_chain.getLastStatetransferBlockId().value(), 5);

    ASSERT_EQ(block_chain.getLastReachableBlockId(), 2);
    ASSERT_EQ(block_chain_imp.loadLastReachableBlockId().value(), 2);
  }

  {
    categorization::RawBlock rb;
    block_chain.addRawBlock(rb, 6);
    ASSERT_TRUE(block_chain.getLastStatetransferBlockId().has_value());
    ASSERT_EQ(block_chain.getLastStatetransferBlockId().value(), 6);

    ASSERT_EQ(block_chain.getLastReachableBlockId(), 2);
    ASSERT_EQ(block_chain_imp.loadLastReachableBlockId().value(), 2);
  }
  // Link the chains
  {
    categorization::RawBlock rb;
    BlockMerkleInput merkle_updates;
    merkle_updates.kv["merkle_key3"] = "merkle_value3";
    merkle_updates.deletes.push_back("merkle_deleted3");
    rb.data.updates.kv["merkle"] = merkle_updates;
    block_chain.addRawBlock(rb, 3);
    ASSERT_FALSE(block_chain.getLastStatetransferBlockId().has_value());

    ASSERT_EQ(block_chain.getLastReachableBlockId(), 6);
    ASSERT_EQ(block_chain_imp.loadLastReachableBlockId().value(), 6);
  }
}

TEST_F(categorized_kvbc, creation_of_category_type_cf) {
  KeyValueBlockchain block_chain{db, true};
  ASSERT_TRUE(db->hasColumnFamily(detail::CAT_ID_TYPE_CF));
}

TEST_F(categorized_kvbc, instantiation_of_categories) {
  auto wb = db->getBatch();
  db->createColumnFamily(detail::CAT_ID_TYPE_CF);
  wb.put(detail::CAT_ID_TYPE_CF,
         std::string("merkle"),
         std::string(1, static_cast<char>(detail::CATEGORY_TYPE::block_merkle)));
  wb.put(detail::CAT_ID_TYPE_CF,
         std::string("merkle2"),
         std::string(1, static_cast<char>(detail::CATEGORY_TYPE::block_merkle)));
  wb.put(
      detail::CAT_ID_TYPE_CF, std::string("imm"), std::string(1, static_cast<char>(detail::CATEGORY_TYPE::immutable)));
  wb.put(detail::CAT_ID_TYPE_CF,
         std::string("versioned_kv"),
         std::string(1, static_cast<char>(detail::CATEGORY_TYPE::versioned_kv)));

  db->write(std::move(wb));

  KeyValueBlockchain block_chain{db, true};
  KeyValueBlockchain::KeyValueBlockchain_tester tester;
  auto cats = tester.getCategories(block_chain);
  ASSERT_EQ(cats.count("merkle"), 1);
  ASSERT_THROW(std::get<detail::VersionedKeyValueCategory>(cats["merkle"]), std::bad_variant_access);
  ASSERT_NO_THROW(std::get<detail::BlockMerkleCategory>(cats["merkle"]));

  ASSERT_EQ(cats.count("merkle2"), 1);
  ASSERT_THROW(std::get<detail::VersionedKeyValueCategory>(cats["merkle2"]), std::bad_variant_access);
  ASSERT_NO_THROW(std::get<detail::BlockMerkleCategory>(cats["merkle2"]));

  ASSERT_EQ(cats.count("imm"), 1);
  ASSERT_THROW(std::get<detail::VersionedKeyValueCategory>(cats["imm"]), std::bad_variant_access);
  ASSERT_NO_THROW(std::get<detail::ImmutableKeyValueCategory>(cats["imm"]));

  ASSERT_EQ(cats.count("versioned_kv"), 1);
  ASSERT_THROW(std::get<detail::BlockMerkleCategory>(cats["versioned_kv"]), std::bad_variant_access);
  ASSERT_NO_THROW(std::get<detail::VersionedKeyValueCategory>(cats["versioned_kv"]));
}

TEST_F(categorized_kvbc, throw_on_non_exist_category_type) {
  KeyValueBlockchain block_chain{db, true};
  auto wb = db->getBatch();
  wb.put(detail::CAT_ID_TYPE_CF,
         std::string("merkle"),
         std::string(1, static_cast<char>(detail::CATEGORY_TYPE::end_of_types)));
  db->write(std::move(wb));

  KeyValueBlockchain::KeyValueBlockchain_tester tester;
  ASSERT_THROW(tester.instantiateCategories(block_chain), std::runtime_error);
}

TEST_F(categorized_kvbc, throw_on_non_exist_category) {
  KeyValueBlockchain block_chain{db, true};
  KeyValueBlockchain::KeyValueBlockchain_tester tester;
  ASSERT_THROW(tester.getCategory("merkle", block_chain), std::runtime_error);
}

TEST_F(categorized_kvbc, get_category) {
  db->createColumnFamily(detail::CAT_ID_TYPE_CF);
  auto wb = db->getBatch();
  wb.put(detail::CAT_ID_TYPE_CF,
         std::string("merkle"),
         std::string(1, static_cast<char>(detail::CATEGORY_TYPE::block_merkle)));
  db->write(std::move(wb));
  KeyValueBlockchain block_chain{db, true};
  KeyValueBlockchain::KeyValueBlockchain_tester tester;
  ASSERT_NO_THROW(std::get<BlockMerkleCategory>(tester.getCategory("merkle", block_chain)));
}

TEST_F(categorized_kvbc, get_block_data) {
  KeyValueBlockchain block_chain{db, true};

  // Add block1
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key1", "merkle_value1");
    merkle_updates.addUpdate("merkle_key2", "merkle_value2");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(true);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }
  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "merkle_value3");
    merkle_updates.addDelete("merkle_key1");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key3", "ver_val3");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    VersionedUpdates ver_updates_2;
    ver_updates_2.addUpdate("ver_key4", "ver_val4");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  // try to get updates
  ASSERT_THROW(block_chain.getBlockData(889), std::runtime_error);
  ASSERT_THROW(block_chain.getBlockData(887), std::runtime_error);
  auto updates = block_chain.getBlockData(1);
  auto variant = updates.kv["versioned"];
  auto versioned_updates = std::get<VersionedInput>(variant);
  // check reconstruction of original kv
  ASSERT_TRUE(versioned_updates.calculate_root_hash);
  auto expected = ValueWithFlags{"ver_val2", true};
  ASSERT_EQ(versioned_updates.kv["ver_key2"], expected);
}

TEST_F(categorized_kvbc, validate_category_creation_on_add) {
  KeyValueBlockchain block_chain{db, true};
  ImmutableUpdates imm_up;
  imm_up.addUpdate("key", {"val", {"0", "1"}});
  Updates up;
  up.add("imm", std::move(imm_up));
  block_chain.addBlock(std::move(up));

  KeyValueBlockchain::KeyValueBlockchain_tester tester;
  auto cats = tester.getCategories(block_chain);
  ASSERT_EQ(cats.count("imm"), 1);
  ASSERT_THROW(std::get<detail::VersionedKeyValueCategory>(cats["imm"]), std::bad_variant_access);
  ASSERT_NO_THROW(std::get<detail::ImmutableKeyValueCategory>(cats["imm"]));

  auto type = db->get(detail::CAT_ID_TYPE_CF, std::string("imm"));
  ASSERT_TRUE(type.has_value());
  ASSERT_EQ(static_cast<detail::CATEGORY_TYPE>((*type)[0]), detail::CATEGORY_TYPE::immutable);
}

TEST_F(categorized_kvbc, compare_raw_blocks) {
  KeyValueBlockchain block_chain{db, true};
  ImmutableUpdates imm_up;
  imm_up.addUpdate("key", {"val", {"0", "1"}});
  Updates up;
  up.add("imm", std::move(imm_up));
  block_chain.addBlock(std::move(up));

  KeyValueBlockchain::KeyValueBlockchain_tester tester;
  auto last_raw = tester.getLastRawBlocked(block_chain);
  ASSERT_EQ(last_raw.first, 1);
  ASSERT_TRUE(last_raw.second.has_value());
  auto raw_from_api = block_chain.getRawBlock(1);
  auto last_raw_imm_data = std::get<ImmutableInput>(last_raw.second.value().updates.kv["imm"]);
  auto raw_from_api_imm_data = std::get<ImmutableInput>(raw_from_api.data.updates.kv["imm"]);
  std::vector<std::string> vec{"0", "1"};
  ASSERT_EQ(last_raw_imm_data.kv["key"].data, "val");
  ASSERT_EQ(last_raw_imm_data.kv["key"].tags, vec);
  ASSERT_EQ(raw_from_api_imm_data.kv["key"].data, "val");
  ASSERT_EQ(raw_from_api_imm_data.kv["key"].tags, vec);

  // ASSERT_EQ(raw_from_api.data, last_raw.second.value().data);
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
