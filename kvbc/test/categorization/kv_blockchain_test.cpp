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
    db->createColumnFamily(BLOCKS_CF);
    db->createColumnFamily(ST_CHAIN_CF);
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
    // E.L need to add support for strings in cmf
    detail::Buffer in{block1_db_val.value().begin(), block1_db_val.value().end()};
    auto block1_from_db = Block::deserialize(in);
    auto merkle_variant = block1_from_db.data.categories_updates_info["merkle"];
    auto merkle_update_info1 = std::get<MerkleUpdatesInfo>(merkle_variant);
    ASSERT_EQ(merkle_update_info1.keys["merkle_deleted"].deleted, true);
    auto kv_hash_variant = block1_from_db.data.categories_updates_info["kv_hash"];
    auto kv_hash_update_info1 = std::get<KeyValueUpdatesInfo>(kv_hash_variant);
    ASSERT_EQ(kv_hash_update_info1.keys["kv_deleted"].deleted, true);
  }
  // get block 2 from DB and test it
  {
    const detail::Buffer& block_db_key = Block::generateKey(2);
    auto block2_db_val = db->get(BLOCKS_CF, block_db_key);
    ASSERT_TRUE(block2_db_val.has_value());
    // E.L need to add support for strings in cmf
    detail::Buffer in{block2_db_val.value().begin(), block2_db_val.value().end()};
    auto block2_from_db = Block::deserialize(in);
    const auto expected_tags = std::vector<std::string>{"1", "2"};
    auto immutable_variant = block2_from_db.data.categories_updates_info["immutable"];
    auto immutable_update_info2 = std::get<ImmutableUpdatesInfo>(immutable_variant);
    ASSERT_EQ(immutable_update_info2.tagged_keys["immutable_key2"], expected_tags);
  }
}

TEST_F(categorized_kvbc, delete_block) {
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
  // Can't delete only block
  ASSERT_THROW(block_chain.deleteBlock(1), std::logic_error);

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

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  // Add block3
  {
    Updates updates;
    MerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "merkle_value3");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    KeyValueUpdates keyval_updates;
    keyval_updates.addUpdate("kv_key3", "key_val3");
    keyval_updates.addDelete("kv_deleted");
    updates.add("kv_hash", std::move(keyval_updates));

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
    MerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key4", "merkle_value4");
    merkle_updates.addDelete("merkle_deleted");
    updates.add("merkle", std::move(merkle_updates));

    KeyValueUpdates keyval_updates;
    keyval_updates.addUpdate("kv_key4", "key_val4");
    keyval_updates.addDelete("kv_deleted");
    updates.add("kv_hash", std::move(keyval_updates));

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
  KeyValueBlockchain block_chain{db};
  detail::Blockchain block_chain_imp{db};
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
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }
  ASSERT_TRUE(block_chain_imp.loadLastReachableBlockId().has_value());
  ASSERT_EQ(block_chain_imp.loadLastReachableBlockId().value(), 2);
  ASSERT_TRUE(block_chain_imp.loadGenesisBlockId().has_value());
  ASSERT_EQ(block_chain_imp.loadGenesisBlockId().value(), 1);
}

TEST_F(categorized_kvbc, add_raw_block) {
  KeyValueBlockchain block_chain{db};
  ASSERT_FALSE(block_chain.getLastStatetransferBlockId().has_value());

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
    ASSERT_EQ(block_chain.getGenesisBlockId(), 1);
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
  KeyValueBlockchain block_chain{db};

  ASSERT_FALSE(block_chain.getLastStatetransferBlockId().has_value());

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
    ASSERT_EQ(block_chain.getGenesisBlockId(), 1);
  }

  {
    categorization::RawBlock rb;
    MerkleUpdatesData merkle_updates;
    merkle_updates.kv["merkle_key3"] = "merkle_value3";
    merkle_updates.deletes.push_back("merkle_deleted3");
    rb.data.category_updates["merkle"] = merkle_updates;
    block_chain.addRawBlock(rb, 5);
    ASSERT_TRUE(block_chain.getLastStatetransferBlockId().has_value());
    ASSERT_EQ(block_chain.getLastStatetransferBlockId().value(), 5);

    ASSERT_THROW(block_chain.getRawBlock(3), std::runtime_error);
    ASSERT_THROW(block_chain.getRawBlock(6), std::runtime_error);
  }

  {
    auto rb = block_chain.getRawBlock(5);
    ASSERT_EQ(std::get<MerkleUpdatesData>(rb.data.category_updates["merkle"]).kv["merkle_key3"], "merkle_value3");
  }

  // E.L not yet possible
  // {
  //   auto rb = block_chain.getRawBlock(1);
  //   ASSERT_EQ(std::get<MerkleUpdatesData>(rb.data.category_updates["merkle"]).kv["merkle_key1"], "merkle_value1");
  // }

  { ASSERT_THROW(block_chain.getRawBlock(0), std::runtime_error); }
}

TEST_F(categorized_kvbc, link_state_transfer_chain) {
  KeyValueBlockchain block_chain{db};
  detail::Blockchain block_chain_imp{db};

  ASSERT_FALSE(block_chain_imp.loadLastReachableBlockId().has_value());
  ASSERT_EQ(block_chain.getLastReachableBlockId(), 0);
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
    ASSERT_EQ(block_chain.getGenesisBlockId(), 1);
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
    MerkleUpdatesData merkle_updates;
    merkle_updates.kv["merkle_key3"] = "merkle_value3";
    merkle_updates.deletes.push_back("merkle_deleted3");
    rb.data.category_updates["merkle"] = merkle_updates;
    block_chain.addRawBlock(rb, 3);
    ASSERT_FALSE(block_chain.getLastStatetransferBlockId().has_value());

    ASSERT_EQ(block_chain.getLastReachableBlockId(), 6);
    ASSERT_EQ(block_chain_imp.loadLastReachableBlockId().value(), 6);
  }
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
