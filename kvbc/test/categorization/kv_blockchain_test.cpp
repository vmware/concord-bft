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
    updates.add("versioned", std::move(ver_updates));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"immutable_val1", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), 1);
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

    // TODO: add a test for delete last reachable for the BlockMerkleCategory

    ASSERT_FALSE(block_chain.getLatestVersion("versioned", "ver_key4"));
    ASSERT_FALSE(block_chain.getLatest("versioned", "ver_key4"));
    ASSERT_FALSE(block_chain.get("versioned", "ver_key4", 4));

    ASSERT_FALSE(block_chain.getLatestVersion("immutable", "immutable_key4"));
    ASSERT_FALSE(block_chain.getLatest("immutable", "immutable_key4"));
    ASSERT_FALSE(block_chain.get("immutable", "immutable_key4", 4));

    // delete last block
    ASSERT_TRUE(block_chain.deleteBlock(3));
    const detail::Buffer& block3_db_key = Block::generateKey(3);
    auto block3_db_val = db->get(BLOCKS_CF, block3_db_key);
    ASSERT_FALSE(block3_db_val.has_value());

    ASSERT_FALSE(block_chain.getLatestVersion("versioned", "ver_key3"));
    ASSERT_FALSE(block_chain.getLatest("versioned", "ver_key3"));
    ASSERT_FALSE(block_chain.get("versioned", "ver_key3", 3));

    ASSERT_FALSE(block_chain.getLatestVersion("immutable", "immutable_key3"));
    ASSERT_FALSE(block_chain.getLatest("immutable", "immutable_key3"));
    ASSERT_FALSE(block_chain.get("immutable", "immutable_key3", 3));

    // only block 2 left
    ASSERT_THROW(block_chain.deleteBlock(2), std::logic_error);
  }

  // Cannot delete the only block as last reachable.
  ASSERT_THROW(block_chain.deleteLastReachableBlock(), std::logic_error);

  ASSERT_EQ(block_chain.getLastReachableBlockId(), 2);
  ASSERT_EQ(block_chain.getGenesisBlockId(), 2);

  ASSERT_TRUE(block_chain.getLatestVersion("merkle", "merkle_key2"));
  ASSERT_TRUE(block_chain.getLatest("merkle", "merkle_key2"));
  ASSERT_TRUE(block_chain.get("merkle", "merkle_key2", 2));

  ASSERT_TRUE(block_chain.getLatestVersion("versioned", "ver_key2"));
  ASSERT_TRUE(block_chain.getLatest("versioned", "ver_key2"));
  ASSERT_TRUE(block_chain.get("versioned", "ver_key2", 2));

  ASSERT_TRUE(block_chain.getLatestVersion("immutable", "immutable_key2"));
  ASSERT_TRUE(block_chain.getLatest("immutable", "immutable_key2"));
  ASSERT_TRUE(block_chain.get("immutable", "immutable_key2", 2));

  // Block 1 has been deleted as genesis, expect its versioned and merkle data to still be there. Immutable data should
  // be deleted.
  {
    const auto latest_version = block_chain.getLatestVersion("versioned", "ver_key1");
    ASSERT_TRUE(latest_version);
    ASSERT_EQ(latest_version->version, 1);

    const auto latest_value = block_chain.getLatest("versioned", "ver_key1");
    ASSERT_TRUE(latest_value);
    ASSERT_EQ(std::get<VersionedValue>(*latest_value).block_id, 1);
    ASSERT_EQ(std::get<VersionedValue>(*latest_value).data, "ver_val1");

    const auto value = block_chain.get("versioned", "ver_key1", 1);
    ASSERT_TRUE(value);
    ASSERT_EQ(std::get<VersionedValue>(*value).block_id, 1);
    ASSERT_EQ(std::get<VersionedValue>(*value).data, "ver_val1");
  }
  {
    const auto latest_version = block_chain.getLatestVersion("merkle", "merkle_key1");
    ASSERT_TRUE(latest_version);
    ASSERT_EQ(latest_version->version, 1);

    const auto latest_value = block_chain.getLatest("merkle", "merkle_key1");
    ASSERT_TRUE(latest_value);
    ASSERT_EQ(std::get<MerkleValue>(*latest_value).block_id, 1);
    ASSERT_EQ(std::get<MerkleValue>(*latest_value).data, "merkle_value1");

    const auto value = block_chain.get("merkle", "merkle_key1", 1);
    ASSERT_TRUE(value);
    ASSERT_EQ(std::get<MerkleValue>(*value).block_id, 1);
    ASSERT_EQ(std::get<MerkleValue>(*value).data, "merkle_value1");
  }
  {
    ASSERT_FALSE(block_chain.getLatestVersion("immutable", "immutable_key1"));
    ASSERT_FALSE(block_chain.getLatest("immutable", "immutable_key1"));
    ASSERT_FALSE(block_chain.get("immutable", "immutable_key1", 1));
  }
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

    ASSERT_FALSE(block_chain.getRawBlock(3));
    ASSERT_FALSE(block_chain.getRawBlock(6));
  }

  {
    auto rb = block_chain.getRawBlock(5);
    ASSERT_TRUE(rb);
    ASSERT_EQ(std::get<BlockMerkleInput>(rb->data.updates.kv["merkle"]).kv["merkle_key3"], "merkle_value3");
  }

  // E.L not yet possible
  // {
  //   auto rb = block_chain.getRawBlock(1);
  //   ASSERT_EQ(std::get<BlockMerkleInput>(rb.data.category_updates["merkle"]).kv["merkle_key1"],
  //   "merkle_value1");
  // }

  { ASSERT_FALSE(block_chain.getRawBlock(0)); }
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
  wb.put(detail::CAT_ID_TYPE_CF, std::string("merkle"), std::string(1, static_cast<char>(CATEGORY_TYPE::block_merkle)));
  wb.put(
      detail::CAT_ID_TYPE_CF, std::string("merkle2"), std::string(1, static_cast<char>(CATEGORY_TYPE::block_merkle)));
  wb.put(detail::CAT_ID_TYPE_CF, std::string("imm"), std::string(1, static_cast<char>(CATEGORY_TYPE::immutable)));
  wb.put(detail::CAT_ID_TYPE_CF,
         std::string("versioned_kv"),
         std::string(1, static_cast<char>(CATEGORY_TYPE::versioned_kv)));

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
  wb.put(detail::CAT_ID_TYPE_CF, std::string("merkle"), std::string(1, static_cast<char>(CATEGORY_TYPE::end_of_types)));
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
  wb.put(detail::CAT_ID_TYPE_CF, std::string("merkle"), std::string(1, static_cast<char>(CATEGORY_TYPE::block_merkle)));
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
    Updates updates_copy = updates;

    ImmutableUpdates immutable_updates;
    immutable_updates.calculateRootHash(true);
    immutable_updates.addUpdate("immutable_key1", {"immutable_val1", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

    auto up_before_move = updates;

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
    auto reconstructed_updates = block_chain.getBlockUpdates(1);
    ASSERT_TRUE(reconstructed_updates);
    ASSERT_EQ(up_before_move, *reconstructed_updates);
  }
  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "merkle_value3");
    merkle_updates.addDelete("merkle_key1");
    merkle_updates.addUpdate("merkle_key2", "update");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key3", "ver_val3");
    ver_updates.addUpdate("ver_key1", "update");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    VersionedUpdates ver_updates_2;
    ver_updates_2.calculateRootHash(true);
    ver_updates_2.addUpdate("ver_key4", "ver_val4");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.calculateRootHash(false);
    immutable_updates.addUpdate("immutable_key1", {"ddd", {"1", "2"}});
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

    auto up_before_move = updates;
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
    auto reconstructed_updates = block_chain.getBlockUpdates(2);
    ASSERT_TRUE(reconstructed_updates);
    ASSERT_EQ(up_before_move, *reconstructed_updates);
  }

  // try to get updates
  ASSERT_FALSE(block_chain.getBlockUpdates(889));
  ASSERT_FALSE(block_chain.getBlockUpdates(887));
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
  ASSERT_EQ(static_cast<CATEGORY_TYPE>((*type)[0]), CATEGORY_TYPE::immutable);
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
  ASSERT_TRUE(raw_from_api);
  auto last_raw_imm_data = std::get<ImmutableInput>(last_raw.second.value().updates.kv["imm"]);
  auto raw_from_api_imm_data = std::get<ImmutableInput>(raw_from_api->data.updates.kv["imm"]);
  std::vector<std::string> vec{"0", "1"};
  ASSERT_EQ(last_raw_imm_data.kv["key"].data, "val");
  ASSERT_EQ(last_raw_imm_data.kv["key"].tags, vec);
  ASSERT_EQ(raw_from_api_imm_data.kv["key"].data, "val");
  ASSERT_EQ(raw_from_api_imm_data.kv["key"].tags, vec);

  // ASSERT_EQ(raw_from_api.data, last_raw.second.value().data);
}

TEST_F(categorized_kvbc, single_read_with_version) {
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

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"immutable_val1", {"1", "2"}});
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }

  ASSERT_NO_THROW(block_chain.get("non_exist", "key", 1));
  ASSERT_FALSE(block_chain.get("merkle", "non_exist", 1).has_value());
  ASSERT_FALSE(block_chain.get("versioned", "non_exist", 1).has_value());
  ASSERT_FALSE(block_chain.get("immutable", "non_exist", 1).has_value());
  auto ex1 = categorization::MerkleValue{{1, "merkle_value1"}};
  ASSERT_EQ(std::get<categorization::MerkleValue>(block_chain.get("merkle", "merkle_key1", 1).value()), ex1);
  auto ex2 = categorization::VersionedValue{{1, "ver_val1"}};
  ASSERT_EQ(std::get<categorization::VersionedValue>(block_chain.get("versioned", "ver_key1", 1).value()), ex2);
  auto ex_imm = categorization::ImmutableValue{{1, "immutable_val1"}};
  ASSERT_EQ(std::get<categorization::ImmutableValue>(block_chain.get("immutable", "immutable_key1", 1).value()),
            ex_imm);

  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "merkle_value3");
    merkle_updates.addDelete("merkle_key1");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key3", "ver_val3");
    ver_updates.addUpdate("ver_key1", "update");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    VersionedUpdates ver_updates_2;
    ver_updates_2.addUpdate("ver_key4", "ver_val4");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"update", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  // added at block 1 but requested version 2
  ASSERT_FALSE(block_chain.get("merkle", "merkle_key2", 2).has_value());

  // deleted
  // At version 1 key exists
  ASSERT_TRUE(block_chain.get("merkle", "merkle_key1", 1).has_value());
  ASSERT_EQ(std::get<categorization::MerkleValue>(block_chain.get("merkle", "merkle_key1", 1).value()), ex1);
  // At version 2 key deleted
  ASSERT_FALSE(block_chain.get("merkle", "merkle_key1", 2).has_value());
  // At version 1 key exists
  ASSERT_TRUE(block_chain.get("versioned", "ver_key2", 1).has_value());
  auto ex3 = categorization::VersionedValue{{1, "ver_val2"}};
  ASSERT_EQ(std::get<categorization::VersionedValue>(block_chain.get("versioned", "ver_key2", 1).value()), ex3);
  // At version 2 key deleted
  ASSERT_FALSE(block_chain.get("versioned", "ver_key2", 2).has_value());

  // update
  // at version 1 is the original value
  ASSERT_EQ(std::get<categorization::VersionedValue>(block_chain.get("versioned", "ver_key1", 1).value()), ex2);
  auto ex4 = categorization::VersionedValue{{2, "update"}};
  ASSERT_EQ(std::get<categorization::VersionedValue>(block_chain.get("versioned", "ver_key1", 2).value()), ex4);
  // Immutable for version 1 now does not exist
  ASSERT_FALSE(block_chain.get("immutable", "immutable_key1", 1).has_value());
  // Immutable for version 2
  auto ex_imm2 = categorization::ImmutableValue{{2, "update"}};
  ASSERT_EQ(std::get<categorization::ImmutableValue>(block_chain.get("immutable", "immutable_key1", 2).value()),
            ex_imm2);
}

TEST_F(categorized_kvbc, single_get_latest) {
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

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"immutable_val1", {"1", "2"}});
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }

  ASSERT_FALSE(block_chain.getLatest("merkle", "non_exist").has_value());
  ASSERT_FALSE(block_chain.getLatest("versioned", "non_exist").has_value());
  ASSERT_FALSE(block_chain.getLatest("immutable", "non_exist").has_value());
  auto ex1 = categorization::MerkleValue{{1, "merkle_value1"}};
  ASSERT_EQ(std::get<categorization::MerkleValue>(block_chain.getLatest("merkle", "merkle_key1").value()), ex1);
  auto ex2 = categorization::VersionedValue{{1, "ver_val1"}};
  ASSERT_EQ(std::get<categorization::VersionedValue>(block_chain.getLatest("versioned", "ver_key1").value()), ex2);
  auto ex_imm = categorization::ImmutableValue{{1, "immutable_val1"}};
  ASSERT_EQ(std::get<categorization::ImmutableValue>(block_chain.getLatest("immutable", "immutable_key1").value()),
            ex_imm);

  // Add block2
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "merkle_value3");
    merkle_updates.addDelete("merkle_key1");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key3", "ver_val3");
    ver_updates.addUpdate("ver_key1", "update");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    VersionedUpdates ver_updates_2;
    ver_updates_2.addUpdate("ver_key4", "ver_val4");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"update", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  // added at block 1
  auto ex_mer = categorization::MerkleValue{{1, "merkle_value2"}};
  ASSERT_EQ(std::get<categorization::MerkleValue>(block_chain.getLatest("merkle", "merkle_key2").value()), ex_mer);

  // deleted
  ASSERT_FALSE(block_chain.getLatest("merkle", "merkle_key1").has_value());
  ASSERT_FALSE(block_chain.getLatest("versioned", "ver_key2").has_value());

  // update
  auto ex4 = categorization::VersionedValue{{2, "update"}};
  ASSERT_EQ(std::get<categorization::VersionedValue>(block_chain.getLatest("versioned", "ver_key1").value()), ex4);

  // Immutable for version 2
  auto ex_imm2 = categorization::ImmutableValue{{2, "update"}};
  ASSERT_EQ(std::get<categorization::ImmutableValue>(block_chain.getLatest("immutable", "immutable_key1").value()),
            ex_imm2);
}

TEST_F(categorized_kvbc, multi_read_with_version) {
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

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"immutable_val1", {"1", "2"}});
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

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
    ver_updates.addUpdate("ver_key1", "update");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    VersionedUpdates ver_updates_2;
    ver_updates_2.addUpdate("ver_key4", "ver_val4");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"update", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  // Add block3
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "update");
    merkle_updates.addUpdate("merkle_key4", "merkle_value4");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates_2;
    ver_updates_2.addUpdate("ver_key4", "update");
    ver_updates_2.addUpdate("ver_key5", "ver_val6");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"update2", {"1", "2"}});
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)3);
  }

  // Merkle
  {
    std::vector<std::string> merkle_keys{
        "merkle_key1", "merkle_key2", "merkle_key3", "merkle_key4", "merkle_key3", "merkle_key1"};
    std::vector<BlockId> bad_size{3, 4, 2, 1};
    std::vector<std::optional<categorization::Value>> values;
    ASSERT_DEATH(block_chain.multiGet("merkle", merkle_keys, bad_size, values), "");
    // #0 -> merkle_value1 | #1 -> not exist | #2 -> merkle_value3 | #3 merkle_key4 -> not exist | #4 -> update | #5 ->
    // not exist(deleted)
    std::vector<BlockId> versions{1, 2, 2, 1, 3, 2};
    block_chain.multiGet("merkle", merkle_keys, versions, values);
    ASSERT_FALSE(values[1].has_value());
    ASSERT_FALSE(values[3].has_value());
    ASSERT_FALSE(values[5].has_value());
    auto ex0 = categorization::MerkleValue{{1, "merkle_value1"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[0].value()), ex0);
    auto ex1 = categorization::MerkleValue{{2, "merkle_value3"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[2].value()), ex1);
    auto ex2 = categorization::MerkleValue{{3, "update"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[4].value()), ex2);
  }
  // Versioned
  {
    std::vector<std::string> keys{
        "ver_key1", "ver_key1", "ver_key1", "ver_key2", "ver_key2", "ver_key3", "ver_key3", "dummy"};
    std::vector<BlockId> bad_size{3};
    std::vector<std::optional<categorization::Value>> values;
    ASSERT_DEATH(block_chain.multiGet("versioned", keys, bad_size, values), "");
    // #0 -> ver_val1 | #1 -> update | #2 -> not exist | #3-> ver_val2 | #4 -> not exist(deleted) |#5 -> ver_val3 |#6 ->
    // not exist | #7 -> not exist
    std::vector<BlockId> versions{1, 2, 3, 1, 2, 2, 12, 1};
    block_chain.multiGet("versioned", keys, versions, values);
    ASSERT_FALSE(values[2].has_value());
    ASSERT_FALSE(values[4].has_value());
    ASSERT_FALSE(values[6].has_value());
    ASSERT_FALSE(values[7].has_value());
    auto ex0 = categorization::VersionedValue{{1, "ver_val1"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[0].value()), ex0);
    auto ex1 = categorization::VersionedValue{{2, "update"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[1].value()), ex1);
    auto ex2 = categorization::VersionedValue{{1, "ver_val2"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[3].value()), ex2);
    auto ex3 = categorization::VersionedValue{{2, "ver_val3"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[5].value()), ex3);
  }

  // Versioned_2
  {
    std::vector<std::string> keys{"ver_key4", "ver_key5"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> ver_val4 | #1 -> ver_val6
    std::vector<BlockId> versions{2, 3};
    block_chain.multiGet("versioned_2", keys, versions, values);
    auto ex0 = categorization::VersionedValue{{2, "ver_val4"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[0].value()), ex0);
    auto ex1 = categorization::VersionedValue{{3, "ver_val6"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[1].value()), ex1);
  }

  // Immutable
  {
    std::vector<std::string> keys{
        "immutable_key1", "immutable_key1", "immutable_key1", "immutable_key2", "immutable_key2", "dummy"};
    std::vector<BlockId> bad_size{3};
    std::vector<std::optional<categorization::Value>> values;
    ASSERT_DEATH(block_chain.multiGet("immutable", keys, bad_size, values), "");
    // #0 -> not exist | #1 -> not exist | #2 -> update2 | #3->immutable_val2 | #4 -> not exist | #5 -> not exist
    std::vector<BlockId> versions{1, 2, 3, 3, 12, 3};
    block_chain.multiGet("immutable", keys, versions, values);
    ASSERT_FALSE(values[0].has_value());
    ASSERT_FALSE(values[1].has_value());
    ASSERT_FALSE(values[4].has_value());
    ASSERT_FALSE(values[5].has_value());
    auto ex0 = categorization::ImmutableValue{{3, "update2"}};
    ASSERT_EQ(std::get<categorization::ImmutableValue>(values[2].value()), ex0);
    auto ex1 = categorization::ImmutableValue{{3, "immutable_val2"}};
    ASSERT_EQ(std::get<categorization::ImmutableValue>(values[3].value()), ex1);
  }
}

TEST_F(categorized_kvbc, multi_read_latest) {
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

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"immutable_val1", {"1", "2"}});
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }

  // Merkle
  {
    std::vector<std::string> merkle_keys{"merkle_key1", "merkle_key2", "merkle_key3"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> merkle_value1 | #1 -> merkle_value2 | #2 -> not exist
    block_chain.multiGetLatest("merkle", merkle_keys, values);
    ASSERT_FALSE(values[2].has_value());
    auto ex0 = categorization::MerkleValue{{1, "merkle_value1"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[0].value()), ex0);
    auto ex1 = categorization::MerkleValue{{1, "merkle_value2"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[1].value()), ex1);
  }

  // Versioned
  {
    std::vector<std::string> keys{"ver_key1", "ver_key2", "ver_key3"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> ver_val1 | #1 -> ver_val2 | #2 -> not exist
    block_chain.multiGetLatest("versioned", keys, values);
    ASSERT_FALSE(values[2].has_value());
    auto ex0 = categorization::VersionedValue{{1, "ver_val1"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[0].value()), ex0);
    auto ex1 = categorization::VersionedValue{{1, "ver_val2"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[1].value()), ex1);
  }

  // Immutable
  {
    std::vector<std::string> keys{"immutable_key1", "immutable_key2", "dummy"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> immutable_val1 | #1 -> immutable_val2 | #2 -> not exist
    block_chain.multiGetLatest("immutable", keys, values);
    ASSERT_FALSE(values[2].has_value());
    auto ex0 = categorization::ImmutableValue{{1, "immutable_val1"}};
    ASSERT_EQ(std::get<categorization::ImmutableValue>(values[0].value()), ex0);
    auto ex1 = categorization::ImmutableValue{{1, "immutable_val2"}};
    ASSERT_EQ(std::get<categorization::ImmutableValue>(values[1].value()), ex1);
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
    ver_updates.addUpdate("ver_key1", "update");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    VersionedUpdates ver_updates_2;
    ver_updates_2.addUpdate("ver_key4", "ver_val4");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"update", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  // Merkle
  {
    std::vector<std::string> merkle_keys{"merkle_key1", "merkle_key2", "merkle_key3"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> not exist | #1 -> merkle_value2 | #2 -> merkle_value3
    block_chain.multiGetLatest("merkle", merkle_keys, values);
    ASSERT_FALSE(values[0].has_value());
    auto ex0 = categorization::MerkleValue{{1, "merkle_value2"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[1].value()), ex0);
    auto ex1 = categorization::MerkleValue{{2, "merkle_value3"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[2].value()), ex1);
  }

  // Versioned
  {
    std::vector<std::string> keys{"ver_key1", "ver_key2", "ver_key3"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> update | #1 -> not exist | #2 -> ver_val3
    block_chain.multiGetLatest("versioned", keys, values);
    ASSERT_FALSE(values[1].has_value());
    auto ex0 = categorization::VersionedValue{{2, "update"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[0].value()), ex0);
    auto ex1 = categorization::VersionedValue{{2, "ver_val3"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[2].value()), ex1);
  }

  // Immutable
  {
    std::vector<std::string> keys{"immutable_key1", "immutable_key2", "dummy"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> update | #1 -> immutable_val2 | #2 -> not exist
    block_chain.multiGetLatest("immutable", keys, values);
    ASSERT_FALSE(values[2].has_value());
    auto ex0 = categorization::ImmutableValue{{2, "update"}};
    ASSERT_EQ(std::get<categorization::ImmutableValue>(values[0].value()), ex0);
    auto ex1 = categorization::ImmutableValue{{1, "immutable_val2"}};
    ASSERT_EQ(std::get<categorization::ImmutableValue>(values[1].value()), ex1);
  }

  // Add block3
  {
    Updates updates;
    BlockMerkleUpdates merkle_updates;
    merkle_updates.addUpdate("merkle_key3", "update");
    merkle_updates.addUpdate("merkle_key4", "merkle_value4");
    updates.add("merkle", std::move(merkle_updates));

    VersionedUpdates ver_updates;
    ver_updates.addUpdate("ver_key4", "update");
    ver_updates.addDelete("ver_key1");
    updates.add("versioned", std::move(ver_updates));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"update2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)3);
  }

  // Merkle
  {
    std::vector<std::string> merkle_keys{"merkle_key1", "merkle_key2", "merkle_key3", "merkle_key4"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> not exist | #1 -> merkle_value2 | #2 -> update | #4 -> merkle_key4
    block_chain.multiGetLatest("merkle", merkle_keys, values);
    ASSERT_FALSE(values[0].has_value());
    auto ex0 = categorization::MerkleValue{{1, "merkle_value2"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[1].value()), ex0);
    auto ex1 = categorization::MerkleValue{{3, "update"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[2].value()), ex1);
    auto ex2 = categorization::MerkleValue{{3, "merkle_value4"}};
    ASSERT_EQ(std::get<categorization::MerkleValue>(values[3].value()), ex2);
  }

  // Versioned
  {
    std::vector<std::string> keys{"ver_key1", "ver_key2", "ver_key3", "ver_key4"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> not exist | #1 -> not exist | #2 -> ver_val3 | #3 -> update
    block_chain.multiGetLatest("versioned", keys, values);
    ASSERT_FALSE(values[0].has_value());
    ASSERT_FALSE(values[1].has_value());
    auto ex0 = categorization::VersionedValue{{2, "ver_val3"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[2].value()), ex0);
    auto ex1 = categorization::VersionedValue{{3, "update"}};
    ASSERT_EQ(std::get<categorization::VersionedValue>(values[3].value()), ex1);
  }

  // Immutable
  {
    std::vector<std::string> keys{"immutable_key1", "immutable_key2", "dummy"};
    std::vector<std::optional<categorization::Value>> values;
    // #0 -> update2 | #1 -> immutable_val2 | #2 -> not exist
    block_chain.multiGetLatest("immutable", keys, values);
    ASSERT_FALSE(values[2].has_value());
    auto ex0 = categorization::ImmutableValue{{3, "update2"}};
    ASSERT_EQ(std::get<categorization::ImmutableValue>(values[0].value()), ex0);
    auto ex1 = categorization::ImmutableValue{{1, "immutable_val2"}};
    ASSERT_EQ(std::get<categorization::ImmutableValue>(values[1].value()), ex1);
  }
}

TEST_F(categorized_kvbc, single_get_latest_version) {
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

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"immutable_val1", {"1", "2"}});
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }

  // Merkle
  {
    ASSERT_FALSE(block_chain.getLatestVersion("merkle", "dummy").has_value());
    ASSERT_FALSE(block_chain.getLatestVersion("merkle", "merkle_key1").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("merkle", "merkle_key1").value().version, 1);
  }

  // Versioned
  {
    ASSERT_FALSE(block_chain.getLatestVersion("versioned", "dummy").has_value());
    ASSERT_FALSE(block_chain.getLatestVersion("versioned", "ver_key1").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("versioned", "ver_key1").value().version, 1);
  }

  // Immutable
  {
    ASSERT_FALSE(block_chain.getLatestVersion("immutable", "dummy").has_value());
    ASSERT_FALSE(block_chain.getLatestVersion("immutable", "immutable_key1").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("immutable", "immutable_key1").value().version, 1);
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
    ver_updates.addUpdate("ver_key1", "update");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    VersionedUpdates ver_updates_2;
    ver_updates_2.addUpdate("ver_key4", "ver_val4");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"update", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  // Merkle
  {
    ASSERT_TRUE(block_chain.getLatestVersion("merkle", "merkle_key1").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("merkle", "merkle_key1").value().version, 2);

    ASSERT_FALSE(block_chain.getLatestVersion("merkle", "merkle_key3").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("merkle", "merkle_key3").value().version, 2);

    ASSERT_FALSE(block_chain.getLatestVersion("merkle", "merkle_key2").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("merkle", "merkle_key2").value().version, 1);
  }

  // Versioned
  {
    ASSERT_TRUE(block_chain.getLatestVersion("versioned", "ver_key2").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("versioned", "ver_key2").value().version, 2);

    ASSERT_FALSE(block_chain.getLatestVersion("versioned", "ver_key1").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("versioned", "ver_key1").value().version, 2);

    ASSERT_FALSE(block_chain.getLatestVersion("versioned", "ver_key3").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("versioned", "ver_key3").value().version, 2);
  }

  // Immutable
  {
    ASSERT_FALSE(block_chain.getLatestVersion("immutable", "immutable_key1").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("immutable", "immutable_key1").value().version, 2);

    ASSERT_FALSE(block_chain.getLatestVersion("immutable", "immutable_key2").value().deleted);
    ASSERT_EQ(block_chain.getLatestVersion("immutable", "immutable_key2").value().version, 1);
  }
}

TEST_F(categorized_kvbc, multi_get_latest_version) {
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
    ver_updates.addUpdate("ver_key0", "v");
    updates.add("versioned", std::move(ver_updates));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"immutable_val1", {"1", "2"}});
    immutable_updates.addUpdate("immutable_key2", {"immutable_val2", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));

    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)1);
  }

  // Merkle
  {
    std::vector<std::optional<categorization::TaggedVersion>> versions;
    std::vector<std::string> keys{"dummy", "merkle_key1"};
    block_chain.multiGetLatestVersion("merkle", keys, versions);
    ASSERT_FALSE(versions[0].has_value());
    ASSERT_EQ(versions[1].value().version, 1);
  }

  // Versioned
  {
    std::vector<std::optional<categorization::TaggedVersion>> versions;
    std::vector<std::string> keys{"dummy", "ver_key1"};
    block_chain.multiGetLatestVersion("versioned", keys, versions);
    ASSERT_FALSE(versions[0].has_value());
    ASSERT_EQ(versions[1].value().version, 1);
  }

  // Immutable
  {
    std::vector<std::optional<categorization::TaggedVersion>> versions;
    std::vector<std::string> keys{"dummy", "immutable_key1"};
    block_chain.multiGetLatestVersion("immutable", keys, versions);
    ASSERT_FALSE(versions[0].has_value());
    ASSERT_EQ(versions[1].value().version, 1);
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
    ver_updates.addUpdate("ver_key1", "update");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    VersionedUpdates ver_updates_2;
    ver_updates_2.addUpdate("ver_key4", "ver_val4");
    updates.add("versioned_2", std::move(ver_updates_2));

    ImmutableUpdates immutable_updates;
    immutable_updates.addUpdate("immutable_key1", {"update", {"1", "2"}});
    updates.add("immutable", std::move(immutable_updates));
    ASSERT_EQ(block_chain.addBlock(std::move(updates)), (BlockId)2);
  }

  // Merkle
  {
    std::vector<std::optional<categorization::TaggedVersion>> versions;
    std::vector<std::string> keys{"merkle_key1", "merkle_key3", "merkle_key2"};
    block_chain.multiGetLatestVersion("merkle", keys, versions);
    ASSERT_TRUE(versions[0].value().deleted);
    ASSERT_EQ(versions[0].value().version, 2);

    ASSERT_FALSE(versions[1].value().deleted);
    ASSERT_EQ(versions[1].value().version, 2);

    ASSERT_FALSE(versions[2].value().deleted);
    ASSERT_EQ(versions[2].value().version, 1);
  }

  // Versioned
  {
    std::vector<std::optional<categorization::TaggedVersion>> versions;
    std::vector<std::string> keys{"ver_key2", "ver_key1", "ver_key3", "ver_key0"};
    block_chain.multiGetLatestVersion("versioned", keys, versions);
    ASSERT_TRUE(versions[0].value().deleted);
    ASSERT_EQ(versions[0].value().version, 2);

    ASSERT_FALSE(versions[1].value().deleted);
    ASSERT_EQ(versions[1].value().version, 2);

    ASSERT_FALSE(versions[2].value().deleted);
    ASSERT_EQ(versions[2].value().version, 2);

    ASSERT_FALSE(versions[3].value().deleted);
    ASSERT_EQ(versions[3].value().version, 1);
  }

  // Immutable
  {
    std::vector<std::optional<categorization::TaggedVersion>> versions;
    std::vector<std::string> keys{"immutable_key1", "immutable_key2"};
    block_chain.multiGetLatestVersion("immutable", keys, versions);

    ASSERT_FALSE(versions[0].value().deleted);
    ASSERT_EQ(versions[0].value().version, 2);

    ASSERT_FALSE(versions[1].value().deleted);
    ASSERT_EQ(versions[1].value().version, 1);
  }
}

TEST_F(categorized_kvbc, updates_append_single_key_value_non_existent_category) {
  auto updates = Updates{};

  ASSERT_FALSE(updates.appendKeyValue<BlockMerkleUpdates>("non-existent", "k", "v"));
  ASSERT_FALSE(updates.appendKeyValue<VersionedUpdates>("non-existent", "k", VersionedUpdates::Value{"v", false}));
  ASSERT_FALSE(updates.appendKeyValue<ImmutableUpdates>(
      "non-existent", "k", ImmutableUpdates::ImmutableValue{"v", {"t1", "t2"}}));

  ASSERT_EQ(updates.size(), 0);
  ASSERT_TRUE(updates.empty());
}

TEST_F(categorized_kvbc, updates_append_single_key_value) {
  auto updates = Updates{};

  {
    auto merkle_updates = BlockMerkleUpdates{};
    merkle_updates.addUpdate("mk1", "mv1");
    updates.add("merkle", std::move(merkle_updates));
  }

  {
    auto ver_updates = VersionedUpdates{};
    ver_updates.addUpdate("vk1", "vv1");
    updates.add("versioned", std::move(ver_updates));
  }

  {
    auto immutable_updates = ImmutableUpdates{};
    immutable_updates.addUpdate("ik1", {"iv1", {"t1", "t2"}});
    updates.add("immutable", std::move(immutable_updates));
  }

  // Before appending single key-values.
  ASSERT_EQ(updates.size(), 3);
  ASSERT_FALSE(updates.empty());

  // Append single key-values.
  ASSERT_TRUE(updates.appendKeyValue<BlockMerkleUpdates>("merkle", "mk2", "mv2"));
  ASSERT_TRUE(updates.appendKeyValue<VersionedUpdates>("versioned", "vk2", VersionedUpdates::Value{"vv2", false}));
  ASSERT_TRUE(updates.appendKeyValue<ImmutableUpdates>(
      "immutable", "ik2", ImmutableUpdates::ImmutableValue{"iv2", {"t1", "t2"}}));

  // After appending single key-values.
  ASSERT_EQ(updates.size(), 6);
  ASSERT_FALSE(updates.empty());

  auto expected = Updates{};
  {
    auto merkle_updates = BlockMerkleUpdates{};
    merkle_updates.addUpdate("mk1", "mv1");
    merkle_updates.addUpdate("mk2", "mv2");
    expected.add("merkle", std::move(merkle_updates));

    auto ver_updates = VersionedUpdates{};
    ver_updates.addUpdate("vk1", "vv1");
    ver_updates.addUpdate("vk2", "vv2");
    expected.add("versioned", std::move(ver_updates));

    auto immutable_updates = ImmutableUpdates{};
    immutable_updates.addUpdate("ik1", {"iv1", {"t1", "t2"}});
    immutable_updates.addUpdate("ik2", {"iv2", {"t1", "t2"}});
    expected.add("immutable", std::move(immutable_updates));
  }

  ASSERT_EQ(updates, expected);
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
