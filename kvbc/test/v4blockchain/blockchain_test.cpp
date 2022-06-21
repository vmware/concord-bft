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
#include "v4blockchain/detail/blockchain.h"
#include "storage/test/storage_test_common.h"
#include "v4blockchain/detail/column_families.h"
#include <random>

using concord::storage::rocksdb::NativeClient;
using namespace concord::kvbc;
using namespace ::testing;

namespace {

class v4_blockchain : public Test {
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

TEST_F(v4_blockchain, creation) {
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }

  auto wb = db->getBatch();

  auto blockchain = v4blockchain::detail::Blockchain{db};
  {
    auto block = blockchain.getBlockData(420);
    ASSERT_FALSE(block.has_value());
  }

  auto versioned_cat = std::string("versioned");
  auto key = std::string("key");
  auto val = std::string("val");
  auto updates = categorization::Updates{};
  auto ver_updates = categorization::VersionedUpdates{};
  ver_updates.addUpdate("key", "val");
  updates.add(versioned_cat, std::move(ver_updates));

  auto id = blockchain.addBlock(updates, wb);
  ASSERT_EQ(id, 1);
  ASSERT_EQ(blockchain.from_storage, 1);
  db->write(std::move(wb));

  auto blockstr = blockchain.getBlockData(id);
  ASSERT_TRUE(blockstr.has_value());

  auto block = v4blockchain::detail::Block(*blockstr);
  const auto& emdig = block.parentDigest();
  ASSERT_EQ(emdig, empty_digest);

  const auto& v = block.getVersion();
  ASSERT_EQ(v, v4blockchain::detail::Block::BLOCK_VERSION);

  auto reconstruct_updates = block.getUpdates();
  auto input = reconstruct_updates.categoryUpdates();
  ASSERT_EQ(input.kv.count(versioned_cat), 1);
  auto reconstruct_ver_updates = std::get<categorization::VersionedInput>(input.kv[versioned_cat]);
  ASSERT_EQ(reconstruct_ver_updates.kv[key].data, val);
  ASSERT_EQ(reconstruct_ver_updates.kv[key].stale_on_update, false);
}

TEST_F(v4_blockchain, calculate_empty_digest) {
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }

  auto wb = db->getBatch();

  auto blockchain = v4blockchain::detail::Blockchain{db};

  {
    const auto& dig = blockchain.calculateBlockDigest(0);
    ASSERT_EQ(dig, empty_digest);
  }
}

TEST_F(v4_blockchain, basic_chain) {
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }

  auto blockchain = v4blockchain::detail::Blockchain{db};
  // First block
  {
    auto wb = db->getBatch();
    auto versioned_cat = std::string("versioned");
    auto key = std::string("key");
    auto val = std::string("val");
    auto updates = categorization::Updates{};
    auto ver_updates = categorization::VersionedUpdates{};
    ver_updates.addUpdate("key", "val");
    updates.add(versioned_cat, std::move(ver_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 1);
    ASSERT_EQ(blockchain.from_storage, 1);
    db->write(std::move(wb));
    blockchain.setBlockId(id);

    // Validate block from storage
    auto blockstr = blockchain.getBlockData(id);
    ASSERT_TRUE(blockstr.has_value());
    auto block = v4blockchain::detail::Block(*blockstr);
    const auto& emdig = block.parentDigest();
    ASSERT_EQ(emdig, empty_digest);
    const auto& v = block.getVersion();
    ASSERT_EQ(v, v4blockchain::detail::Block::BLOCK_VERSION);
    auto reconstruct_updates = block.getUpdates();
    auto input = reconstruct_updates.categoryUpdates();
    ASSERT_EQ(input.kv.count(versioned_cat), 1);
    auto reconstruct_ver_updates = std::get<categorization::VersionedInput>(input.kv[versioned_cat]);
    ASSERT_EQ(reconstruct_ver_updates.kv[key].data, val);
    ASSERT_EQ(reconstruct_ver_updates.kv[key].stale_on_update, false);
  }

  // Second
  {
    auto wb = db->getBatch();
    auto imm_cat = std::string("immuatables");
    auto immkey = std::string("immkey");
    auto immval = std::string("immval");
    auto updates = categorization::Updates{};
    auto imm_updates = categorization::ImmutableUpdates{};
    imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
    updates.add(imm_cat, std::move(imm_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 2);
    ASSERT_EQ(blockchain.from_storage, 1);
    ASSERT_EQ(blockchain.from_future, 1);
    db->write(std::move(wb));
    blockchain.setBlockId(id);

    // Get block from storage
    auto blockstr = blockchain.getBlockData(id);
    ASSERT_TRUE(blockstr.has_value());
    auto block = v4blockchain::detail::Block(*blockstr);
    const auto& dig = block.parentDigest();

    // compare again digest of parent from storage
    auto parent_blockstr = blockchain.getBlockData(id - 1);
    ASSERT_TRUE(parent_blockstr.has_value());
    auto parent_digest =
        v4blockchain::detail::Block::calculateDigest(id - 1, parent_blockstr->c_str(), parent_blockstr->size());
    ASSERT_EQ(dig, parent_digest);

    // Validate block from storage
    auto reconstruct_updates = block.getUpdates();
    auto input = reconstruct_updates.categoryUpdates();
    ASSERT_EQ(input.kv.count(imm_cat), 1);
    auto reconstruct_imm_updates = std::get<categorization::ImmutableInput>(input.kv[imm_cat]);
    ASSERT_EQ(reconstruct_imm_updates.kv[immkey].data, immval);
    std::vector<std::string> v = {"1", "2", "33"};
    ASSERT_EQ(reconstruct_imm_updates.kv[immkey].tags, v);
  }
}

TEST_F(v4_blockchain, adv_chain) {
  concord::util::digest::BlockDigest empty_digest;
  for (auto& d : empty_digest) {
    d = 0;
  }

  {
    auto blockchain = v4blockchain::detail::Blockchain{db};
    // First block
    {
      auto wb = db->getBatch();
      auto versioned_cat = std::string("versioned");
      auto key = std::string("key");
      auto val = std::string("val");
      auto updates = categorization::Updates{};
      auto ver_updates = categorization::VersionedUpdates{};
      ver_updates.addUpdate("key", "val");
      updates.add(versioned_cat, std::move(ver_updates));

      auto id = blockchain.addBlock(updates, wb);
      ASSERT_EQ(id, 1);
      db->write(std::move(wb));
      blockchain.setBlockId(id);

      // Validate block from storage
      auto blockstr = blockchain.getBlockData(id);
      ASSERT_TRUE(blockstr.has_value());
      auto block = v4blockchain::detail::Block(*blockstr);
      const auto& emdig = block.parentDigest();
      ASSERT_EQ(emdig, empty_digest);
      const auto& v = block.getVersion();
      ASSERT_EQ(v, v4blockchain::detail::Block::BLOCK_VERSION);
      auto reconstruct_updates = block.getUpdates();
      auto input = reconstruct_updates.categoryUpdates();
      ASSERT_EQ(input.kv.count(versioned_cat), 1);
      auto reconstruct_ver_updates = std::get<categorization::VersionedInput>(input.kv[versioned_cat]);
      ASSERT_EQ(reconstruct_ver_updates.kv[key].data, val);
      ASSERT_EQ(reconstruct_ver_updates.kv[key].stale_on_update, false);
    }

    // Second
    {
      auto wb = db->getBatch();
      auto imm_cat = std::string("immuatables");
      auto immkey = std::string("immkey");
      auto immval = std::string("immval");
      auto updates = categorization::Updates{};
      auto imm_updates = categorization::ImmutableUpdates{};
      imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
      updates.add(imm_cat, std::move(imm_updates));

      auto id = blockchain.addBlock(updates, wb);
      ASSERT_EQ(id, 2);
      ASSERT_EQ(blockchain.from_storage, 1);
      ASSERT_EQ(blockchain.from_future, 1);
      db->write(std::move(wb));
      blockchain.setBlockId(id);

      // Get block from storage
      auto blockstr = blockchain.getBlockData(id);
      ASSERT_TRUE(blockstr.has_value());
      auto block = v4blockchain::detail::Block(*blockstr);
      const auto& dig = block.parentDigest();

      // compare again digest of parent from storage
      auto parent_blockstr = blockchain.getBlockData(id - 1);
      ASSERT_TRUE(parent_blockstr.has_value());
      auto parent_digest =
          v4blockchain::detail::Block::calculateDigest(id - 1, parent_blockstr->c_str(), parent_blockstr->size());
      ASSERT_EQ(dig, parent_digest);

      // Validate block from storage
      auto reconstruct_updates = block.getUpdates();
      auto input = reconstruct_updates.categoryUpdates();
      ASSERT_EQ(input.kv.count(imm_cat), 1);
      auto reconstruct_imm_updates = std::get<categorization::ImmutableInput>(input.kv[imm_cat]);
      ASSERT_EQ(reconstruct_imm_updates.kv[immkey].data, immval);
      std::vector<std::string> v = {"1", "2", "33"};
      ASSERT_EQ(reconstruct_imm_updates.kv[immkey].tags, v);
    }
  }

  // load blockchain from storage
  {
    auto blockchain = v4blockchain::detail::Blockchain{db};
    ASSERT_EQ(blockchain.getLastReachable(), 2);
    auto wb = db->getBatch();

    auto imm_cat = std::string("immuatables");
    auto immkey = std::string("immkey");
    auto immval = std::string("immval");
    auto updates = categorization::Updates{};
    auto imm_updates = categorization::ImmutableUpdates{};
    imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
    updates.add(imm_cat, std::move(imm_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 3);
    ASSERT_EQ(blockchain.from_storage, 1);
    ASSERT_EQ(blockchain.from_future, 0);
    db->write(std::move(wb));
    blockchain.setBlockId(id);
  }

  // load blockchain from storage
  {
    auto blockchain = v4blockchain::detail::Blockchain{db};
    ASSERT_EQ(blockchain.getLastReachable(), 3);

    {
      auto wb = db->getBatch();

      auto imm_cat = std::string("immuatables");
      auto immkey = std::string("immkey");
      auto immval = std::string("immval");
      auto updates = categorization::Updates{};
      auto imm_updates = categorization::ImmutableUpdates{};
      imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
      updates.add(imm_cat, std::move(imm_updates));

      auto id = blockchain.addBlock(updates, wb);
      ASSERT_EQ(id, 4);
      ASSERT_EQ(blockchain.from_storage, 1);
      ASSERT_EQ(blockchain.from_future, 0);
      db->write(std::move(wb));
      blockchain.setBlockId(id);
    }
    {
      auto wb = db->getBatch();

      auto imm_cat = std::string("immuatables");
      auto immkey = std::string("immkey");
      auto immval = std::string("immval");
      auto updates = categorization::Updates{};
      auto imm_updates = categorization::ImmutableUpdates{};
      imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
      updates.add(imm_cat, std::move(imm_updates));

      auto id = blockchain.addBlock(updates, wb);
      ASSERT_EQ(id, 5);
      ASSERT_EQ(blockchain.from_storage, 1);
      ASSERT_EQ(blockchain.from_future, 1);

      db->write(std::move(wb));
      blockchain.setBlockId(id);

      // Get block from storage
      auto blockstr = blockchain.getBlockData(5);
      ASSERT_TRUE(blockstr.has_value());
      auto block = v4blockchain::detail::Block(*blockstr);
      const auto& dig = block.parentDigest();

      // compare against digest of parent from storage
      auto parent_blockstr = blockchain.getBlockData(5 - 1);
      ASSERT_TRUE(parent_blockstr.has_value());
      auto parent_digest =
          v4blockchain::detail::Block::calculateDigest(id - 1, parent_blockstr->c_str(), parent_blockstr->size());
      ASSERT_EQ(dig, parent_digest);
    }
  }

  // load blockchain from storage and check all keys in multiget
  {
    auto blockchain = v4blockchain::detail::Blockchain{db};
    std::vector<BlockId> block_ids{1, 2, 3, 4, 5};
    std::unordered_map<BlockId, std::optional<std::string>> values;
    ASSERT_NO_THROW(blockchain.multiGetBlockData(block_ids, values));
    ASSERT_EQ(block_ids.size(), values.size());
    for (const auto& bid : block_ids) {
      const auto valit = values.find(bid);
      ASSERT_NE(valit, values.cend());
      ASSERT_TRUE((valit->second).has_value());
    }
    std::unordered_map<BlockId, std::optional<categorization::Updates>> updates;
    ASSERT_NO_THROW(blockchain.multiGetBlockUpdates(block_ids, updates));
    ASSERT_EQ(block_ids.size(), updates.size());
    for (const auto& bid : block_ids) {
      const auto valit = updates.find(bid);
      ASSERT_NE(valit, updates.cend());
      ASSERT_TRUE((valit->second).has_value());
    }
  }

  // load blockchain from storage and check all keys in multiget for duplicates
  {
    auto blockchain = v4blockchain::detail::Blockchain{db};
    std::vector<BlockId> block_ids{1, 2, 3, 3, 4, 5};
    std::unordered_map<BlockId, std::optional<std::string>> values;
    ASSERT_THROW(blockchain.multiGetBlockData(block_ids, values), std::logic_error);
    std::unordered_map<BlockId, std::optional<categorization::Updates>> updates;
    blockchain.multiGetBlockUpdates(block_ids, updates);
    ASSERT_GT(block_ids.size(), updates.size());
    for (const auto& bid : block_ids) {
      const auto valit = updates.find(bid);
      ASSERT_NE(valit, updates.cend());
      ASSERT_TRUE((valit->second).has_value());
    }
  }

  // load blockchain from storage and check all keys in multiget for unknown values
  {
    auto blockchain = v4blockchain::detail::Blockchain{db};
    std::vector<BlockId> block_ids{1, 2, 3, 4, 5, 6, 7, 8};
    std::unordered_map<BlockId, std::optional<std::string>> values;
    ASSERT_NO_THROW(blockchain.multiGetBlockData(block_ids, values));
    ASSERT_EQ(block_ids.size(), values.size());
    for (const auto& bid : block_ids) {
      const auto valit = values.find(bid);
      ASSERT_NE(valit, values.cend());
      if (bid > blockchain.getLastReachable()) {
        ASSERT_FALSE((valit->second).has_value());
      } else {
        ASSERT_TRUE((valit->second).has_value());
      }
    }
    std::unordered_map<BlockId, std::optional<categorization::Updates>> updates;
    ASSERT_NO_THROW(blockchain.multiGetBlockUpdates(block_ids, updates));
    ASSERT_EQ(block_ids.size(), updates.size());
    for (const auto& bid : block_ids) {
      const auto valit = updates.find(bid);
      ASSERT_NE(valit, updates.cend());
      if (bid > blockchain.getLastReachable()) {
        ASSERT_FALSE((valit->second).has_value());
      } else {
        ASSERT_TRUE((valit->second).has_value());
      }
    }
  }
  // load blockchain from storage and check all keys in multiget for duplicate unknown values
  {
    auto blockchain = v4blockchain::detail::Blockchain{db};
    std::vector<BlockId> block_ids{1, 2, 3, 4, 5, 6, 6, 7, 8};
    std::unordered_map<BlockId, std::optional<std::string>> values;
    ASSERT_THROW(blockchain.multiGetBlockData(block_ids, values), std::logic_error);
    std::unordered_map<BlockId, std::optional<categorization::Updates>> updates;
    blockchain.multiGetBlockUpdates(block_ids, updates);
    ASSERT_GT(block_ids.size(), updates.size());
    for (const auto& bid : block_ids) {
      const auto valit = updates.find(bid);
      ASSERT_NE(valit, updates.cend());
      if (bid > blockchain.getLastReachable()) {
        ASSERT_FALSE((valit->second).has_value());
      } else {
        ASSERT_TRUE((valit->second).has_value());
      }
    }
  }
}

TEST_F(v4_blockchain, delete_until) {
  auto blockchain = v4blockchain::detail::Blockchain{db};
  // Can't delete from empty blockchain
  ASSERT_DEATH(blockchain.deleteBlocksUntil(1), "");
  // block 1
  {
    auto wb = db->getBatch();
    auto versioned_cat = std::string("versioned");
    auto key = std::string("key");
    auto val = std::string("val");
    auto updates = categorization::Updates{};
    auto ver_updates = categorization::VersionedUpdates{};
    ver_updates.addUpdate("key", "val");
    updates.add(versioned_cat, std::move(ver_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 1);
    db->write(std::move(wb));
    blockchain.setBlockId(id);
  }

  // 2
  {
    auto wb = db->getBatch();
    auto imm_cat = std::string("immuatables");
    auto immkey = std::string("immkey");
    auto immval = std::string("immval");
    auto updates = categorization::Updates{};
    auto imm_updates = categorization::ImmutableUpdates{};
    imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
    updates.add(imm_cat, std::move(imm_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 2);
    db->write(std::move(wb));
    blockchain.setBlockId(id);
  }

  // 3
  {
    auto wb = db->getBatch();
    auto imm_cat = std::string("immuatables");
    auto immkey = std::string("immkey");
    auto immval = std::string("immval");
    auto updates = categorization::Updates{};
    auto imm_updates = categorization::ImmutableUpdates{};
    imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
    updates.add(imm_cat, std::move(imm_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 3);
    db->write(std::move(wb));
    blockchain.setBlockId(id);
  }

  // 4
  {
    auto wb = db->getBatch();
    auto imm_cat = std::string("immuatables");
    auto immkey = std::string("immkey");
    auto immval = std::string("immval");
    auto updates = categorization::Updates{};
    auto imm_updates = categorization::ImmutableUpdates{};
    imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
    updates.add(imm_cat, std::move(imm_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 4);
    db->write(std::move(wb));
    blockchain.setBlockId(id);
  }

  {
    uint64_t until = 3;
    auto id = blockchain.deleteBlocksUntil(until);
    ASSERT_EQ(id, until - 1);
    ASSERT_EQ(until, blockchain.getGenesisBlockId());
    auto block_2 = blockchain.getBlockData(2);
    ASSERT_FALSE(block_2.has_value());
    auto block_until = blockchain.getBlockData(until);
    ASSERT_TRUE(block_until.has_value());
  }

  {
    uint64_t until = 100;
    auto id = blockchain.deleteBlocksUntil(until);
    ASSERT_EQ(id, 3);
    ASSERT_EQ(blockchain.getLastReachable(), blockchain.getGenesisBlockId());
    auto block_3 = blockchain.getBlockData(3);
    ASSERT_FALSE(block_3.has_value());
    auto block_until = blockchain.getBlockData(4);
    ASSERT_TRUE(block_until.has_value());
  }

  {
    auto blockchain_local = v4blockchain::detail::Blockchain{db};
    ASSERT_EQ(blockchain_local.getLastReachable(), blockchain_local.getGenesisBlockId());
    ASSERT_EQ(blockchain_local.getLastReachable(), 4);
    auto id = blockchain_local.deleteBlocksUntil(5);
    // single block on the chain, no actuall deletion
    ASSERT_EQ(id, 3);
    // until is less than the genesis
    ASSERT_DEATH(blockchain_local.deleteBlocksUntil(1), "");
  }
}

TEST_F(v4_blockchain, auto_delete_until) {
  auto blockchain = v4blockchain::detail::Blockchain{db};
  // Can't delete from empty blockchain
  ASSERT_DEATH(blockchain.deleteBlocksUntil(1), "");

  std::random_device seed;
  std::mt19937 gen{seed()};                     // seed the generator
  std::uniform_int_distribution dist{20, 100};  // set min and max
  int numblocks = dist(gen);                    // generate number

  for (uint64_t id = 1; id <= (uint64_t)numblocks; ++id) {
    auto wb = db->getBatch();
    auto versioned_cat = std::string("versioned");
    auto key = std::string("key");
    auto val = std::string("val");
    auto updates = categorization::Updates{};
    auto ver_updates = categorization::VersionedUpdates{};
    ver_updates.addUpdate("key", "val");
    updates.add(versioned_cat, std::move(ver_updates));
    ASSERT_EQ(id, blockchain.addBlock(updates, wb));
    db->write(std::move(wb));
    blockchain.setBlockId(id);
  }
  auto curr_last = blockchain.getLastReachable();
  auto last_del = blockchain.deleteBlocksUntil(numblocks + 1);
  ASSERT_EQ(last_del, curr_last - 1);
  ASSERT_EQ(last_del + 1, blockchain.getGenesisBlockId());
  auto del_block = blockchain.getBlockData(last_del - 10);
  ASSERT_FALSE(del_block.has_value());
  auto gen_data = blockchain.getBlockData(blockchain.getGenesisBlockId());
  ASSERT_TRUE(gen_data.has_value());

  for (uint64_t id = curr_last + 1; id <= (uint64_t)numblocks + curr_last; ++id) {
    auto wb = db->getBatch();
    auto versioned_cat = std::string("versioned");
    auto key = std::string("key");
    auto val = std::string("val");
    auto updates = categorization::Updates{};
    auto ver_updates = categorization::VersionedUpdates{};
    ver_updates.addUpdate("key", "val");
    updates.add(versioned_cat, std::move(ver_updates));
    ASSERT_EQ(id, blockchain.addBlock(updates, wb));
    db->write(std::move(wb));
    blockchain.setBlockId(id);
  }

  curr_last = blockchain.getLastReachable();
  auto del_until = curr_last - 10;
  ASSERT_EQ(blockchain.deleteBlocksUntil(del_until), del_until - 1);
  ASSERT_EQ(del_until, blockchain.getGenesisBlockId());

  del_block = blockchain.getBlockData(del_until - 1);
  ASSERT_FALSE(del_block.has_value());
  gen_data = blockchain.getBlockData(blockchain.getGenesisBlockId());
  ASSERT_TRUE(gen_data.has_value());
}

TEST_F(v4_blockchain, delete_genesis) {
  auto blockchain = v4blockchain::detail::Blockchain{db};
  // Can't delete from empty blockchain
  ASSERT_DEATH(blockchain.deleteGenesisBlock(), "");
  // block 1
  {
    auto wb = db->getBatch();
    auto versioned_cat = std::string("versioned");
    auto key = std::string("key");
    auto val = std::string("val");
    auto updates = categorization::Updates{};
    auto ver_updates = categorization::VersionedUpdates{};
    ver_updates.addUpdate("key", "val");
    updates.add(versioned_cat, std::move(ver_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 1);
    db->write(std::move(wb));
    blockchain.setBlockId(id);
  }
  // Can't delete single block
  ASSERT_DEATH(blockchain.deleteGenesisBlock(), "");

  // 2
  {
    auto wb = db->getBatch();
    auto imm_cat = std::string("immuatables");
    auto immkey = std::string("immkey");
    auto immval = std::string("immval");
    auto updates = categorization::Updates{};
    auto imm_updates = categorization::ImmutableUpdates{};
    imm_updates.addUpdate("immkey", categorization::ImmutableUpdates::ImmutableValue{"immval", {"1", "2", "33"}});
    updates.add(imm_cat, std::move(imm_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 2);
    db->write(std::move(wb));
    blockchain.setBlockId(id);
  }

  blockchain.deleteGenesisBlock();
  ASSERT_EQ(blockchain.getGenesisBlockId(), 2);
  // Can't delete single block
  ASSERT_DEATH(blockchain.deleteGenesisBlock(), "");

  {
    auto blockchain_local = v4blockchain::detail::Blockchain{db};
    ASSERT_EQ(blockchain_local.getGenesisBlockId(), 2);
    ASSERT_DEATH(blockchain_local.deleteGenesisBlock(), "");
  }
}

TEST_F(v4_blockchain, block_updates) {
  auto blockchain = v4blockchain::detail::Blockchain{db};
  // First block
  {
    auto wb = db->getBatch();
    auto versioned_cat = std::string("versioned");
    auto key = std::string("key");
    auto val = std::string("val");
    auto updates = categorization::Updates{};
    auto ver_updates = categorization::VersionedUpdates{};
    ver_updates.addUpdate("key", "val");
    updates.add(versioned_cat, std::move(ver_updates));

    auto id = blockchain.addBlock(updates, wb);
    ASSERT_EQ(id, 1);
    ASSERT_EQ(blockchain.from_storage, 1);
    db->write(std::move(wb));
    blockchain.setBlockId(id);

    // Validate updates from storage
    auto blockstr3 = blockchain.getBlockData(3);
    ASSERT_FALSE(blockstr3.has_value());
    auto updates3 = blockchain.getBlockUpdates(3);
    ASSERT_FALSE(updates3.has_value());

    auto updates1 = blockchain.getBlockUpdates(id);
    ASSERT_TRUE(updates1.has_value());
    ASSERT_EQ(updates1->categoryUpdates(), updates.categoryUpdates());
  }
}

}  // end namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
