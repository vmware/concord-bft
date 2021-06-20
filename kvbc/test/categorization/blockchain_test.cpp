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

TEST_F(categorized_kvbc, creation_of_state_transfter_blockchain_cf) {
  detail::Blockchain::StateTransfer st(db);
  ASSERT_TRUE(db->hasColumnFamily(detail::ST_CHAIN_CF));
}

TEST_F(categorized_kvbc, last_reachable_block) {
  detail::Blockchain block_chain{db};
  ASSERT_FALSE(block_chain.loadLastReachableBlockId().has_value());

  {
    Block first{1};
    auto wb = db->getBatch();
    block_chain.addBlock(first, wb);
    db->write(std::move(wb));
    ASSERT_EQ(block_chain.loadLastReachableBlockId().value(), 1);
  }

  {
    Block sec{300};
    auto wb = db->getBatch();
    block_chain.addBlock(sec, wb);
    db->write(std::move(wb));
    ASSERT_EQ(block_chain.loadLastReachableBlockId().value(), 300);
  }

  // delete non-existing block
  {
    auto wb = db->getBatch();
    block_chain.deleteBlock(200, wb);
    db->write(std::move(wb));
    ASSERT_EQ(block_chain.loadLastReachableBlockId().value(), 300);
  }

  // delete last and load
  {
    auto wb = db->getBatch();
    block_chain.deleteBlock(300, wb);
    db->write(std::move(wb));
    ASSERT_EQ(block_chain.loadLastReachableBlockId().value(), 1);
  }
}

TEST_F(categorized_kvbc, genesis_block) {
  detail::Blockchain block_chain{db};
  ASSERT_FALSE(block_chain.loadGenesisBlockId().has_value());

  {
    Block first{1};
    auto wb = db->getBatch();
    block_chain.addBlock(first, wb);
    db->write(std::move(wb));
    ASSERT_EQ(block_chain.loadGenesisBlockId().value(), 1);
  }

  {
    Block sec{300};
    auto wb = db->getBatch();
    block_chain.addBlock(sec, wb);
    db->write(std::move(wb));
    ASSERT_EQ(block_chain.loadGenesisBlockId().value(), 1);
  }

  // delete non-existing block
  {
    auto wb = db->getBatch();
    block_chain.deleteBlock(200, wb);
    db->write(std::move(wb));
    ASSERT_EQ(block_chain.loadGenesisBlockId().value(), 1);
  }

  // delete the genesis and load
  {
    auto wb = db->getBatch();
    block_chain.deleteBlock(1, wb);
    db->write(std::move(wb));
    ASSERT_EQ(block_chain.loadGenesisBlockId().value(), 300);
  }
}

TEST_F(categorized_kvbc, get_block) {
  detail::Blockchain block_chain{db};

  {
    auto block = block_chain.getBlock(100);
    ASSERT_FALSE(block.has_value());
  }

  {
    BlockDigest pHash;
    std::random_device rd;
    for (int i = 0; i < (int)pHash.size(); i++) {
      pHash[i] = (uint8_t)(rd() % 255);
    }

    auto key = std::string("key");

    uint64_t state_root_version = 886;
    BlockMerkleOutput mui;
    mui.keys[key] = MerkleKeyFlag{true};
    mui.root_hash = pHash;
    mui.state_root_version = state_root_version;

    Block block{888};
    block.add("merkle", std::move(mui));
    block.setParentHash(pHash);

    auto wb = db->getBatch();
    block_chain.addBlock(block, wb);
    db->write(std::move(wb));

    auto block_from_db = block_chain.getBlock(888);
    ASSERT_TRUE(block_from_db.has_value());
    auto merkle_variant = (*block_from_db).data.categories_updates_info["merkle"];
    ASSERT_TRUE(std::get<BlockMerkleOutput>(merkle_variant).keys[key].deleted);
  }
}

TEST_F(categorized_kvbc, fail_get_raw) {
  detail::Blockchain block_chain{db};
  CategoriesMap cats;
  auto rb = block_chain.getRawBlock(100, cats);
  ASSERT_FALSE(rb.has_value());
}

TEST_F(categorized_kvbc, load_and_delete_st_blocks) {
  detail::Blockchain::StateTransfer st_block_chain{db};

  ASSERT_FALSE(st_block_chain.getLastBlockId() > 0);

  // Add raw block 100
  {
    categorization::RawBlock rb;
    auto wb = db->getBatch();
    st_block_chain.addBlock(100, rb, wb);
    db->write(std::move(wb));
    st_block_chain.loadLastBlockId();
    ASSERT_EQ(st_block_chain.getLastBlockId(), 100);
  }
  // delete non-existing
  {
    auto wb = db->getBatch();
    st_block_chain.deleteBlock(101, wb);
    db->write(std::move(wb));
    st_block_chain.updateLastIdAfterDeletion(101);
    ASSERT_EQ(st_block_chain.getLastBlockId(), 100);
  }
  // Add raw block 300
  {
    categorization::RawBlock rb;
    auto wb = db->getBatch();
    st_block_chain.addBlock(300, rb, wb);
    db->write(std::move(wb));
    st_block_chain.loadLastBlockId();
    ASSERT_EQ(st_block_chain.getLastBlockId(), 300);
  }
  // Add raw block 200
  {
    categorization::RawBlock rb;
    auto wb = db->getBatch();
    st_block_chain.addBlock(200, rb, wb);
    db->write(std::move(wb));
    st_block_chain.loadLastBlockId();
    ASSERT_EQ(st_block_chain.getLastBlockId(), 300);
  }
  // Add raw block 1
  {
    categorization::RawBlock rb;
    auto wb = db->getBatch();
    st_block_chain.addBlock(1, rb, wb);
    db->write(std::move(wb));
    st_block_chain.loadLastBlockId();
    ASSERT_EQ(st_block_chain.getLastBlockId(), 300);
  }
  // delete raw block 200
  {
    auto wb = db->getBatch();
    st_block_chain.deleteBlock(200, wb);
    db->write(std::move(wb));
    st_block_chain.updateLastIdAfterDeletion(200);
    ASSERT_EQ(st_block_chain.getLastBlockId(), 300);
  }
  // delete raw block 300
  {
    auto wb = db->getBatch();
    st_block_chain.deleteBlock(300, wb);
    db->write(std::move(wb));
    st_block_chain.updateLastIdAfterDeletion(300);
    ASSERT_EQ(st_block_chain.getLastBlockId(), 100);
  }
  {
    auto wb = db->getBatch();
    st_block_chain.deleteBlock(100, wb);
    db->write(std::move(wb));
    st_block_chain.updateLastIdAfterDeletion(100);
    ASSERT_EQ(st_block_chain.getLastBlockId(), 1);
  }
  {
    auto wb = db->getBatch();
    st_block_chain.deleteBlock(1, wb);
    db->write(std::move(wb));
    st_block_chain.updateLastIdAfterDeletion(1);
    ASSERT_FALSE(st_block_chain.getLastBlockId() > 0);
  }
}

TEST_F(categorized_kvbc, get_st_block) {
  detail::Blockchain::StateTransfer st_block_chain{db};

  ASSERT_FALSE(st_block_chain.getLastBlockId() > 0);

  {
    categorization::RawBlock rb;
    std::array<uint8_t, 32> arr{'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a',
                                'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a'};
    rb.data.block_merkle_root_hash["merkle"] = arr;
    auto wb = db->getBatch();
    st_block_chain.addBlock(100, rb, wb);
    db->write(std::move(wb));
    auto db_rb = st_block_chain.getRawBlock(100);
    ASSERT_EQ(db_rb.value().data.block_merkle_root_hash["merkle"], arr);
  }
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
