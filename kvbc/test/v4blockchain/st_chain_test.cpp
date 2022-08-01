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
#include "v4blockchain/detail/st_chain.h"

using concord::storage::rocksdb::NativeClient;
using namespace concord::kvbc;
using namespace ::testing;
using namespace concord;

namespace {

class v4_kvbc : public Test {
 protected:
  void SetUp() override {
    destroyDb();
    db = TestRocksDb::createNative();
    categories = std::map<std::string, categorization::CATEGORY_TYPE>{
        {"merkle", categorization::CATEGORY_TYPE::block_merkle},
        {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
        {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
        {"immutable", categorization::CATEGORY_TYPE::immutable}};
  }

  void TearDown() override { destroyDb(); }

  void destroyDb() {
    db.reset();
    ASSERT_EQ(0, db.use_count());
    cleanup();
  }

 protected:
  std::map<std::string, categorization::CATEGORY_TYPE> categories;
  std::shared_ptr<NativeClient> db;
};

TEST_F(v4_kvbc, blocks) {
  {
    auto st_chain = v4blockchain::detail::StChain{db};
    auto id1 = kvbc::BlockId{1};
    auto block1 = std::string{"block1"};
    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::ST_CHAIN_CF));
    ASSERT_FALSE(st_chain.hasBlock(id1));
    { st_chain.addBlock(id1, block1.c_str(), block1.size()); }

    ASSERT_TRUE(st_chain.hasBlock(id1));

    {
      auto write_batch = db->getBatch();
      st_chain.deleteBlock(id1, write_batch);
      db->write(std::move(write_batch));
    }
    ASSERT_FALSE(st_chain.hasBlock(id1));
  }
}

TEST_F(v4_kvbc, get_blocks) {
  {
    auto st_chain = v4blockchain::detail::StChain{db};
    auto id1 = kvbc::BlockId{1};
    auto block1_str = std::string{"block1"};
    auto block1 = v4blockchain::detail::Block(block1_str);
    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::ST_CHAIN_CF));
    ASSERT_FALSE(st_chain.getBlock(id1).has_value());
    ASSERT_FALSE(st_chain.getBlockData(id1).has_value());
    { st_chain.addBlock(id1, block1_str.c_str(), block1_str.size()); }
    auto opt_block = st_chain.getBlock(id1);
    ASSERT_TRUE(opt_block.has_value());
    ASSERT_EQ(opt_block->getBuffer(), block1.getBuffer());
    auto opt_block_data = st_chain.getBlockData(id1);
    ASSERT_TRUE(opt_block_data.has_value());
    ASSERT_EQ(*opt_block_data, block1_str);

    {
      auto write_batch = db->getBatch();
      st_chain.deleteBlock(id1, write_batch);
      db->write(std::move(write_batch));
    }

    ASSERT_FALSE(st_chain.getBlock(id1).has_value());
    ASSERT_FALSE(st_chain.getBlockData(id1).has_value());
  }
}

TEST_F(v4_kvbc, load_block_id) {
  {
    auto st_chain = v4blockchain::detail::StChain{db};
    ASSERT_EQ(st_chain.getLastBlockId(), 0);
  }
  auto id1 = kvbc::BlockId{1};
  auto id5 = kvbc::BlockId{5};
  auto block_buff = std::string{"block1"};
  {
    auto st_chain = v4blockchain::detail::StChain{db};
    auto block1 = std::string{"block1"};
    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::ST_CHAIN_CF));
    st_chain.addBlock(id1, block_buff.c_str(), block_buff.size());
  }

  {
    auto st_chain = v4blockchain::detail::StChain{db};
    ASSERT_EQ(st_chain.getLastBlockId(), id1);
    st_chain.addBlock(id5, block_buff.c_str(), block_buff.size());
  }

  {
    auto st_chain = v4blockchain::detail::StChain{db};
    ASSERT_EQ(st_chain.getLastBlockId(), id5);
  }
}

TEST_F(v4_kvbc, update_block_id) {
  auto id1 = kvbc::BlockId{1};
  auto id3 = kvbc::BlockId{3};
  auto id5 = kvbc::BlockId{5};
  auto id7 = kvbc::BlockId{7};
  auto block_buff = std::string{"block1"};
  {
    auto st_chain = v4blockchain::detail::StChain{db};
    auto block1 = std::string{"block1"};
    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::ST_CHAIN_CF));
    st_chain.addBlock(id1, block_buff.c_str(), block_buff.size());
  }
  // delete the single block
  {
    auto st_chain = v4blockchain::detail::StChain{db};
    ASSERT_EQ(st_chain.getLastBlockId(), id1);
    auto write_batch = db->getBatch();
    st_chain.deleteBlock(id1, write_batch);
    db->write(std::move(write_batch));
    st_chain.updateLastIdAfterDeletion(id1);
    ASSERT_EQ(st_chain.getLastBlockId(), 0);
  }

  // load an empty chain and add a new block.
  {
    auto st_chain = v4blockchain::detail::StChain{db};
    ASSERT_EQ(st_chain.getLastBlockId(), 0);
    auto block1 = std::string{"block1"};
    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::ST_CHAIN_CF));
    st_chain.addBlock(id1, block_buff.c_str(), block_buff.size());
  }

  {
    auto st_chain = v4blockchain::detail::StChain{db};
    ASSERT_EQ(st_chain.getLastBlockId(), id1);
    st_chain.addBlock(id5, block_buff.c_str(), block_buff.size());
    st_chain.updateLastIdAfterDeletion(id5);
    ASSERT_EQ(st_chain.getLastBlockId(), id5);
  }

  {
    auto st_chain = v4blockchain::detail::StChain{db};
    ASSERT_EQ(st_chain.getLastBlockId(), id5);
    st_chain.updateLastIdIfBigger(id3);
    ASSERT_EQ(st_chain.getLastBlockId(), id5);
    st_chain.updateLastIdIfBigger(id7);
    ASSERT_EQ(st_chain.getLastBlockId(), id7);
  }
  // load reads the actual idx from storage
  {
    auto st_chain = v4blockchain::detail::StChain{db};
    ASSERT_EQ(st_chain.getLastBlockId(), id5);
    st_chain.updateLastIdAfterDeletion(id3);
    ASSERT_EQ(st_chain.getLastBlockId(), id5);
    auto write_batch = db->getBatch();
    st_chain.deleteBlock(id5, write_batch);
    db->write(std::move(write_batch));
    st_chain.updateLastIdAfterDeletion(id5);
    ASSERT_EQ(st_chain.getLastBlockId(), id1);
  }
}

}  // namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
