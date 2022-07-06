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
#include "kvbc_adapter/v4blockchain/app_state_adapter.hpp"
#include "v4blockchain/v4_blockchain.h"
#include <memory>
#include "db_adapter_interface.h"

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
        {"concord_internal", categorization::CATEGORY_TYPE::versioned_kv},
        {"immutable", categorization::CATEGORY_TYPE::immutable}};
    bc = std::make_shared<v4blockchain::KeyValueBlockchain>(db, true, categories);
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
  std::shared_ptr<v4blockchain::KeyValueBlockchain> bc;
};

TEST_F(v4_kvbc, put_get_blocks) {
  auto state_adapter = concord::kvbc::adapter::v4blockchain::AppStateAdapter{bc};
  char buffer[1000];
  uint32_t out = 0;
  ASSERT_EQ(state_adapter.getLastBlockNum(), 0);
  // Add block to blockchain and validate get
  ASSERT_FALSE(state_adapter.hasBlock(1));
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

  ASSERT_EQ(bc->add(std::move(updates)), (BlockId)1);
  ASSERT_TRUE(state_adapter.hasBlock(1));
  ASSERT_EQ(state_adapter.getLastBlockNum(), 1);

  ASSERT_TRUE(state_adapter.getBlock(1, buffer, 1000, &out));

  // Add block to ST chain
  auto block = std::string("block");
  auto blockDiff = std::string("blockDiff");
  state_adapter.putBlock(8, block.c_str(), block.size(), false);
  ASSERT_TRUE(state_adapter.hasBlock(8));
  ASSERT_EQ(state_adapter.getLastBlockNum(), 8);

  // try to add existing st block with different content
  ASSERT_THROW(state_adapter.putBlock(8, blockDiff.c_str(), blockDiff.size(), false), std::runtime_error);
  // not found
  ASSERT_THROW(state_adapter.getBlock(2, buffer, 1000, &out), kvbc::NotFoundException);
  // buffer too small
  ASSERT_THROW(state_adapter.getBlock(8, buffer, 1, &out), std::runtime_error);
  ASSERT_TRUE(state_adapter.getBlock(8, buffer, 1000, &out));
  auto getBlock = std::string(buffer, out);
  ASSERT_EQ(getBlock, block);
}

}  // namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
