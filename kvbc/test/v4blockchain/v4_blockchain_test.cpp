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
#include "v4blockchain/detail/column_families.h"

using concord::storage::rocksdb::NativeClient;
using namespace concord::kvbc;
using namespace ::testing;

namespace {

class v4_kvbc : public Test {
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

// Add a block which contains updates per category.
// Each category handles its updates and returs an output which goes to the block structure.
// The block structure is then inserted into the DB.
// we test that the block that is written to DB contains the expected data.
TEST_F(v4_kvbc, creation) {
  v4blockchain::KeyValueBlockchain block_chain{
      db,
      true,
      std::map<std::string, categorization::CATEGORY_TYPE>{{"merkle", categorization::CATEGORY_TYPE::block_merkle},
                                                           {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
                                                           {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
                                                           {"immutable", categorization::CATEGORY_TYPE::immutable}}};

  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::BLOCKS_CF));
  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::ST_CHAIN_CF));
  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::LATEST_KEYS_CF));
}

}  // end namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
