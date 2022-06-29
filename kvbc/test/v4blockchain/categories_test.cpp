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
#include "categorization/db_categories.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "v4blockchain/detail/categories.h"

using concord::storage::rocksdb::NativeClient;
using namespace concord::kvbc;
using namespace ::testing;

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

TEST_F(v4_kvbc, creation) {
  v4blockchain::detail::Categories category_mapping{db, categories};

  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::CATEGORIES_CF));

  for (const auto& [k, v] : categories) {
    ASSERT_EQ(category_mapping.categoryPrefix(k).size(), 1);
    ASSERT_EQ(category_mapping.categoryType(k), v);
  }
}

TEST_F(v4_kvbc, load) {
  std::unordered_map<std::string, std::string> init;
  std::unordered_map<std::string, std::string> load;
  {
    v4blockchain::detail::Categories category_mapping{db, categories};

    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::CATEGORIES_CF));
    init = category_mapping.prefixMap();
    for (const auto& [k, v] : categories) {
      ASSERT_EQ(category_mapping.categoryPrefix(k).size(), 1);
      ASSERT_EQ(category_mapping.categoryType(k), v);
    }
  }

  {
    v4blockchain::detail::Categories category_mapping{db, std::nullopt};
    load = category_mapping.prefixMap();
    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::CATEGORIES_CF));
    for (const auto& [k, v] : categories) {
      ASSERT_EQ(category_mapping.categoryPrefix(k).size(), 1);
      ASSERT_EQ(category_mapping.categoryType(k), v);
    }
  }
  ASSERT_EQ(init, load);
  ASSERT_EQ(init.size(), categories.size());
}

TEST_F(v4_kvbc, add_cat_on_load) {
  {
    v4blockchain::detail::Categories category_mapping{db, categories};

    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::CATEGORIES_CF));

    for (const auto& [k, v] : categories) {
      ASSERT_EQ(category_mapping.categoryPrefix(k).size(), 1);
      ASSERT_EQ(category_mapping.categoryType(k), v);
    }
  }

  {
    std::map<std::string, categorization::CATEGORY_TYPE> reload = categories;
    reload["imm2"] = categorization::CATEGORY_TYPE::immutable;
    v4blockchain::detail::Categories category_mapping{db, reload};

    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::CATEGORIES_CF));

    for (const auto& [k, v] : reload) {
      ASSERT_EQ(category_mapping.categoryPrefix(k).size(), 1);
      ASSERT_EQ(category_mapping.categoryType(k), v);
    }
  }
}

TEST_F(v4_kvbc, throw_if_not_exist) {
  {
    v4blockchain::detail::Categories category_mapping{db, categories};

    ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::CATEGORIES_CF));

    ASSERT_THROW(category_mapping.categoryPrefix("imm3").size(), std::runtime_error);
  }
}

}  // namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
