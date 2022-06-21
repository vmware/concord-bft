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
#include "rocksdb/native_client.h"

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
  void testGetValueAndVersion(const v4blockchain::detail::LatestKeys& latest_keys,
                              BlockId latest_block_id,
                              const std::string& category_id,
                              const std::string& key,
                              bool val_is_null_opt,
                              bool ver_is_null_opt,
                              std::vector<std::string>& keys,
                              std::vector<bool>& val_is_null_opts,
                              std::vector<bool>& ver_is_null_opts) {
    keys.push_back(key);
    val_is_null_opts.push_back(val_is_null_opt);
    ver_is_null_opts.push_back(ver_is_null_opt);
    auto value = latest_keys.getValue(category_id, key);
    ASSERT_EQ(value.has_value(), !val_is_null_opt);

    auto version = latest_keys.getLatestVersion(category_id, key);
    ASSERT_EQ(version.has_value(), !ver_is_null_opt);
    if (!ver_is_null_opt) {
      ASSERT_FALSE(version->deleted);
      ASSERT_LE(version->version, latest_block_id);
    }
  }

  void testMultiGetValueAndVersion(const v4blockchain::detail::LatestKeys& latest_keys,
                                   BlockId latest_block_id,
                                   const std::string& category_id,
                                   const std::vector<std::string>& keys,
                                   const std::vector<bool>& val_is_null_opts,
                                   const std::vector<bool>& ver_is_null_opts) {
    std::vector<std::optional<categorization::Value>> values;
    latest_keys.multiGetValue(category_id, keys, values);
    ASSERT_EQ(keys.size(), values.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      ASSERT_EQ((values[i]).has_value(), !val_is_null_opts[i]);
    }
    std::vector<std::optional<categorization::TaggedVersion>> versions;
    latest_keys.multiGetLatestVersion(category_id, keys, versions);
    ASSERT_EQ(keys.size(), versions.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      ASSERT_EQ((versions[i]).has_value(), !ver_is_null_opts[i]);
      if (!ver_is_null_opts[i]) {
        ASSERT_FALSE((versions[i])->deleted);
        ASSERT_LE((versions[i]->version), latest_block_id);
      }
    }
  }

 protected:
  std::map<std::string, categorization::CATEGORY_TYPE> categories;
  std::shared_ptr<NativeClient> db;
};

TEST_F(v4_kvbc, creation) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories};

  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::LATEST_KEYS_CF));
  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::IMMUTABLE_KEYS_CF));
  for (const auto& [k, v] : categories) {
    (void)v;
    ASSERT_TRUE(latest_keys.getCategoryPrefix(k).size() > 0);
  }
}

std::shared_ptr<rocksdb::Statistics> completeRocksDBConfiguration(
    ::rocksdb::Options& db_options, std::vector<::rocksdb::ColumnFamilyDescriptor>& cf_descs) {
  for (auto& d : cf_descs) {
    if ((d.name == concord::kvbc::v4blockchain::detail::LATEST_KEYS_CF) ||
        (d.name == concord::kvbc::v4blockchain::detail::IMMUTABLE_KEYS_CF)) {
      d.options.compaction_filter = concord::kvbc::v4blockchain::detail::LatestKeys::LKCompactionFilter::getFilter();
    }
  }
  return db_options.statistics;
}

TEST(column_family, set_filter) {
  {
    auto db = TestRocksDb::createNative();
    auto categories = std::map<std::string, categorization::CATEGORY_TYPE>{
        {"merkle", categorization::CATEGORY_TYPE::block_merkle},
        {"versioned", categorization::CATEGORY_TYPE::versioned_kv},
        {"versioned_2", categorization::CATEGORY_TYPE::versioned_kv},
        {"immutable", categorization::CATEGORY_TYPE::immutable}};
    v4blockchain::detail::LatestKeys latest_keys{db, categories};
    auto options = db->columnFamilyOptions(v4blockchain::detail::LATEST_KEYS_CF);
    ASSERT_NE(options.compaction_filter, nullptr);
    ASSERT_EQ(options.compaction_filter, v4blockchain::detail::LatestKeys::LKCompactionFilter::getFilter());
  }
  {
    std::string dbConfPath = "./rocksdb_config_test.ini";
    auto opts = concord::storage::rocksdb::NativeClient::UserOptions{
        dbConfPath, [](::rocksdb::Options& db_options, std::vector<::rocksdb::ColumnFamilyDescriptor>& cf_descs) {
          completeRocksDBConfiguration(db_options, cf_descs);
        }};
    auto db = TestRocksDb::createNative(opts);
    auto options = db->columnFamilyOptions(v4blockchain::detail::LATEST_KEYS_CF);
    ASSERT_NE(options.compaction_filter, nullptr);
    ASSERT_EQ(std::string(options.compaction_filter->Name()), std::string("LatestKeysCompactionFilter"));
  }
}

TEST_F(v4_kvbc, add_and_get_keys) {
  struct Expected {
    std::string type;
    std::string cat_id;
    std::string val;
    uint64_t version;
    v4blockchain::detail::LatestKeys::Flags flags;
    bool deleted;
  };
  v4blockchain::detail::LatestKeys::Flags no_flags = {0};
  v4blockchain::detail::LatestKeys latest_keys{db, categories};
  std::random_device seed;
  std::mt19937 gen{seed()};                   // seed the generator
  std::uniform_int_distribution dist{5, 20};  // set min and max
  uint64_t numblocks = (uint64_t)dist(gen);   // generate number

  std::map<std::string, Expected> expected_results;
  {
    for (uint64_t i = 0; i < numblocks; i++) {
      categorization::Updates updates;
      int numkeys = dist(gen);  // generate number
      categorization::BlockMerkleUpdates merkle_updates;
      std::string merkle_key_prefix = "merkle_key";
      std::string merkle_val_prefix = "merkle_val";
      for (int j = 0; j < numkeys; ++j) {
        std::string key = merkle_key_prefix + std::to_string(i) + std::to_string(j);
        std::string val = merkle_val_prefix + std::to_string(i) + std::to_string(j);
        merkle_updates.addUpdate(std::string(key), std::string(val));
        expected_results[key] = {"merkle", "merkle", val, i, no_flags, false};
      }
      // delete
      if (i > 0) {
        std::random_device seed;
        std::mt19937 gen{seed()};
        std::uniform_int_distribution dist{0, int(i - 1)};
        int block_idx = dist(gen);
        int key_idx = 3;
        std::string key = merkle_key_prefix + std::to_string(block_idx) + std::to_string(key_idx);
        merkle_updates.addDelete(std::string(key));
        expected_results[key] = {"merkle", "merkle", "", i, no_flags, true};
      }
      // update
      if (i > 0) {
        std::random_device seed;
        std::mt19937 gen{seed()};
        std::uniform_int_distribution dist{0, int(i - 1)};
        int block_idx = dist(gen);
        int key_idx = 4;
        std::string key = merkle_key_prefix + std::to_string(block_idx) + std::to_string(key_idx);
        std::string val = "updated_" + std::to_string(block_idx);
        merkle_updates.addUpdate(std::string(key), std::string(val));
        expected_results[key] = {"merkle", "merkle", val, i, no_flags, false};
      }
      updates.add("merkle", std::move(merkle_updates));

      categorization::VersionedUpdates ver_updates;
      std::string ver_key_prefix = "ver_key";
      std::string ver_val_prefix = "ver_val";
      for (int j = 0; j < numkeys; ++j) {
        std::string key = ver_key_prefix + std::to_string(i) + std::to_string(j);
        std::string val = ver_val_prefix + std::to_string(i) + std::to_string(j);
        v4blockchain::detail::LatestKeys::Flags fl =
            (j % 2 == 0) ? v4blockchain::detail::LatestKeys::STALE_ON_UPDATE : no_flags;
        auto ver_value = concord::kvbc::categorization::VersionedUpdates::Value{
            std::string(val), fl == v4blockchain::detail::LatestKeys::STALE_ON_UPDATE};
        ver_updates.addUpdate(std::string(key), std::move(ver_value));
        expected_results[key] = {"versioned", "versioned", val, i, fl, false};
      }

      if (i > 0) {
        std::random_device seed;
        std::mt19937 gen{seed()};
        std::uniform_int_distribution dist{0, int(i - 1)};
        int block_idx = dist(gen);
        int key_idx = 3;
        std::string key = ver_key_prefix + std::to_string(block_idx) + std::to_string(key_idx);
        ver_updates.addDelete(std::string(key));
        expected_results[key] = {"versioned", "versioned", "", i, no_flags, true};
      }

      // update
      if (i > 0) {
        std::random_device seed;
        std::mt19937 gen{seed()};
        std::uniform_int_distribution dist{0, int(i - 1)};
        int block_idx = dist(gen);
        int key_idx = 4;
        std::string key = ver_key_prefix + std::to_string(block_idx) + std::to_string(key_idx);
        std::string val = "updated_" + std::to_string(block_idx);
        v4blockchain::detail::LatestKeys::Flags fl =
            (block_idx % 2 == 0) ? v4blockchain::detail::LatestKeys::STALE_ON_UPDATE : no_flags;
        auto ver_value = concord::kvbc::categorization::VersionedUpdates::Value{
            std::string(val), fl == v4blockchain::detail::LatestKeys::STALE_ON_UPDATE};
        ver_updates.addUpdate(std::string(key), std::move(ver_value));
        expected_results[key] = {"versioned", "versioned", val, i, fl, false};
      }

      updates.add("versioned", std::move(ver_updates));

      categorization::ImmutableUpdates immutable_updates;
      std::string imm_key_prefix = "imm_key";
      std::string imm_val_prefix = "imm_val";
      for (int j = 0; j < numkeys; ++j) {
        std::string key = imm_key_prefix + std::to_string(i) + std::to_string(j);
        std::string val = imm_val_prefix + std::to_string(i) + std::to_string(j);
        immutable_updates.addUpdate(std::string(key), {std::string(val), {"1"}});
        expected_results[key] = {
            "immutable", "immutable", val, i, concord::kvbc::v4blockchain::detail::LatestKeys::STALE_ON_UPDATE, false};
      }
      updates.add("immutable", std::move(immutable_updates));

      auto wb = db->getBatch();
      latest_keys.addBlockKeys(updates, i, wb);
      db->write(std::move(wb));
    }

    for (const auto& [k, v] : expected_results) {
      auto opt_val = latest_keys.getValue(v.cat_id, k);
      auto opt_version = latest_keys.getLatestVersion(v.cat_id, k);
      if (v.deleted) {
        ASSERT_FALSE(opt_val.has_value());
        ASSERT_FALSE(opt_version.has_value());
        continue;
      }
      std::string val;
      BlockId id{};
      if (v.type == "merkle") {
        auto v = std::get<concord::kvbc::categorization::MerkleValue>(*opt_val);
        val = v.data;
        id = v.block_id;
      } else if (v.type == "versioned") {
        auto v = std::get<concord::kvbc::categorization::VersionedValue>(*opt_val);
        val = v.data;
        id = v.block_id;
      } else if (v.type == "immutable") {
        auto v = std::get<concord::kvbc::categorization::ImmutableValue>(*opt_val);
        val = v.data;
        id = v.block_id;
      } else {
        ASSERT_TRUE(false);
      }
      ASSERT_EQ(val, v.val);
      ASSERT_EQ(id, v.version);

      ASSERT_EQ(opt_version->version, v.version);
      ASSERT_FALSE(opt_version->deleted);

      auto cf = v.type == "immutable" ? v4blockchain::detail::IMMUTABLE_KEYS_CF : v4blockchain::detail::LATEST_KEYS_CF;
      auto db_val = db->get(cf, latest_keys.getCategoryPrefix(v.cat_id) + k);
      if (!db_val.has_value()) {
        ASSERT_TRUE(false);
      }
      if (v.type == "versioned") {
        auto flag = concord::kvbc::v4blockchain::detail::LatestKeys::isStaleOnUpdate(*db_val);
        auto comp_flag = v.flags == concord::kvbc::v4blockchain::detail::LatestKeys::STALE_ON_UPDATE;
        if (flag != comp_flag) {
          ASSERT_TRUE(false);
        }
      } else if (v.type == "immutable") {
        ASSERT_TRUE(concord::kvbc::v4blockchain::detail::LatestKeys::isStaleOnUpdate(*db_val));
      }
    }
  }
}

}  // end namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
