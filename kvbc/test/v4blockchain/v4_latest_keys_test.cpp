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
    auto latest_version = concord::kvbc::v4blockchain::detail::Blockchain::generateKey(latest_block_id);
    keys.push_back(key);
    val_is_null_opts.push_back(val_is_null_opt);
    ver_is_null_opts.push_back(ver_is_null_opt);
    auto value = latest_keys.getValue(category_id, latest_version, key);
    ASSERT_EQ(value.has_value(), !val_is_null_opt);

    auto version = latest_keys.getLatestVersion(category_id, latest_version, key);
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
    auto latest_version = concord::kvbc::v4blockchain::detail::Blockchain::generateKey(latest_block_id);
    std::vector<std::optional<categorization::Value>> values;
    latest_keys.multiGetValue(category_id, latest_version, keys, values);
    ASSERT_EQ(keys.size(), values.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      ASSERT_EQ((values[i]).has_value(), !val_is_null_opts[i]);
    }
    std::vector<std::optional<categorization::TaggedVersion>> versions;
    latest_keys.multiGetLatestVersion(category_id, latest_version, keys, versions);
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
  v4blockchain::detail::LatestKeys latest_keys{db, categories, []() { return 1; }};

  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::LATEST_KEYS_CF));
  for (const auto& [k, v] : categories) {
    (void)v;
    ASSERT_TRUE(latest_keys.getCategoryPrefix(k).size() > 0);
  }
}

TEST_F(v4_kvbc, add_merkle_keys) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories, []() { return 1; }};
  std::vector<std::string> keys;
  std::vector<bool> val_is_null_opts;
  std::vector<bool> ver_is_null_opts;

  std::string no_flags = {0};
  {
    uint64_t timestamp = 40;
    auto timestamp_str = v4blockchain::detail::Blockchain::generateKey(timestamp);
    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    auto key1 = std::string("merkle_key1");
    auto val1 = std::string("merkle_value1");
    auto key2 = std::string("merkle_key2");
    auto val2 = std::string("merkle_value2");
    merkle_updates.addUpdate(std::string("merkle_key1"), std::string("merkle_value1"));
    merkle_updates.addUpdate(std::string("merkle_key2"), std::string("merkle_value2"));
    updates.add("merkle", std::move(merkle_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, timestamp, wb);
    db->write(std::move(wb));
    testGetValueAndVersion(
        latest_keys, timestamp, "merkle", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, timestamp, "merkle", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);

    std::string out_ts;
    // without category prefix
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF, key1, timestamp_str, &out_ts);
    ASSERT_FALSE(val.has_value());
    // with
    val = db->get(
        v4blockchain::detail::LATEST_KEYS_CF, latest_keys.getCategoryPrefix("merkle") + key1, timestamp_str, &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(*val, val1 + no_flags);
    ASSERT_EQ(out_ts, timestamp_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(timestamp, iout_ts);
  }

  {
    uint64_t prev_timestamp = 40;
    auto prev_timestamp_str = v4blockchain::detail::Blockchain::generateKey(prev_timestamp);
    uint64_t higher_timestamp = 400;
    auto higher_timestamp_str = v4blockchain::detail::Blockchain::generateKey(higher_timestamp);
    uint64_t lower_timestamp = 4;
    auto lower_timestamp_str = v4blockchain::detail::Blockchain::generateKey(lower_timestamp);
    uint64_t close_timestamp = 41;
    auto close_timestamp_str = v4blockchain::detail::Blockchain::generateKey(close_timestamp);
    auto prev_val1 = std::string("merkle_value1");
    auto prev_val2 = std::string("merkle_value2");
    uint64_t timestamp = 42;
    auto timestamp_str = v4blockchain::detail::Blockchain::generateKey(timestamp);

    categorization::Updates updates;
    categorization::BlockMerkleUpdates merkle_updates;
    // Update key
    auto key1 = std::string("merkle_key1");
    auto val1 = std::string("merkle_updated_value");
    merkle_updates.addUpdate(std::string("merkle_key1"), std::string("merkle_updated_value"));
    // delete key2 at timestmap 42
    auto key2 = std::string("merkle_key2");
    merkle_updates.addDelete(std::string("merkle_key2"));
    // new key
    auto key3 = std::string("merkle_key3");
    auto val3 = std::string("merkle_value3");
    merkle_updates.addUpdate(std::string("merkle_key3"), std::string("merkle_value3"));

    updates.add("merkle", std::move(merkle_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, timestamp, wb);
    db->write(std::move(wb));
    testGetValueAndVersion(
        latest_keys, timestamp, "merkle", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, timestamp, "merkle", key2, true, true, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(latest_keys, 40, "merkle", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, timestamp, "merkle", key3, false, false, keys, val_is_null_opts, ver_is_null_opts);

    std::string out_ts;
    //////////KEY1//////////////////////////////

    // without category prefix
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF, key1, timestamp_str, &out_ts);
    ASSERT_FALSE(val.has_value());

    // get key1 updated value of this timestamp
    val = db->get(
        v4blockchain::detail::LATEST_KEYS_CF, latest_keys.getCategoryPrefix("merkle") + key1, timestamp_str, &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(*val, val1 + no_flags);
    ASSERT_EQ(out_ts, timestamp_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(timestamp, iout_ts);
    // Get previous version
    out_ts.clear();
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  latest_keys.getCategoryPrefix("merkle") + key1,
                  prev_timestamp_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(*val, prev_val1 + no_flags);
    ASSERT_EQ(out_ts, prev_timestamp_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(prev_timestamp, iout_ts);

    //////////KEY2//////////////////////////////
    out_ts.clear();
    // try to get key2 at current timestamp
    val = db->get(
        v4blockchain::detail::LATEST_KEYS_CF, latest_keys.getCategoryPrefix("merkle") + key2, timestamp_str, &out_ts);
    ASSERT_FALSE(val.has_value());
    // try to get key2 at higher timestamp
    out_ts.clear();
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  latest_keys.getCategoryPrefix("merkle") + key2,
                  higher_timestamp_str,
                  &out_ts);
    ASSERT_FALSE(val.has_value());
    // try to get key2 at lower timestamp
    out_ts.clear();
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  latest_keys.getCategoryPrefix("merkle") + key2,
                  lower_timestamp_str,
                  &out_ts);
    ASSERT_FALSE(val.has_value());
    // Get previous version
    out_ts.clear();
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  latest_keys.getCategoryPrefix("merkle") + key2,
                  prev_timestamp_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(*val, prev_val2 + no_flags);
    ASSERT_EQ(out_ts, prev_timestamp_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(prev_timestamp, iout_ts);
    // Get close version
    out_ts.clear();
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  latest_keys.getCategoryPrefix("merkle") + key2,
                  close_timestamp_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(*val, prev_val2 + no_flags);
    ASSERT_EQ(out_ts, prev_timestamp_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(prev_timestamp, iout_ts);
  }

  for (size_t i = 0; i < keys.size(); ++i) {
    if (keys[i] == "merkle_key2") {
      val_is_null_opts[i] = true;
      ver_is_null_opts[i] = true;
    }
  }
  testMultiGetValueAndVersion(latest_keys, 400, "merkle", keys, val_is_null_opts, ver_is_null_opts);

  for (size_t i = 0; i < keys.size(); ++i) {
    if (keys[i] == "merkle_key2") {  // since Key2 was added at 40
      val_is_null_opts[i] = false;
      ver_is_null_opts[i] = false;
    }
    if (keys[i] == "merkle_key3") {  // since Key3 was not there at 40
      val_is_null_opts[i] = true;
      ver_is_null_opts[i] = true;
    }
  }
  testMultiGetValueAndVersion(latest_keys, 40, "merkle", keys, val_is_null_opts, ver_is_null_opts);
}

TEST_F(v4_kvbc, add_version_keys) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories, []() { return 1; }};
  std::string no_flags = {0};
  std::string stale_on_update_flag = {1};
  std::vector<std::string> keys;
  std::vector<bool> val_is_null_opts;
  std::vector<bool> ver_is_null_opts;
  uint64_t block_id1 = 1;
  auto block_id1_str = v4blockchain::detail::Blockchain::generateKey(block_id1);
  std::string key1 = "ver_key1";
  std::string key2 = "ver_key2";
  std::string val1 = "ver_val1";
  std::string val2 = "ver_val2";
  auto ver_val1 = categorization::ValueWithFlags{"ver_val1", false};
  auto ver_val2 = categorization::ValueWithFlags{"ver_val2", true};

  // Block 1
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, block_id1, wb);
    db->write(std::move(wb));
    testGetValueAndVersion(
        latest_keys, block_id1, "versioned", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, block_id1, "versioned", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);
  }

  std::string out_ts;

  //////////KEY1//////////////////////////////

  // without category prefix
  auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF, key1, block_id1_str, &out_ts);
  ASSERT_FALSE(val.has_value());

  // get key1 updated value of this timestamp
  val = db->get(
      v4blockchain::detail::LATEST_KEYS_CF, latest_keys.getCategoryPrefix("versioned") + key1, block_id1_str, &out_ts);
  ASSERT_TRUE(val.has_value());

  ASSERT_EQ(*val, ver_val1.data + no_flags);
  ASSERT_EQ(out_ts, block_id1_str);
  auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
  ASSERT_EQ(block_id1, iout_ts);
  out_ts.clear();
  //////////KEY2//////////////////////////////
  val = db->get(
      v4blockchain::detail::LATEST_KEYS_CF, latest_keys.getCategoryPrefix("versioned") + key2, block_id1_str, &out_ts);
  ASSERT_TRUE(val.has_value());
  ASSERT_EQ(*val, ver_val2.data + stale_on_update_flag);
  ASSERT_EQ(out_ts, block_id1_str);
  iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
  ASSERT_EQ(block_id1, iout_ts);
  testMultiGetValueAndVersion(latest_keys, block_id1, "versioned", keys, val_is_null_opts, ver_is_null_opts);
}

TEST_F(v4_kvbc, add_version_keys_adv) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories, []() { return 1; }};
  std::string no_flags = {0};
  std::string stale_on_update_flag = {1};
  std::vector<std::string> keys;
  std::vector<bool> val_is_null_opts;
  std::vector<bool> ver_is_null_opts;

  uint64_t block_id1 = 1;
  uint64_t block_id10 = 10;
  uint64_t block_id112 = 112;
  uint64_t block_id122 = 122;
  uint64_t block_id180 = 180;
  uint64_t block_id190 = 190;
  auto block_id1_str = v4blockchain::detail::Blockchain::generateKey(block_id1);
  auto block_id10_str = v4blockchain::detail::Blockchain::generateKey(block_id10);
  auto block_id112_str = v4blockchain::detail::Blockchain::generateKey(block_id112);
  auto block_id122_str = v4blockchain::detail::Blockchain::generateKey(block_id122);
  auto block_id180_str = v4blockchain::detail::Blockchain::generateKey(block_id180);
  auto block_id190_str = v4blockchain::detail::Blockchain::generateKey(block_id190);

  std::string key1 = "ver_key1";
  std::string key2 = "ver_key2";
  std::string val1 = "ver_val1";
  std::string val2 = "ver_val2";
  auto ver_val1 = categorization::ValueWithFlags{"ver_val1", false};
  auto ver_val2 = categorization::ValueWithFlags{"ver_val2", true};
  auto ver_val3 = categorization::ValueWithFlags{"ver_val_updated", false};
  auto ver_val4 = categorization::ValueWithFlags{"ver_val_after_delete", false};

  // Block 1
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, block_id1, wb);
    db->write(std::move(wb));
    testGetValueAndVersion(
        latest_keys, block_id1, "versioned", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, block_id1, "versioned", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);
  }

  // Block 2
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key1", "ver_val_updated");
    ver_updates.addDelete("ver_key2");
    updates.add("versioned", std::move(ver_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, block_id112, wb);
    db->write(std::move(wb));
    testGetValueAndVersion(
        latest_keys, block_id112, "versioned", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, block_id112, "versioned", key2, true, true, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, block_id1, "versioned", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);
  }

  // Block 3
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addDelete("ver_key1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val_after_delete", false});
    updates.add("versioned", std::move(ver_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, block_id180, wb);
    db->write(std::move(wb));
    testGetValueAndVersion(
        latest_keys, block_id180, "versioned", key1, true, true, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, block_id112, "versioned", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, block_id180, "versioned", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);
  }

  std::string out_ts;

  //////////KEY1//////////////////////////////

  // get key1  value of this timestamp
  {
    testGetValueAndVersion(
        latest_keys, block_id1, "versioned", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key1,
                       block_id1_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(*val, ver_val1.data + no_flags);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
  }
  // get key1 value of  higer timestamp
  {
    testGetValueAndVersion(
        latest_keys, block_id10, "versioned", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key1,
                       block_id10_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    ASSERT_EQ(*val, ver_val1.data + no_flags);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
  }
  // get key1 updated value
  {
    testGetValueAndVersion(
        latest_keys, block_id112, "versioned", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key1,
                       block_id112_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    ASSERT_EQ(*val, ver_val3.data + no_flags);
    ASSERT_EQ(out_ts, block_id112_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id112, iout_ts);
    out_ts.clear();
  }

  ///////////KEY2/////////////////
  {
    testGetValueAndVersion(
        latest_keys, block_id1, "versioned", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id1_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    ASSERT_EQ(*val, ver_val2.data + stale_on_update_flag);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
  }
  // get key2 value of  higer timestamp
  {
    testGetValueAndVersion(
        latest_keys, block_id10, "versioned", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id10_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    ASSERT_EQ(*val, ver_val2.data + stale_on_update_flag);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
  }
  // get key2 deleted value
  {
    testGetValueAndVersion(
        latest_keys, block_id112, "versioned", key2, true, true, keys, val_is_null_opts, ver_is_null_opts);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id112_str,
                       &out_ts);
    ASSERT_FALSE(val.has_value());
  }

  // get key2 updates value
  {
    testGetValueAndVersion(
        latest_keys, block_id180, "versioned", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id180_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(*val, ver_val4.data + no_flags);
    ASSERT_EQ(out_ts, block_id180_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id180, iout_ts);
    out_ts.clear();
  }

  std::vector<uint64_t> blocks{block_id1, block_id10, block_id112, block_id122, block_id180, block_id190};
  for (const auto blk : blocks) {
    for (size_t i = 0; i < keys.size(); ++i) {
      val_is_null_opts[i] = false;
      ver_is_null_opts[i] = false;
      if ((blk == block_id112) || (blk == block_id122)) {
        if (keys[i] == key2) {
          val_is_null_opts[i] = true;
          ver_is_null_opts[i] = true;
        }
      } else if ((blk == block_id180) || (blk == block_id190)) {
        if (keys[i] == key1) {
          val_is_null_opts[i] = true;
          ver_is_null_opts[i] = true;
        }
      }
    }
    testMultiGetValueAndVersion(latest_keys, blk, "versioned", keys, val_is_null_opts, ver_is_null_opts);
  }
}

TEST_F(v4_kvbc, add_immutable_keys) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories, []() { return 1; }};
  std::string no_flags = {0};
  std::string stale_on_update_flag = {1};
  std::vector<std::string> keys;
  std::vector<bool> val_is_null_opts;
  std::vector<bool> ver_is_null_opts;
  uint64_t block_id1 = 1;
  auto block_id1_str = v4blockchain::detail::Blockchain::generateKey(block_id1);
  std::string key1 = "imm_key1";
  std::string val1 = "imm_val1";

  auto imm_val1 = categorization::ImmutableValueUpdate{"imm_val1", {"1", "2"}};

  // Block 1
  {
    categorization::Updates updates;
    categorization::ImmutableUpdates imm_updates;
    imm_updates.addUpdate("imm_key1", categorization::ImmutableUpdates::ImmutableValue{"imm_val1", {"1", "2"}});
    updates.add("immutable", std::move(imm_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, block_id1, wb);
    db->write(std::move(wb));
  }

  std::string out_ts;

  //////////KEY1//////////////////////////////
  testGetValueAndVersion(
      latest_keys, block_id1, "immutable", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
  // without category prefix
  auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF, key1, block_id1_str, &out_ts);
  ASSERT_FALSE(val.has_value());

  // get key1 updated value of this timestamp
  val = db->get(
      v4blockchain::detail::LATEST_KEYS_CF, latest_keys.getCategoryPrefix("immutable") + key1, block_id1_str, &out_ts);
  ASSERT_TRUE(val.has_value());
  ASSERT_EQ(*val, imm_val1.data + stale_on_update_flag);
  ASSERT_EQ(out_ts, block_id1_str);
  auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
  ASSERT_EQ(block_id1, iout_ts);
  out_ts.clear();
  testMultiGetValueAndVersion(latest_keys, block_id1, "immutable", keys, val_is_null_opts, ver_is_null_opts);
}

TEST_F(v4_kvbc, add_immutable_keys_adv) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories, []() { return 1; }};
  std::string no_flags = {0};
  std::string stale_on_update_flag = {1};
  std::vector<std::string> keys;
  std::vector<bool> val_is_null_opts;
  std::vector<bool> ver_is_null_opts;
  uint64_t block_id1 = 1;
  auto block_id1_str = v4blockchain::detail::Blockchain::generateKey(block_id1);
  uint64_t block_id10 = 10;
  auto block_id10_str = v4blockchain::detail::Blockchain::generateKey(block_id10);
  uint64_t block_id100 = 100;
  auto block_id100_str = v4blockchain::detail::Blockchain::generateKey(block_id100);
  std::string key1 = "imm_key1";
  std::string val1 = "imm_val1";

  auto imm_val1 = categorization::ImmutableValueUpdate{"imm_val1", {"1", "2"}};

  // Block 1
  {
    categorization::Updates updates;
    categorization::ImmutableUpdates imm_updates;
    imm_updates.addUpdate("imm_key1", categorization::ImmutableUpdates::ImmutableValue{"imm_val1", {"1", "2"}});
    updates.add("immutable", std::move(imm_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, block_id1, wb);
    db->write(std::move(wb));
  }

  std::string out_ts;

  //////////KEY1//////////////////////////////
  testGetValueAndVersion(
      latest_keys, block_id1, "immutable", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
  // without category prefix
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF, key1, block_id1_str, &out_ts);
    ASSERT_FALSE(val.has_value());

    // get key1 updated value of this timestamp
    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  latest_keys.getCategoryPrefix("immutable") + key1,
                  block_id1_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(*val, imm_val1.data + stale_on_update_flag);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();

    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  latest_keys.getCategoryPrefix("immutable") + key1,
                  block_id10_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(out_ts, block_id1_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
  }

  // Block 2
  {
    categorization::Updates updates;
    categorization::ImmutableUpdates imm_updates;
    imm_updates.addUpdate("imm_key1",
                          categorization::ImmutableUpdates::ImmutableValue{"imm_val1_updated", {"11", "2"}});
    updates.add("immutable", std::move(imm_updates));

    auto wb = db->getBatch();
    ASSERT_THROW(latest_keys.addBlockKeys(updates, block_id100, wb), std::runtime_error);
    db->write(std::move(wb));
    testGetValueAndVersion(
        latest_keys, block_id100, "immutable", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
  }
  // Check that original key still exists.
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("immutable") + key1,
                       block_id1_str,
                       &out_ts);

    ASSERT_EQ(*val, imm_val1.data + stale_on_update_flag);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();

    val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                  latest_keys.getCategoryPrefix("immutable") + key1,
                  block_id100_str,
                  &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(out_ts, block_id1_str);
    iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
  }
  testMultiGetValueAndVersion(latest_keys, block_id1, "immutable", keys, val_is_null_opts, ver_is_null_opts);
  testMultiGetValueAndVersion(latest_keys, block_id10, "immutable", keys, val_is_null_opts, ver_is_null_opts);
  testMultiGetValueAndVersion(latest_keys, block_id100, "immutable", keys, val_is_null_opts, ver_is_null_opts);
}

TEST_F(v4_kvbc, detect_stale_on_update) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories, []() { return 1; }};
  std::vector<std::string> keys;
  std::vector<bool> val_is_null_opts;
  std::vector<bool> ver_is_null_opts;
  uint64_t block_id1 = 1;
  auto block_id1_str = v4blockchain::detail::Blockchain::generateKey(block_id1);
  std::string key1 = "ver_key1";
  std::string key2 = "ver_key2";
  std::string val1 = "ver_val1";
  std::string val2 = "ver_val2";
  auto ver_val1 = categorization::ValueWithFlags{"ver_val1", false};
  auto ver_val2 = categorization::ValueWithFlags{"ver_val2", true};

  // Block 1
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, block_id1, wb);
    db->write(std::move(wb));
    testGetValueAndVersion(
        latest_keys, block_id1, "versioned", key1, false, false, keys, val_is_null_opts, ver_is_null_opts);
    testGetValueAndVersion(
        latest_keys, block_id1, "versioned", key2, false, false, keys, val_is_null_opts, ver_is_null_opts);
  }

  std::string out_ts;

  //////////KEY1//////////////////////////////
  // get key1 updated value of this timestamp
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key1,
                       block_id1_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_FALSE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));
  }

  //////////KEY2//////////////////////////////
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id1_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_TRUE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));
  }
}

TEST_F(v4_kvbc, compaction_filter) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories, []() { return 100; }};
  std::string no_flags = {0};
  std::string stale_on_update_flag = {1};

  uint64_t block_id1 = 1;
  uint64_t block_id100 = 100;
  auto block_id1_str = v4blockchain::detail::Blockchain::generateKey(block_id1);
  auto block_id100_str = v4blockchain::detail::Blockchain::generateKey(block_id100);
  std::string key1 = "ver_key1";
  std::string key2 = "ver_key2";
  std::string key3 = "ver_key3";
  std::string val1 = "ver_val1";
  std::string val2 = "ver_val2";
  auto ver_val1 = categorization::ValueWithFlags{"ver_val1", false};
  auto ver_val2 = categorization::ValueWithFlags{"ver_val2", true};

  // Block 1
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key1", "ver_val1");
    ver_updates.addUpdate("ver_key2", categorization::VersionedUpdates::Value{"ver_val2", true});
    updates.add("versioned", std::move(ver_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, block_id1, wb);
    db->write(std::move(wb));
  }

  // Block 100
  {
    categorization::Updates updates;
    categorization::VersionedUpdates ver_updates;
    ver_updates.calculateRootHash(false);
    ver_updates.addUpdate("ver_key3", categorization::VersionedUpdates::Value{"ver_val3", true});
    updates.add("versioned", std::move(ver_updates));

    auto wb = db->getBatch();
    latest_keys.addBlockKeys(updates, block_id100, wb);
    db->write(std::move(wb));
  }

  std::string out_ts;

  //////////KEY1//////////////////////////////
  // get key1 updated value of this timestamp
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key1,
                       block_id1_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    auto st_key = latest_keys.getCategoryPrefix("versioned") + key1 + block_id1_str;
    ASSERT_FALSE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));
    // Not stale on update
    bool changed;
    ASSERT_FALSE(latest_keys.getCompFilter()->Filter(0, st_key, *val, &out_ts, &changed));
  }

  //////////KEY2//////////////////////////////
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id1_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_TRUE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));

    auto st_key = latest_keys.getCategoryPrefix("versioned") + key2 + block_id1_str;
    // stale on update lower than genesis
    bool changed;
    ASSERT_TRUE(latest_keys.getCompFilter()->Filter(0, st_key, *val, &out_ts, &changed));
  }

  //////////KEY3//////////////////////////////
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key3,
                       block_id100_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());
    ASSERT_TRUE(v4blockchain::detail::LatestKeys::isStaleOnUpdate(*val));

    auto st_key = latest_keys.getCategoryPrefix("versioned") + key3 + block_id100_str;
    // stale on update equal to genesis
    bool changed;
    ASSERT_FALSE(latest_keys.getCompFilter()->Filter(0, st_key, *val, &out_ts, &changed));
  }
}

}  // end namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
