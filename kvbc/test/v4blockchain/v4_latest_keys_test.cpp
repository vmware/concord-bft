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

 protected:
  std::map<std::string, categorization::CATEGORY_TYPE> categories;
  std::shared_ptr<NativeClient> db;
};

TEST_F(v4_kvbc, creation) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories};

  ASSERT_TRUE(db->hasColumnFamily(v4blockchain::detail::LATEST_KEYS_CF));
  for (const auto& [k, v] : categories) {
    (void)v;
    ASSERT_TRUE(latest_keys.getCategoryPrefix(k).size() > 0);
  }
}

TEST_F(v4_kvbc, add_merkle_keys) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories};
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
}

TEST_F(v4_kvbc, add_version_keys) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories};
  std::string no_flags = {0};
  std::string stale_on_update_flag = {1};

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

  std::vector<uint8_t> serialized_value;
  concord::kvbc::categorization::serialize(serialized_value, ver_val1);
  std::string str_val(serialized_value.begin(), serialized_value.end());

  ASSERT_EQ(*val, str_val + no_flags);
  ASSERT_EQ(out_ts, block_id1_str);
  auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
  ASSERT_EQ(block_id1, iout_ts);
  out_ts.clear();
  serialized_value.clear();
  //////////KEY2//////////////////////////////
  val = db->get(
      v4blockchain::detail::LATEST_KEYS_CF, latest_keys.getCategoryPrefix("versioned") + key2, block_id1_str, &out_ts);
  ASSERT_TRUE(val.has_value());
  concord::kvbc::categorization::serialize(serialized_value, ver_val2);
  std::string str_val2(serialized_value.begin(), serialized_value.end());
  ASSERT_EQ(*val, str_val2 + stale_on_update_flag);
  ASSERT_EQ(out_ts, block_id1_str);
  iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
  ASSERT_EQ(block_id1, iout_ts);
}

TEST_F(v4_kvbc, add_version_keys_adv) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories};
  std::string no_flags = {0};
  std::string stale_on_update_flag = {1};

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
  }

  std::string out_ts;

  //////////KEY1//////////////////////////////

  // get key1  value of this timestamp
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key1,
                       block_id1_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    std::vector<uint8_t> serialized_value;
    concord::kvbc::categorization::serialize(serialized_value, ver_val1);
    std::string str_val(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val + no_flags);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
    serialized_value.clear();
  }
  // get key1 value of  higer timestamp
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key1,
                       block_id10_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    std::vector<uint8_t> serialized_value;
    concord::kvbc::categorization::serialize(serialized_value, ver_val1);
    std::string str_val(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val + no_flags);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
    serialized_value.clear();
  }
  // get key1 updated value
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key1,
                       block_id112_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    std::vector<uint8_t> serialized_value;
    concord::kvbc::categorization::serialize(serialized_value, ver_val3);
    std::string str_val(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val + no_flags);
    ASSERT_EQ(out_ts, block_id112_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id112, iout_ts);
    out_ts.clear();
    serialized_value.clear();
  }

  ///////////KEY2/////////////////
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id1_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    std::vector<uint8_t> serialized_value;
    concord::kvbc::categorization::serialize(serialized_value, ver_val2);
    std::string str_val(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val + stale_on_update_flag);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
    serialized_value.clear();
  }
  // get key2 value of  higer timestamp
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id10_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    std::vector<uint8_t> serialized_value;
    concord::kvbc::categorization::serialize(serialized_value, ver_val2);
    std::string str_val(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val + stale_on_update_flag);
    ASSERT_EQ(out_ts, block_id1_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id1, iout_ts);
    out_ts.clear();
    serialized_value.clear();
  }
  // get key1 deleted value
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id112_str,
                       &out_ts);
    ASSERT_FALSE(val.has_value());
  }

  // get key2 updates value
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("versioned") + key2,
                       block_id180_str,
                       &out_ts);
    ASSERT_TRUE(val.has_value());

    std::vector<uint8_t> serialized_value;
    concord::kvbc::categorization::serialize(serialized_value, ver_val4);
    std::string str_val(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val + no_flags);
    ASSERT_EQ(out_ts, block_id180_str);
    auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
    ASSERT_EQ(block_id180, iout_ts);
    out_ts.clear();
    serialized_value.clear();
  }
}

TEST_F(v4_kvbc, add_immutable_keys) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories};
  std::string no_flags = {0};

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

  // without category prefix
  auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF, key1, block_id1_str, &out_ts);
  ASSERT_FALSE(val.has_value());

  // get key1 updated value of this timestamp
  val = db->get(
      v4blockchain::detail::LATEST_KEYS_CF, latest_keys.getCategoryPrefix("immutable") + key1, block_id1_str, &out_ts);
  ASSERT_TRUE(val.has_value());

  std::vector<uint8_t> serialized_value;
  concord::kvbc::categorization::serialize(serialized_value, imm_val1);
  std::string str_val(serialized_value.begin(), serialized_value.end());

  ASSERT_EQ(*val, str_val + no_flags);
  ASSERT_EQ(out_ts, block_id1_str);
  auto iout_ts = concordUtils::fromBigEndianBuffer<uint64_t>(out_ts.data());
  ASSERT_EQ(block_id1, iout_ts);
  out_ts.clear();
  serialized_value.clear();
}

TEST_F(v4_kvbc, add_immutable_keys_adv) {
  v4blockchain::detail::LatestKeys latest_keys{db, categories};
  std::string no_flags = {0};

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

    std::vector<uint8_t> serialized_value;
    concord::kvbc::categorization::serialize(serialized_value, imm_val1);
    std::string str_val(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val + no_flags);
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
    serialized_value.clear();
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
  }
  // Check that original key still exists.
  {
    auto val = db->get(v4blockchain::detail::LATEST_KEYS_CF,
                       latest_keys.getCategoryPrefix("immutable") + key1,
                       block_id1_str,
                       &out_ts);
    std::vector<uint8_t> serialized_value;
    concord::kvbc::categorization::serialize(serialized_value, imm_val1);
    std::string str_val(serialized_value.begin(), serialized_value.end());

    ASSERT_EQ(*val, str_val + no_flags);
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
    serialized_value.clear();
  }
}

}  // end namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
