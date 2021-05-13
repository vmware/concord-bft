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

#include "categorization/base_types.h"
#include "categorization/column_families.h"
#include "categorization/details.h"
#include "categorization/versioned_kv_category.h"
#include "kv_types.hpp"
#include "rocksdb/native_client.h"
#include "storage/test/storage_test_common.h"

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <variant>

namespace {

using namespace ::testing;
using namespace concord::storage::rocksdb;
using namespace concord::kvbc;
using namespace concord::kvbc::categorization;
using namespace concord::kvbc::categorization::detail;
using namespace std::literals;

class versioned_kv_category : public Test {
  void SetUp() override {
    cleanup();
    db = TestRocksDb::createNative();
    cat = VersionedKeyValueCategory{category_id, db};
  }
  void TearDown() override { cleanup(); }

 protected:
  auto add(BlockId block_id, VersionedInput &&in) {
    auto update_batch = db->getBatch();
    auto out = cat.add(block_id, std::move(in), update_batch);
    db->write(std::move(update_batch));
    return out;
  }

 protected:
  const std::string category_id{"cat"};
  const std::string values_cf{category_id + VERSIONED_KV_VALUES_CF_SUFFIX};
  const std::string latest_ver_cf{category_id + VERSIONED_KV_LATEST_VER_CF_SUFFIX};
  const std::string active_cf_{category_id + VERSIONED_KV_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF_SUFFIX};
  std::shared_ptr<NativeClient> db;

  VersionedKeyValueCategory cat;
};

TEST_F(versioned_kv_category, create_column_families_on_construction) {
  ASSERT_THAT(
      db->columnFamilies(),
      ContainerEq(std::unordered_set<std::string>{db->defaultColumnFamily(), values_cf, latest_ver_cf, active_cf_}));
}

TEST_F(versioned_kv_category, empty_updates) {
  // Calculate root hash = false.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = false;
    auto batch = db->getBatch();
    const auto out = cat.add(1, std::move(in), batch);
    ASSERT_EQ(batch.count(), 0);
    ASSERT_FALSE(out.root_hash);
    ASSERT_TRUE(out.keys.empty());
  }

  // Calculate root hash = true.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    auto batch = db->getBatch();
    const auto out = cat.add(1, std::move(in), batch);
    ASSERT_EQ(batch.count(), 0);
    ASSERT_TRUE(out.root_hash);
    // Expect the empty SHA3-256 hash.
    ASSERT_THAT(*out.root_hash, ContainerEq(Hash{0xa7, 0xff, 0xc6, 0xf8, 0xbf, 0x1e, 0xd7, 0x66, 0x51, 0xc1, 0x47,
                                                 0x56, 0xa0, 0x61, 0xd6, 0x62, 0xf5, 0x80, 0xff, 0x4d, 0xe4, 0x3b,
                                                 0x49, 0xfa, 0x82, 0xd8, 0x0a, 0x4b, 0x80, 0xf8, 0x43, 0x4a}));
    ASSERT_TRUE(out.keys.empty());
  }
}

TEST_F(versioned_kv_category, calculate_root_hash_toggle) {
  const auto stale_on_update = false;

  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k"] = ValueWithFlags{"v", stale_on_update};
    const auto out = add(1, std::move(in));
    ASSERT_TRUE(out.root_hash);
  }

  {
    auto in = VersionedInput{};
    in.calculate_root_hash = false;
    in.kv["k"] = ValueWithFlags{"v", stale_on_update};
    const auto out = add(1, std::move(in));
    ASSERT_FALSE(out.root_hash);
  }
}

TEST_F(versioned_kv_category, non_existent_key) { ASSERT_FALSE(cat.getLatest("non-existent"s)); }

TEST_F(versioned_kv_category, get_and_get_latest) {
  const auto stale_on_update = false;

  // Update key "k" with "v1" at block 1.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k"] = ValueWithFlags{"v1", stale_on_update};
    add(1, std::move(in));
  }

  // Update key "k" with "v3" at block 3.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k"] = ValueWithFlags{"v3", stale_on_update};
    add(3, std::move(in));
  }

  // Get key "k" at block 1.
  {
    const auto value = cat.get("k", 1);
    ASSERT_TRUE(value);
    ASSERT_EQ(asVersioned(value).block_id, 1);
    ASSERT_EQ(asVersioned(value).data, "v1");
  }

  // Get key "k" at the non-existent versions.
  {
    ASSERT_FALSE(cat.get("k", 2));
    ASSERT_FALSE(cat.get("k", 4));
  }

  // Get key "k" at block 3.
  {
    const auto value = cat.get("k", 3);
    ASSERT_TRUE(value);
    ASSERT_EQ(asVersioned(value).block_id, 3);
    ASSERT_EQ(asVersioned(value).data, "v3");
  }

  // Get the latest value of key "k".
  {
    const auto value = cat.getLatest("k");
    ASSERT_TRUE(value);
    ASSERT_EQ(asVersioned(value).block_id, 3);
    ASSERT_EQ(asVersioned(value).data, "v3");
  }
}

TEST_F(versioned_kv_category, multi_get_and_multi_get_latest) {
  const auto stale_on_update = false;

  // Update keys "ka" and "kb" with "va1" and "vb1" at block 1.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va1", stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb1", stale_on_update};
    add(1, std::move(in));
  }

  // Update keys "ka" and "kb" with "va3" and "vb3" at block 3.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va3", stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb3", stale_on_update};
    add(3, std::move(in));
  }

  // Get keys "ka" and "kb" at block 1.
  {
    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGet({"ka", "kb", "non-existent"}, {1, 1, 1}, values);
    const auto expected = std::vector<std::optional<categorization::Value>>{
        VersionedValue{{1, "va1"}}, VersionedValue{{1, "vb1"}}, std::nullopt};
    ASSERT_EQ(values, expected);

    // Make sure that subsequent calls with the same vector work.
    cat.multiGet({"kb"}, {3}, values);
    const auto expected_subs = std::vector<std::optional<categorization::Value>>{VersionedValue{{3, "vb3"}}};
    ASSERT_EQ(values, expected_subs);
  }

  // Get keys "ka" and "kb" at block 3.
  {
    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGet({"non-existent1", "ka", "non-existent2", "kb", "non-existent3"}, {3, 3, 3, 3, 3}, values);
    const auto expected = std::vector<std::optional<categorization::Value>>{
        std::nullopt, VersionedValue{{3, "va3"}}, std::nullopt, VersionedValue{{3, "vb3"}}, std::nullopt};
    ASSERT_EQ(values, expected);
  }

  // Get keys at non-existent versions.
  {
    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGet({"ka", "kb", "ka"}, {2, 4, 1}, values);
    const auto expected =
        std::vector<std::optional<categorization::Value>>{std::nullopt, std::nullopt, VersionedValue{{1, "va1"}}};
    ASSERT_EQ(values, expected);
  }

  // Get the latest values of keys with a non-existent key.
  {
    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGetLatest({"ka", "kb", "non-existent"}, values);
    const auto expected = std::vector<std::optional<categorization::Value>>{
        VersionedValue{{3, "va3"}}, VersionedValue{{3, "vb3"}}, std::nullopt};
    ASSERT_EQ(values, expected);
  }

  // Get the latest values of keys.
  auto values = std::vector<std::optional<categorization::Value>>{};
  {
    cat.multiGetLatest({"ka", "kb"}, values);
    const auto expected =
        std::vector<std::optional<categorization::Value>>{VersionedValue{{3, "va3"}}, VersionedValue{{3, "vb3"}}};
    ASSERT_EQ(values, expected);
  }

  // Make sure that subsequent calls with the same vector work.
  {
    cat.multiGetLatest({"kb"}, values);
    const auto expected = std::vector<std::optional<categorization::Value>>{VersionedValue{{3, "vb3"}}};
    ASSERT_EQ(values, expected);
  }
}

TEST_F(versioned_kv_category, get_latest_ver_and_multi_get_latest_ver) {
  const auto stale_on_update = false;
  const auto deleted = false;

  // Update keys "ka" and "kb" with "va1" and "vb1" at block 1.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va1", stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb1", stale_on_update};
    add(1, std::move(in));
  }

  // Update keys "ka" and "kb" with "va3" and "vb3" at block 3.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va3", stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb3", stale_on_update};
    add(3, std::move(in));
  }

  // Get latest version of existing keys.
  {
    const auto ka = cat.getLatestVersion("ka");
    ASSERT_TRUE(ka);
    ASSERT_FALSE(ka->deleted);
    ASSERT_EQ(ka->version, 3);

    const auto kb = cat.getLatestVersion("kb");
    ASSERT_TRUE(kb);
    ASSERT_FALSE(kb->deleted);
    ASSERT_EQ(kb->version, 3);
  }

  // Get latest version of a non-existent key.
  {
    const auto ver = cat.getLatestVersion("non-existent");
    ASSERT_FALSE(ver);
  }

  // Get multiple latest versions.
  auto versions = std::vector<std::optional<TaggedVersion>>{};
  {
    cat.multiGetLatestVersion({"ka", "non-existent", "kb"}, versions);
    const auto expected =
        std::vector<std::optional<TaggedVersion>>{TaggedVersion{deleted, 3}, std::nullopt, TaggedVersion{deleted, 3}};
    ASSERT_EQ(versions, expected);
  }

  // Make sure that subsequent calls with the same vector work.
  {
    cat.multiGetLatestVersion({"kb"}, versions);
    const auto expected = std::vector<std::optional<TaggedVersion>>{TaggedVersion{deleted, 3}};
    ASSERT_EQ(versions, expected);
  }
}

TEST_F(versioned_kv_category, get_proof) {
  const auto stale_on_update = false;
  const auto block1 = 1;
  const auto block2 = 2;
  const auto invalid_block = 3;

  // Add block1 with provable keys.
  auto in = VersionedInput{};
  in.calculate_root_hash = true;
  in.kv["k1"] = ValueWithFlags{"v1", stale_on_update};
  in.kv["k2"] = ValueWithFlags{"v2", stale_on_update};
  in.kv["k3"] = ValueWithFlags{"v3", stale_on_update};
  const auto out = add(block1, std::move(in));

  // Insert a block2 after block1 in order to ensure we are providing proofs for the correct block ID.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k1"] = ValueWithFlags{"dummyv1", stale_on_update};
    in.kv["k2"] = ValueWithFlags{"dummyv2", stale_on_update};
    in.kv["k3"] = ValueWithFlags{"dummyv2", stale_on_update};
    add(block2, std::move(in));
  }

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // h("k2") = 189284195f920d885bc46edf2d6c2c56194d3333448eda64ddd726c901b59c28
  // h("v2") = 86a74b56a4ca89e2a292dc3995a15149a2843b038965d0feabd3d20a663f759f
  // h("k3") = d2cd0fe12ce97350e7d136d40707373305040a5c5a72b5aded93ecd104548244
  // h("v3") = ef1ceacbca55ac4c6d196f5aa52e9c712574d3f42810309e74dfa697861ecf88
  // root_hash = h(h("k1") || h("v1") || h("k2") || h("v2") || h("k3") || h("v3")) =
  //           = acc07312b86daec7d0b8b53bd67e49d7c5c1677bf174904962a836eb9db4a8cf

  // First key.
  {
    const auto proof = cat.getProof(block1, "k1", out);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block1);
    ASSERT_EQ(proof->key, "k1");
    ASSERT_EQ(proof->value, "v1");
    ASSERT_EQ(proof->key_value_index, 0);  // indexing starts at 0
    ASSERT_THAT(proof->calculateRootHash(),
                ContainerEq(Hash{0xac, 0xc0, 0x73, 0x12, 0xb8, 0x6d, 0xae, 0xc7, 0xd0, 0xb8, 0xb5,
                                 0x3b, 0xd6, 0x7e, 0x49, 0xd7, 0xc5, 0xc1, 0x67, 0x7b, 0xf1, 0x74,
                                 0x90, 0x49, 0x62, 0xa8, 0x36, 0xeb, 0x9d, 0xb4, 0xa8, 0xcf}));
  }

  // Middle key.
  {
    const auto proof = cat.getProof(block1, "k2", out);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block1);
    ASSERT_EQ(proof->key, "k2");
    ASSERT_EQ(proof->value, "v2");
    ASSERT_EQ(proof->key_value_index, 2);  // indexing starts at 0
    ASSERT_THAT(proof->calculateRootHash(),
                ContainerEq(Hash{0xac, 0xc0, 0x73, 0x12, 0xb8, 0x6d, 0xae, 0xc7, 0xd0, 0xb8, 0xb5,
                                 0x3b, 0xd6, 0x7e, 0x49, 0xd7, 0xc5, 0xc1, 0x67, 0x7b, 0xf1, 0x74,
                                 0x90, 0x49, 0x62, 0xa8, 0x36, 0xeb, 0x9d, 0xb4, 0xa8, 0xcf}));
  }

  // Last key.
  {
    const auto proof = cat.getProof(block1, "k3", out);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block1);
    ASSERT_EQ(proof->key, "k3");
    ASSERT_EQ(proof->value, "v3");
    ASSERT_EQ(proof->key_value_index, 4);  // indexing starts at 0
    ASSERT_THAT(proof->calculateRootHash(),
                ContainerEq(Hash{0xac, 0xc0, 0x73, 0x12, 0xb8, 0x6d, 0xae, 0xc7, 0xd0, 0xb8, 0xb5,
                                 0x3b, 0xd6, 0x7e, 0x49, 0xd7, 0xc5, 0xc1, 0x67, 0x7b, 0xf1, 0x74,
                                 0x90, 0x49, 0x62, 0xa8, 0x36, 0xeb, 0x9d, 0xb4, 0xa8, 0xcf}));
  }

  // Invalid block ID.
  {
    ASSERT_FALSE(cat.getProof(invalid_block, "k1", out));
    ASSERT_FALSE(cat.getProof(invalid_block, "k2", out));
    ASSERT_FALSE(cat.getProof(invalid_block, "k3", out));
  }

  // Non-existing key.
  { ASSERT_FALSE(cat.getProof(block1, "non-existing", out)); }
}

TEST_F(versioned_kv_category, delete_key) {
  const auto stale_on_update = false;

  // Update key "k" to "v1" in block 1.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k"] = ValueWithFlags{"v1", stale_on_update};
    add(1, std::move(in));
  }

  // Delete key "k" in block 2.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.deletes.push_back("k");
    const auto out = add(2, std::move(in));
    ASSERT_EQ(out.keys.size(), 1);
    ASSERT_EQ(out.keys.cbegin()->first, "k");
    ASSERT_TRUE(out.keys.cbegin()->second.deleted);
  }

  // Key "k" is still available at block 1 via get().
  {
    const auto value = cat.get("k", 1);
    ASSERT_TRUE(value);
    ASSERT_EQ(asVersioned(value).block_id, 1);
    ASSERT_EQ(asVersioned(value).data, "v1");
  }

  // Key "k" is still available at block 1 via multiGet().
  {
    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGet({"k"}, {1}, values);
    const auto expected = std::vector<std::optional<categorization::Value>>{VersionedValue{{1, "v1"}}};
    ASSERT_EQ(values, expected);
  }

  // Key "k" is no longer available for blocks >= 2 via get().
  {
    ASSERT_FALSE(cat.get("k", 2));
    ASSERT_FALSE(cat.get("k", 3));
  }

  // Key "k" is no longer available for blocks >= 2 via multiGet().
  {
    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGet({"k", "k"}, {2, 3}, values);
    const auto expected = std::vector<std::optional<categorization::Value>>{std::nullopt, std::nullopt};
    ASSERT_EQ(values, expected);
  }

  // Key "k" has no latest value via getLatest().
  { ASSERT_FALSE(cat.getLatest("k")); }

  {
    // Key "k" has no latest value via multiGetLatest().
    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGetLatest({"k"}, values);
    const auto expected = std::vector<std::optional<categorization::Value>>{std::nullopt};
    ASSERT_EQ(values, expected);
  }

  // Key "k" has a latest version of 2 via getLatestVersion() - the version it was deleted at.
  {
    const auto latest = cat.getLatestVersion("k");
    ASSERT_TRUE(latest);
    ASSERT_TRUE(latest->deleted);
    ASSERT_EQ(latest->version, 2);
  }

  // Key "k" has a latest version of 2 via multiGetLatestVersion() - the version it was deleted at.
  {
    const auto deleted = true;
    auto latest_versions = std::vector<std::optional<TaggedVersion>>{};
    cat.multiGetLatestVersion({"k"}, latest_versions);
    const auto expected = std::vector<std::optional<TaggedVersion>>{TaggedVersion{deleted, 2}};
    ASSERT_EQ(latest_versions, expected);
  }
}

TEST_F(versioned_kv_category, multi_get_latest_with_updates_and_deletes) {
  const auto stale_on_update = false;

  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k1"] = ValueWithFlags{"v1", stale_on_update};
    in.kv["k2"] = ValueWithFlags{"v2", stale_on_update};
    in.kv["k3"] = ValueWithFlags{"v3", stale_on_update};
    in.kv["k4"] = ValueWithFlags{"v4", stale_on_update};
    in.kv["k5"] = ValueWithFlags{"v5", stale_on_update};
    add(1, std::move(in));
  }

  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k2"] = ValueWithFlags{"new_v2", stale_on_update};
    in.deletes.push_back("k3");
    in.deletes.push_back("k5");
    add(2, std::move(in));
  }

  {
    auto keys = std::vector<std::string>{"k1", "k2", "k3", "non-existent", "k4", "k5", "non-existent2"};
    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGetLatest(keys, values);

    // "k1"'s value at version 1.
    ASSERT_TRUE(values[0]);
    const auto expected1 = categorization::Value{VersionedValue{{1, "v1"}}};
    ASSERT_EQ(expected1, *values[0]);

    // "k2"'s value at version 2.
    const auto expected2 = categorization::Value{VersionedValue{{2, "new_v2"}}};
    ASSERT_TRUE(values[1]);
    ASSERT_EQ(expected2, *values[1]);

    // "k3" has been deleted.
    ASSERT_FALSE(values[2]);

    // "non-existent".
    ASSERT_FALSE(values[3]);

    // "k4"'s value at version 1.
    ASSERT_TRUE(values[4]);
    const auto expected4 = categorization::Value{VersionedValue{{1, "v4"}}};
    ASSERT_EQ(expected4, *values[4]);

    // "k5" has been deleted.
    ASSERT_FALSE(values[5]);

    // "non-existent2".
    ASSERT_FALSE(values[6]);
  }
}

TEST_F(versioned_kv_category, propagate_stale_on_update) {
  {
    const auto stale_on_update = false;
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k"] = ValueWithFlags{"v1", stale_on_update};
    const auto out = add(1, std::move(in));

    ASSERT_FALSE(out.keys.cbegin()->second.stale_on_update);
  }

  {
    const auto stale_on_update = true;
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k"] = ValueWithFlags{"v2", stale_on_update};
    const auto out = add(2, std::move(in));

    ASSERT_TRUE(out.keys.cbegin()->second.stale_on_update);
  }
}

TEST_F(versioned_kv_category, delete_genesis_with_update) {
  const auto mark_stale_on_update = true;
  const auto non_stale_on_update = false;

  // Add keys "ka", "kb", "kc" and "kd" in block 1. Mark "kc" and "kd" as stale on update.
  auto out1 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va1", non_stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb1", non_stale_on_update};
    in.kv["kc"] = ValueWithFlags{"vc1", mark_stale_on_update};
    in.kv["kd"] = ValueWithFlags{"vd1", mark_stale_on_update};
    out1 = add(1, std::move(in));
  }

  // Update key "ka" and "kd" in block 2 (making them stale).
  auto out5 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va5", non_stale_on_update};
    in.kv["kd"] = ValueWithFlags{"vd5", non_stale_on_update};
    out5 = add(5, std::move(in));
  }

  // Delete genesis block 1.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(1, out1, batch);
    db->write(std::move(batch));
  }

  // Lookups in block 1.
  {
    ASSERT_FALSE(cat.get("ka", 1));
    ASSERT_FALSE(cat.get("kc", 1));
    ASSERT_FALSE(cat.get("kd", 1));

    const auto value = cat.get("kb", 1);
    ASSERT_TRUE(value);
    ASSERT_EQ(asVersioned(value).block_id, 1);
    ASSERT_EQ(asVersioned(value).data, "vb1");
  }

  // Make sure we have a latest version for "ka".
  {
    const auto latest = cat.getLatestVersion("ka");
    ASSERT_TRUE(latest);
    ASSERT_FALSE(latest->deleted);
    ASSERT_EQ(latest->version, 5);

    const auto value = cat.getLatest("ka");
    ASSERT_EQ(asVersioned(value).block_id, 5);
    ASSERT_EQ(asVersioned(value).data, "va5");

    const auto deleted = false;
    auto latest_versions = std::vector<std::optional<TaggedVersion>>{};
    cat.multiGetLatestVersion({"ka"}, latest_versions);
    const auto expected_versions = std::vector<std::optional<TaggedVersion>>{TaggedVersion{deleted, 5}};
    ASSERT_EQ(latest_versions, expected_versions);

    auto latest_values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGetLatest({"ka"}, latest_values);
    const auto expected_values = std::vector<std::optional<categorization::Value>>{VersionedValue{{5, "va5"}}};
    ASSERT_EQ(latest_values, expected_values);
  }

  // Make sure we have a latest version for "kd".
  {
    const auto deleted = false;
    const auto latest = cat.getLatestVersion("kd");
    ASSERT_TRUE(latest);
    ASSERT_FALSE(latest->deleted);
    ASSERT_EQ(latest->version, 5);

    const auto value = cat.getLatest("kd");
    ASSERT_EQ(asVersioned(value).block_id, 5);
    ASSERT_EQ(asVersioned(value).data, "vd5");

    auto latest_versions = std::vector<std::optional<TaggedVersion>>{};
    cat.multiGetLatestVersion({"kd"}, latest_versions);
    const auto expected_versions = std::vector<std::optional<TaggedVersion>>{TaggedVersion{deleted, 5}};
    ASSERT_EQ(latest_versions, expected_versions);

    auto latest_values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGetLatest({"kd"}, latest_values);
    const auto expected_values = std::vector<std::optional<categorization::Value>>{VersionedValue{{5, "vd5"}}};
    ASSERT_EQ(latest_values, expected_values);
  }

  // Make sure there is no "kc" as it is stale-on-update and there is no subsequent version.
  {
    ASSERT_FALSE(cat.get("kc", 1));
    ASSERT_FALSE(cat.getLatest("kc"));
    ASSERT_FALSE(cat.getLatestVersion("kc"));
  }
}

TEST_F(versioned_kv_category, delete_genesis_with_deletes) {
  auto out = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = false;
    in.deletes.push_back("ka");
    in.deletes.push_back("kb");
    out = add(1, std::move(in));
  }

  // Make sure there are keys in the DB.
  {
    auto values_iter = db->getIterator(values_cf);
    values_iter.first();
    ASSERT_TRUE(values_iter);

    auto latest_ver_iter = db->getIterator(latest_ver_cf);
    latest_ver_iter.first();
    ASSERT_TRUE(latest_ver_iter);
  }

  // Delete genesis block 1.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(1, out, batch);
    db->write(std::move(batch));
  }

  // Make sure there are no keys left.
  {
    auto values_iter = db->getIterator(values_cf);
    values_iter.first();
    ASSERT_FALSE(values_iter);

    auto latest_ver_iter = db->getIterator(latest_ver_cf);
    latest_ver_iter.first();
    ASSERT_FALSE(latest_ver_iter);
  }
}

TEST_F(versioned_kv_category, delete_genesis_with_update_and_delete_afterwards) {
  auto out1 = VersionedOutput{};
  {
    const auto mark_stale_on_update = false;
    auto in = VersionedInput{};
    in.calculate_root_hash = false;
    in.kv["k"] = ValueWithFlags{"v", mark_stale_on_update};
    out1 = add(1, std::move(in));
  }

  auto out2 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = false;
    in.deletes.push_back("k");
    out2 = add(2, std::move(in));
  }

  ASSERT_TRUE(cat.get("k", 1));
  ASSERT_FALSE(cat.get("k", 2));

  // Delete genesis block 1.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(1, out1, batch);
    db->write(std::move(batch));
  }

  // Delete genesis block 2.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(2, out2, batch);
    db->write(std::move(batch));
  }

  // Make sure pruning has removed key "k" at block 1.
  {
    ASSERT_FALSE(cat.get("k", 1));
    ASSERT_FALSE(cat.get("k", 2));
  }

  // Make sure there are no keys left as the latest version (2) of key "k" is a deletion and we prune block 2.
  {
    auto values_iter = db->getIterator(values_cf);
    values_iter.first();
    ASSERT_FALSE(values_iter);

    auto latest_ver_iter = db->getIterator(latest_ver_cf);
    latest_ver_iter.first();
    ASSERT_FALSE(latest_ver_iter);

    auto active_iter = db->getIterator(active_cf_);
    active_iter.first();
    ASSERT_FALSE(active_iter);
  }
}

TEST_F(versioned_kv_category, delete_genesis_with_delete_and_update_afterwards) {
  auto out1 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = false;
    in.deletes.push_back("k");
    out1 = add(1, std::move(in));
  }

  ASSERT_FALSE(cat.get("k", 1));

  auto out2 = VersionedOutput{};
  {
    const auto mark_stale_on_update = false;
    auto in = VersionedInput{};
    in.calculate_root_hash = false;
    in.kv["k"] = ValueWithFlags{"v", mark_stale_on_update};
    out2 = add(2, std::move(in));
  }

  // Delete genesis block 1.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(1, out1, batch);
    db->write(std::move(batch));
  }

  // Delete genesis block 2.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(2, out2, batch);
    db->write(std::move(batch));
  }

  // Make sure pruning has no effect on keys.
  {
    ASSERT_FALSE(cat.get("k", 1));
    const auto v = cat.get("k", 2);
    ASSERT_TRUE(v);
    ASSERT_EQ(asVersioned(v).block_id, 2);
    ASSERT_EQ(asVersioned(v).data, "v");
  }

  // Make sure key "k" is present in the active column family as block 2 (its latest version) was pruned.
  ASSERT_TRUE(db->get(active_cf_, "k"sv));
}

TEST_F(versioned_kv_category, delete_genesis_with_active_keys) {
  const auto stale_on_update = false;

  // Add block 1 with 2 keys.
  auto out1 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va1", stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb1", stale_on_update};
    out1 = add(1, std::move(in));
  }

  // Delete genesis block 1 with active keys inside.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(1, out1, batch);
    db->write(std::move(batch));
  }

  // Verify that keys are persisted in in the active key column family.
  ASSERT_TRUE(db->get(active_cf_, "ka"sv));
  ASSERT_TRUE(db->get(active_cf_, "kb"sv));

  // Add block 2 with the same 2 keys as in block 1.
  auto out2 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va2", stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb2", stale_on_update};
    out2 = add(2, std::move(in));
  }

  // Add block 3 with the same 2 keys as in block 1 and 2.
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va3", stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb3", stale_on_update};
    add(3, std::move(in));
  }

  // Delete genesis block 2. Since there is a subsequent block 3 with the same keys, the keys in block 2 are not active
  // anymore and can be deleted.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(2, out2, batch);
    db->write(std::move(batch));
  }

  // Verify that keys that were active at block version 1 are deleted.
  {
    ASSERT_FALSE(cat.get("ka", 1));
    ASSERT_FALSE(cat.get("kb", 1));

    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGet({"ka", "kb"}, {1, 1}, values);
    ASSERT_EQ(values.size(), 2);
    ASSERT_FALSE(values[0]);
    ASSERT_FALSE(values[1]);
  }

  // Verify that keys in the active key column family are deleted.
  ASSERT_FALSE(db->get(active_cf_, "ka"sv));
  ASSERT_FALSE(db->get(active_cf_, "kb"sv));
}

TEST_F(versioned_kv_category, delete_genesis_with_active_keys_and_previous_active_keys) {
  const auto stale_on_update = false;

  // Add block 1 with 2 keys.
  auto out1 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va1", stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb1", stale_on_update};
    out1 = add(1, std::move(in));
  }

  // Delete genesis block 1 with active keys inside.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(1, out1, batch);
    db->write(std::move(batch));
  }

  // Verify that keys are persisted in in the active key column family.
  ASSERT_TRUE(db->get(active_cf_, "ka"sv));
  ASSERT_TRUE(db->get(active_cf_, "kb"sv));

  // Add block 2 with the same 2 keys as in block 1.
  auto out2 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va2", stale_on_update};
    in.kv["kb"] = ValueWithFlags{"vb2", stale_on_update};
    out2 = add(2, std::move(in));
  }

  // Delete genesis block 2. That should remove the active keys at block 1 and update the active key versions in the
  // active key column family.
  {
    auto batch = db->getBatch();
    cat.deleteGenesisBlock(2, out2, batch);
    db->write(std::move(batch));
  }

  // Verify that keys that were active at block version 1 are deleted.
  {
    ASSERT_FALSE(cat.get("ka", 1));
    ASSERT_FALSE(cat.get("kb", 1));

    auto values = std::vector<std::optional<categorization::Value>>{};
    cat.multiGet({"ka", "kb"}, {1, 1}, values);
    ASSERT_EQ(values.size(), 2);
    ASSERT_FALSE(values[0]);
    ASSERT_FALSE(values[1]);
  }

  // Verify that keys in the active key column family are of version 2 and not 1.
  {
    const auto ka = db->get(active_cf_, "ka"sv);
    ASSERT_TRUE(ka);
    auto ver_a = BlockVersion{};
    deserialize(*ka, ver_a);
    ASSERT_EQ(ver_a.version, 2);
  }
  {
    const auto kb = db->get(active_cf_, "kb"sv);
    ASSERT_TRUE(kb);
    auto ver_b = BlockVersion{};
    deserialize(*kb, ver_b);
    ASSERT_EQ(ver_b.version, 2);
  }
}

TEST_F(versioned_kv_category, delete_last_reachable_with_updates) {
  const auto stale_on_update = false;

  // Add key "ka" in block 1.
  auto out1 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va1", stale_on_update};
    out1 = add(1, std::move(in));
  }

  // Update key "ka" in block 5 (making it stale). Add a new key "kab" in block 5.
  auto out5 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["ka"] = ValueWithFlags{"va5", stale_on_update};
    in.kv["kab"] = ValueWithFlags{"vab5", stale_on_update};
    out5 = add(5, std::move(in));
  }

  // Delete last reachable block 5.
  {
    auto batch = db->getBatch();
    cat.deleteLastReachableBlock(5, out5, batch);
    db->write(std::move(batch));
  }

  // Make sure the latest version of "ka" is 1.
  {
    const auto latest_version = cat.getLatestVersion("ka");
    ASSERT_TRUE(latest_version);
    ASSERT_EQ(*latest_version, 1);

    const auto value = cat.getLatest("ka");
    ASSERT_EQ(asVersioned(value).block_id, 1);
    ASSERT_EQ(asVersioned(value).data, "va1");
  }

  // Make sure there's no trace of "kab".
  {
    ASSERT_FALSE(cat.get("kab", 5));
    ASSERT_FALSE(cat.getLatest("kab"));
    ASSERT_FALSE(cat.getLatestVersion("kab"));
  }

  // Delete last reachable block 1.
  {
    auto batch = db->getBatch();
    cat.deleteLastReachableBlock(1, out1, batch);
    db->write(std::move(batch));
  }

  // Make sure the are no keys left.
  {
    auto values_iter = db->getIterator(values_cf);
    values_iter.first();
    ASSERT_FALSE(values_iter);

    auto latest_ver_iter = db->getIterator(latest_ver_cf);
    latest_ver_iter.first();
    ASSERT_FALSE(latest_ver_iter);
  }
}

TEST_F(versioned_kv_category, delete_last_reachable_with_deletes) {
  const auto stale_on_update = false;

  // Add key "k" in block 1.
  auto out1 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k"] = ValueWithFlags{"v1", stale_on_update};
    out1 = add(1, std::move(in));
  }

  // Delete key "k" in block 3.
  auto out3 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.deletes.push_back("k");
    out3 = add(3, std::move(in));
  }

  // Update key "k" in block 5.
  auto out5 = VersionedOutput{};
  {
    auto in = VersionedInput{};
    in.calculate_root_hash = true;
    in.kv["k"] = ValueWithFlags{"v5", stale_on_update};
    out5 = add(5, std::move(in));
  }

  // The latest version is 5.
  {
    const auto latest = cat.getLatestVersion("k");
    ASSERT_TRUE(latest);
    ASSERT_FALSE(latest->deleted);
    ASSERT_EQ(latest->version, 5);
  }

  // Delete last reachable block 5.
  {
    auto batch = db->getBatch();
    cat.deleteLastReachableBlock(5, out5, batch);
    db->write(std::move(batch));
  }

  // Make sure the latest version is 3 - the version key "k" was deleted at.
  {
    const auto latest = cat.getLatestVersion("k");
    ASSERT_TRUE(latest);
    ASSERT_TRUE(latest->deleted);
    ASSERT_EQ(latest->version, 3);

    auto versions = std::vector<std::optional<TaggedVersion>>{};
    cat.multiGetLatestVersion({"k"}, versions);
    ASSERT_EQ(versions.size(), 1);
    ASSERT_TRUE(versions[0]);
    ASSERT_TRUE(versions[0]->deleted);
    ASSERT_EQ(versions[0]->version, 3);
  }

  // Delete last reachable block 3.
  {
    auto batch = db->getBatch();
    cat.deleteLastReachableBlock(3, out3, batch);
    db->write(std::move(batch));
  }

  // Make sure the latest version is 1.
  {
    const auto latest = cat.getLatestVersion("k");
    ASSERT_TRUE(latest);
    ASSERT_FALSE(latest->deleted);
    ASSERT_EQ(latest->version, 1);

    auto versions = std::vector<std::optional<TaggedVersion>>{};
    cat.multiGetLatestVersion({"k"}, versions);
    ASSERT_EQ(versions.size(), 1);
    ASSERT_TRUE(versions[0]);
    ASSERT_FALSE(versions[0]->deleted);
    ASSERT_EQ(versions[0]->version, 1);
  }
}

}  // namespace

int main(int argc, char *argv[]) {
  ::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
