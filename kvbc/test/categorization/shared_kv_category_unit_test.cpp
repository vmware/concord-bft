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

#include "categorization/column_families.h"
#include "categorization/details.h"
#include "categorization/shared_kv_category.h"
#include "kv_types.hpp"
#include "rocksdb/native_client.h"
#include "storage/test/storage_test_common.h"

#include <memory>
#include <optional>
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

class shared_kv_category_test : public Test {
  void SetUp() override {
    cleanup();
    db = TestRocksDb::createNative();
  }
  void TearDown() override { cleanup(); }

 protected:
  std::optional<std::string> value(const std::string &key, BlockId block_id) const {
    return db->get(SHARED_KV_DATA_CF, serialize(versionedKey(key, block_id)));
  }

  std::optional<KeyVersionsPerCategory> keyVersions(const std::string &key) const {
    const auto db_value = db->get(SHARED_KV_KEY_VERSIONS_CF, hash(key));
    if (!db_value) {
      return std::nullopt;
    }
    auto versions = KeyVersionsPerCategory{};
    deserialize(*db_value, versions);
    return versions;
  }

 protected:
  std::shared_ptr<NativeClient> db;
};

TEST_F(shared_kv_category_test, create_column_families_on_construction) {
  auto cat = SharedKeyValueCategory{db};
  ASSERT_THAT(db->columnFamilies(),
              ContainerEq(std::unordered_set<std::string>{
                  db->defaultColumnFamily(), SHARED_KV_DATA_CF, SHARED_KV_KEY_VERSIONS_CF}));
}

TEST_F(shared_kv_category_test, empty_updates) {
  auto update = SharedKeyValueUpdatesData{};
  update.calculate_root_hash = true;
  auto batch = db->getBatch();
  auto cat = SharedKeyValueCategory{db};
  ASSERT_THROW(cat.add(1, std::move(update), batch), std::invalid_argument);
}

TEST_F(shared_kv_category_test, key_without_categories) {
  auto update = SharedKeyValueUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = SharedValueData{"v1", {"c1"}};
  update.kv["k2"] = SharedValueData{"v2", {}};
  auto batch = db->getBatch();
  auto cat = SharedKeyValueCategory{db};
  ASSERT_THROW(cat.add(1, std::move(update), batch), std::invalid_argument);
}

TEST_F(shared_kv_category_test, calculate_root_hash_toggle) {
  auto batch = db->getBatch();
  auto cat = SharedKeyValueCategory{db};

  {
    auto update = SharedKeyValueUpdatesData{};
    update.calculate_root_hash = true;
    update.kv["k1"] = SharedValueData{"v1", {"c1"}};
    const auto update_info = cat.add(1, std::move(update), batch);
    ASSERT_TRUE(update_info.category_root_hashes);
  }

  {
    auto update = SharedKeyValueUpdatesData{};
    update.calculate_root_hash = false;
    update.kv["k1"] = SharedValueData{"v1", {"c1"}};
    const auto update_info = cat.add(1, std::move(update), batch);
    ASSERT_FALSE(update_info.category_root_hashes);
  }
}

TEST_F(shared_kv_category_test, add_one_key_per_category) {
  auto update = SharedKeyValueUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = SharedValueData{"v1", {"c1"}};
  update.kv["k2"] = SharedValueData{"v2", {"c2"}};

  const auto block_id = 1;
  auto batch = db->getBatch();
  auto cat = SharedKeyValueCategory{db};
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  ASSERT_THAT(update_info.keys,
              ContainerEq(std::map<std::string, SharedKeyData>{std::make_pair("k1"s, SharedKeyData{{"c1"}}),
                                                               std::make_pair("k2"s, SharedKeyData{{"c2"}})}));
  ASSERT_TRUE(update_info.category_root_hashes);

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // root_hash = h(h("k1") || h("v1")) = db58ae726159bc3ef4487002a2169b64c4e968f3ea4938da8a62520aa59d9ddb
  {
    auto it = update_info.category_root_hashes->find("c1");
    ASSERT_NE(it, update_info.category_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0xdb, 0x58, 0xae, 0x72, 0x61, 0x59, 0xbc, 0x3e, 0xf4, 0x48, 0x70,
                                             0x02, 0xa2, 0x16, 0x9b, 0x64, 0xc4, 0xe9, 0x68, 0xf3, 0xea, 0x49,
                                             0x38, 0xda, 0x8a, 0x62, 0x52, 0x0a, 0xa5, 0x9d, 0x9d, 0xdb}));
  }

  // h("k2") = 189284195f920d885bc46edf2d6c2c56194d3333448eda64ddd726c901b59c28
  // h("v2") = 86a74b56a4ca89e2a292dc3995a15149a2843b038965d0feabd3d20a663f759f
  // root_hash = h(h("k2") || h("v2")) = 3c38959dcca140355bc0be13c1ab09aba5c4a74672138639fa1136906b69af02
  {
    auto it = update_info.category_root_hashes->find("c2");
    ASSERT_NE(it, update_info.category_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0x3c, 0x38, 0x95, 0x9d, 0xcc, 0xa1, 0x40, 0x35, 0x5b, 0xc0, 0xbe,
                                             0x13, 0xc1, 0xab, 0x09, 0xab, 0xa5, 0xc4, 0xa7, 0x46, 0x72, 0x13,
                                             0x86, 0x39, 0xfa, 0x11, 0x36, 0x90, 0x6b, 0x69, 0xaf, 0x02}));
  }

  // Make sure we've persisted the key-values in the data column family.
  {
    const auto v1 = value("k1", block_id);
    ASSERT_TRUE(v1);
    ASSERT_EQ(*v1, "v1");
  }
  {
    const auto v2 = value("k2", block_id);
    ASSERT_TRUE(v2);
    ASSERT_EQ(*v2, "v2");
  }

  // Make sure we've persisted versions in the key versions column family.
  {
    const auto versions = keyVersions("k1");
    ASSERT_TRUE(versions);
    ASSERT_THAT(versions->data,
                ContainerEq(std::map<std::string, std::vector<std::uint64_t>>{
                    std::make_pair("c1"s, std::vector<std::uint64_t>{block_id})}));
  }
  {
    const auto versions = keyVersions("k2");
    ASSERT_TRUE(versions);
    ASSERT_THAT(versions->data,
                ContainerEq(std::map<std::string, std::vector<std::uint64_t>>{
                    std::make_pair("c2"s, std::vector<std::uint64_t>{block_id})}));
  }
}

TEST_F(shared_kv_category_test, add_key_in_two_categories) {
  auto update = SharedKeyValueUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = SharedValueData{"v1", {"c1", "c2"}};

  const auto block_id = 2;
  auto batch = db->getBatch();
  auto cat = SharedKeyValueCategory{db};
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  ASSERT_THAT(update_info.keys,
              ContainerEq(std::map<std::string, SharedKeyData>{std::make_pair("k1"s, SharedKeyData{{"c1", "c2"}})}));

  ASSERT_TRUE(update_info.category_root_hashes);

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // root_hash = h(h("k1") || h("v1")) = db58ae726159bc3ef4487002a2169b64c4e968f3ea4938da8a62520aa59d9ddb
  {
    auto it = update_info.category_root_hashes->find("c1");
    ASSERT_NE(it, update_info.category_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0xdb, 0x58, 0xae, 0x72, 0x61, 0x59, 0xbc, 0x3e, 0xf4, 0x48, 0x70,
                                             0x02, 0xa2, 0x16, 0x9b, 0x64, 0xc4, 0xe9, 0x68, 0xf3, 0xea, 0x49,
                                             0x38, 0xda, 0x8a, 0x62, 0x52, 0x0a, 0xa5, 0x9d, 0x9d, 0xdb}));
  }

  {
    auto it = update_info.category_root_hashes->find("c2");
    ASSERT_NE(it, update_info.category_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0xdb, 0x58, 0xae, 0x72, 0x61, 0x59, 0xbc, 0x3e, 0xf4, 0x48, 0x70,
                                             0x02, 0xa2, 0x16, 0x9b, 0x64, 0xc4, 0xe9, 0x68, 0xf3, 0xea, 0x49,
                                             0x38, 0xda, 0x8a, 0x62, 0x52, 0x0a, 0xa5, 0x9d, 0x9d, 0xdb}));
  }

  // Make sure we've persisted the key-value in the data column family.
  {
    const auto v1 = value("k1", block_id);
    ASSERT_TRUE(v1);
    ASSERT_EQ(*v1, "v1");
  }

  // Make sure we've persisted versions in the key versions column family.
  {
    const auto versions = keyVersions("k1");
    ASSERT_TRUE(versions);
    ASSERT_THAT(versions->data,
                ContainerEq(std::map<std::string, std::vector<std::uint64_t>>{
                    std::make_pair("c1"s, std::vector<std::uint64_t>{block_id}),
                    std::make_pair("c2"s, std::vector<std::uint64_t>{block_id})}));
  }
}

TEST_F(shared_kv_category_test, add_two_keys_in_one_category) {
  auto update = SharedKeyValueUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = SharedValueData{"v1", {"c1"}};
  update.kv["k2"] = SharedValueData{"v2", {"c1"}};

  const auto block_id = 1;
  auto batch = db->getBatch();
  auto cat = SharedKeyValueCategory{db};
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  ASSERT_THAT(update_info.keys,
              ContainerEq(std::map<std::string, SharedKeyData>{std::make_pair("k1"s, SharedKeyData{{"c1"}}),
                                                               std::make_pair("k2"s, SharedKeyData{{"c1"}})}));

  ASSERT_TRUE(update_info.category_root_hashes);

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // h("k2") = 189284195f920d885bc46edf2d6c2c56194d3333448eda64ddd726c901b59c28
  // h("v2") = 86a74b56a4ca89e2a292dc3995a15149a2843b038965d0feabd3d20a663f759f
  // root_hash = h(h("k1") || h("v1") || h("k2") || h("v2")) =
  //           = 57ddbd4f1dcab48ea5a6429091549b3f811e0bbe3d14d1f6a1f129cf1acfdb86
  {
    auto it = update_info.category_root_hashes->find("c1");
    ASSERT_NE(it, update_info.category_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0x57, 0xdd, 0xbd, 0x4f, 0x1d, 0xca, 0xb4, 0x8e, 0xa5, 0xa6, 0x42,
                                             0x90, 0x91, 0x54, 0x9b, 0x3f, 0x81, 0x1e, 0x0b, 0xbe, 0x3d, 0x14,
                                             0xd1, 0xf6, 0xa1, 0xf1, 0x29, 0xcf, 0x1a, 0xcf, 0xdb, 0x86}));
  }

  // Make sure we've persisted the key-values in the data column family.
  {
    const auto v1 = value("k1", block_id);
    ASSERT_TRUE(v1);
    ASSERT_EQ(*v1, "v1");
  }
  {
    const auto v2 = value("k2", block_id);
    ASSERT_TRUE(v2);
    ASSERT_EQ(*v2, "v2");
  }

  // Make sure we've persisted versions in the key versions column family.
  {
    const auto versions = keyVersions("k1");
    ASSERT_TRUE(versions);
    ASSERT_THAT(versions->data,
                ContainerEq(std::map<std::string, std::vector<std::uint64_t>>{
                    std::make_pair("c1"s, std::vector<std::uint64_t>{block_id})}));
  }
  {
    const auto versions = keyVersions("k2");
    ASSERT_TRUE(versions);
    ASSERT_THAT(versions->data,
                ContainerEq(std::map<std::string, std::vector<std::uint64_t>>{
                    std::make_pair("c1"s, std::vector<std::uint64_t>{block_id})}));
  }
}

TEST_F(shared_kv_category_test, multi_versioned_key_in_one_category) {
  auto cat = SharedKeyValueCategory{db};

  // Block 1.
  {
    auto update1 = SharedKeyValueUpdatesData{};
    update1.calculate_root_hash = true;
    update1.kv["k"] = SharedValueData{"v1", {"c"}};

    auto batch1 = db->getBatch();
    cat.add(1, std::move(update1), batch1);
    db->write(std::move(batch1));
  }

  // Block 2.
  {
    auto update2 = SharedKeyValueUpdatesData{};
    update2.calculate_root_hash = true;
    update2.kv["k"] = SharedValueData{"v2", {"c"}};

    auto batch2 = db->getBatch();
    cat.add(2, std::move(update2), batch2);
    db->write(std::move(batch2));
  }

  // Make sure we've persisted the key-values in the data column family.
  {
    const auto v1 = value("k", 1);
    ASSERT_TRUE(v1);
    ASSERT_EQ(*v1, "v1");
  }
  {
    const auto v2 = value("k", 2);
    ASSERT_TRUE(v2);
    ASSERT_EQ(*v2, "v2");
  }

  // Make sure we've persisted versions in the key versions column family.
  {
    const auto versions = keyVersions("k");
    ASSERT_TRUE(versions);
    ASSERT_THAT(versions->data,
                ContainerEq(std::map<std::string, std::vector<std::uint64_t>>{
                    std::make_pair("c"s, std::vector<std::uint64_t>{1, 2})}));
  }
}

}  // namespace

int main(int argc, char *argv[]) {
  ::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
