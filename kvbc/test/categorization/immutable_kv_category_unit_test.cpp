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
#include "categorization/immutable_kv_category.h"
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

class immutable_kv_category : public Test {
  void SetUp() override {
    cleanup();
    db = TestRocksDb::createNative();
  }
  void TearDown() override { cleanup(); }

 protected:
  const std::string category_id{"cat"};
  const std::string column_family{category_id + IMMUTABLE_KV_CF_SUFFIX};
  std::shared_ptr<NativeClient> db;
};

TEST_F(immutable_kv_category, create_column_family_on_construction) {
  auto cat = ImmutableKeyValueCategory{category_id, db};
  ASSERT_THAT(db->columnFamilies(),
              ContainerEq(std::unordered_set<std::string>{db->defaultColumnFamily(), column_family}));
}

TEST_F(immutable_kv_category, empty_updates) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  // Calculate root hash = false.
  {
    auto update = ImmutableUpdatesData{};
    update.calculate_root_hash = false;
    auto batch = db->getBatch();
    const auto update_info = cat.add(1, std::move(update), batch);
    ASSERT_EQ(batch.count(), 0);
    ASSERT_FALSE(update_info.tag_root_hashes);
    ASSERT_TRUE(update_info.tagged_keys.empty());
  }

  // Calculate root hash = true.
  {
    auto update = ImmutableUpdatesData{};
    update.calculate_root_hash = true;
    auto batch = db->getBatch();
    const auto update_info = cat.add(1, std::move(update), batch);
    ASSERT_EQ(batch.count(), 0);
    ASSERT_FALSE(update_info.tag_root_hashes);
    ASSERT_TRUE(update_info.tagged_keys.empty());
  }
}

TEST_F(immutable_kv_category, calculate_root_hash_toggle) {
  auto cat = ImmutableKeyValueCategory{category_id, db};
  auto batch = db->getBatch();

  {
    auto update = ImmutableUpdatesData{};
    update.calculate_root_hash = true;
    update.kv["k"] = ImmutableValueUpdate{"v", {"t"}};
    const auto update_info = cat.add(1, std::move(update), batch);
    ASSERT_TRUE(update_info.tag_root_hashes);
  }

  {
    auto update = ImmutableUpdatesData{};
    update.calculate_root_hash = false;
    update.kv["k"] = ImmutableValueUpdate{"v", {"t"}};
    const auto update_info = cat.add(1, std::move(update), batch);
    ASSERT_FALSE(update_info.tag_root_hashes);
  }
}

TEST_F(immutable_kv_category, non_existent_key) {
  auto cat = ImmutableKeyValueCategory{category_id, db};
  ASSERT_FALSE(cat.get("non-existent"s));
}

TEST_F(immutable_kv_category, key_without_tags) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  const auto block_id = 42;
  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {}};
  update.kv["k2"] = ImmutableValueUpdate{"v2", {"t2"}};
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  // Verify DB.
  {
    const auto ser = db->get(column_family, "k1"sv);
    ASSERT_TRUE(ser);
    auto db_value = ImmutableValue{};
    deserialize(*ser, db_value);
    const auto expected = ImmutableValue{block_id, "v1"};
    ASSERT_EQ(db_value, expected);
  }

  // Verify update info.
  {
    ASSERT_EQ(update_info.tagged_keys.size(), 2);
    ASSERT_EQ(update_info.tagged_keys.cbegin()->first, "k1");
    ASSERT_TRUE(update_info.tagged_keys.cbegin()->second.empty());
    ASSERT_TRUE(update_info.tag_root_hashes);
    ASSERT_EQ(update_info.tag_root_hashes->size(), 1);
  }

  // Verify get().
  {
    const auto value = cat.get("k1"s);
    ASSERT_TRUE(value);
    ASSERT_EQ(value->block_id, block_id);
    ASSERT_EQ(value->data, "v1");
  }
}

TEST_F(immutable_kv_category, one_tag_per_key) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {"t1"}};
  update.kv["k2"] = ImmutableValueUpdate{"v2", {"t2"}};

  const auto block_id = 1;
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  ASSERT_THAT(update_info.tagged_keys,
              ContainerEq(std::map<std::string, std::vector<std::string>>{
                  std::make_pair("k1"s, std::vector<std::string>{{"t1"}}),
                  std::make_pair("k2"s, std::vector<std::string>{{"t2"}})}));
  ASSERT_TRUE(update_info.tag_root_hashes);

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // root_hash = h(h("k1") || h("v1")) = db58ae726159bc3ef4487002a2169b64c4e968f3ea4938da8a62520aa59d9ddb
  {
    auto it = update_info.tag_root_hashes->find("t1");
    ASSERT_NE(it, update_info.tag_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0xdb, 0x58, 0xae, 0x72, 0x61, 0x59, 0xbc, 0x3e, 0xf4, 0x48, 0x70,
                                             0x02, 0xa2, 0x16, 0x9b, 0x64, 0xc4, 0xe9, 0x68, 0xf3, 0xea, 0x49,
                                             0x38, 0xda, 0x8a, 0x62, 0x52, 0x0a, 0xa5, 0x9d, 0x9d, 0xdb}));
  }

  // h("k2") = 189284195f920d885bc46edf2d6c2c56194d3333448eda64ddd726c901b59c28
  // h("v2") = 86a74b56a4ca89e2a292dc3995a15149a2843b038965d0feabd3d20a663f759f
  // root_hash = h(h("k2") || h("v2")) = 3c38959dcca140355bc0be13c1ab09aba5c4a74672138639fa1136906b69af02
  {
    auto it = update_info.tag_root_hashes->find("t2");
    ASSERT_NE(it, update_info.tag_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0x3c, 0x38, 0x95, 0x9d, 0xcc, 0xa1, 0x40, 0x35, 0x5b, 0xc0, 0xbe,
                                             0x13, 0xc1, 0xab, 0x09, 0xab, 0xa5, 0xc4, 0xa7, 0x46, 0x72, 0x13,
                                             0x86, 0x39, 0xfa, 0x11, 0x36, 0x90, 0x6b, 0x69, 0xaf, 0x02}));
  }

  // Make sure we've persisted the key-values.
  {
    const auto v1 = cat.get("k1"s);
    ASSERT_TRUE(v1);
    ASSERT_EQ(v1->block_id, block_id);
    ASSERT_EQ(v1->data, "v1");
  }
  {
    const auto v2 = cat.get("k2"s);
    ASSERT_TRUE(v2);
    ASSERT_EQ(v2->block_id, block_id);
    ASSERT_EQ(v2->data, "v2");
  }
}

TEST_F(immutable_kv_category, two_tags_per_key) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {"t1", "t2"}};

  const auto block_id = 2;
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  ASSERT_THAT(update_info.tagged_keys,
              ContainerEq(std::map<std::string, std::vector<std::string>>{
                  std::make_pair("k1"s, std::vector<std::string>{"t1", "t2"})}));
  ASSERT_TRUE(update_info.tag_root_hashes);

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // root_hash = h(h("k1") || h("v1")) =
  // db58ae726159bc3ef4487002a2169b64c4e968f3ea4938da8a62520aa59d9ddb
  {
    auto it = update_info.tag_root_hashes->find("t1");
    ASSERT_NE(it, update_info.tag_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0xdb, 0x58, 0xae, 0x72, 0x61, 0x59, 0xbc, 0x3e, 0xf4, 0x48, 0x70,
                                             0x02, 0xa2, 0x16, 0x9b, 0x64, 0xc4, 0xe9, 0x68, 0xf3, 0xea, 0x49,
                                             0x38, 0xda, 0x8a, 0x62, 0x52, 0x0a, 0xa5, 0x9d, 0x9d, 0xdb}));
  }
  {
    auto it = update_info.tag_root_hashes->find("t2");
    ASSERT_NE(it, update_info.tag_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0xdb, 0x58, 0xae, 0x72, 0x61, 0x59, 0xbc, 0x3e, 0xf4, 0x48, 0x70,
                                             0x02, 0xa2, 0x16, 0x9b, 0x64, 0xc4, 0xe9, 0x68, 0xf3, 0xea, 0x49,
                                             0x38, 0xda, 0x8a, 0x62, 0x52, 0x0a, 0xa5, 0x9d, 0x9d, 0xdb}));
  }

  // Make sure we've persisted the key-value.
  {
    const auto v1 = cat.get("k1"s);
    ASSERT_TRUE(v1);
    ASSERT_EQ(v1->block_id, block_id);
    ASSERT_EQ(v1->data, "v1");
  }
}

TEST_F(immutable_kv_category, one_and_two_keys_per_tag) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {"t1", "t2"}};
  update.kv["k2"] = ImmutableValueUpdate{"v2", {"t1"}};

  const auto block_id = 1;
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  ASSERT_THAT(update_info.tagged_keys,
              ContainerEq(std::map<std::string, std::vector<std::string>>{
                  std::make_pair("k1"s, std::vector<std::string>{"t1", "t2"}),
                  std::make_pair("k2"s, std::vector<std::string>{"t1"})}));
  ASSERT_TRUE(update_info.tag_root_hashes);

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // h("k2") = 189284195f920d885bc46edf2d6c2c56194d3333448eda64ddd726c901b59c28
  // h("v2") = 86a74b56a4ca89e2a292dc3995a15149a2843b038965d0feabd3d20a663f759f
  // root_hash = h(h("k1") || h("v1") || h("k2") || h("v2")) =
  //           = 57ddbd4f1dcab48ea5a6429091549b3f811e0bbe3d14d1f6a1f129cf1acfdb86
  {
    auto it = update_info.tag_root_hashes->find("t1");
    ASSERT_NE(it, update_info.tag_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0x57, 0xdd, 0xbd, 0x4f, 0x1d, 0xca, 0xb4, 0x8e, 0xa5, 0xa6, 0x42,
                                             0x90, 0x91, 0x54, 0x9b, 0x3f, 0x81, 0x1e, 0x0b, 0xbe, 0x3d, 0x14,
                                             0xd1, 0xf6, 0xa1, 0xf1, 0x29, 0xcf, 0x1a, 0xcf, 0xdb, 0x86}));
  }

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // root_hash = h(h("k1") || h("v1")) =
  // db58ae726159bc3ef4487002a2169b64c4e968f3ea4938da8a62520aa59d9ddb
  {
    auto it = update_info.tag_root_hashes->find("t2");
    ASSERT_NE(it, update_info.tag_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0xdb, 0x58, 0xae, 0x72, 0x61, 0x59, 0xbc, 0x3e, 0xf4, 0x48, 0x70,
                                             0x02, 0xa2, 0x16, 0x9b, 0x64, 0xc4, 0xe9, 0x68, 0xf3, 0xea, 0x49,
                                             0x38, 0xda, 0x8a, 0x62, 0x52, 0x0a, 0xa5, 0x9d, 0x9d, 0xdb}));
  }

  // Make sure we've persisted the key-values.
  {
    const auto v1 = cat.get("k1"s);
    ASSERT_TRUE(v1);
    ASSERT_EQ(v1->block_id, block_id);
    ASSERT_EQ(v1->data, "v1");
  }
  {
    const auto v2 = cat.get("k2"s);
    ASSERT_TRUE(v2);
    ASSERT_EQ(v2->block_id, block_id);
    ASSERT_EQ(v2->data, "v2");
  }
}

TEST_F(immutable_kv_category, two_keys_per_tag) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {"t"}};
  update.kv["k2"] = ImmutableValueUpdate{"v2", {"t"}};

  const auto block_id = 1;
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  ASSERT_THAT(
      update_info.tagged_keys,
      ContainerEq(std::map<std::string, std::vector<std::string>>{
          std::make_pair("k1"s, std::vector<std::string>{"t"}), std::make_pair("k2"s, std::vector<std::string>{"t"})}));
  ASSERT_TRUE(update_info.tag_root_hashes);

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // h("k2") = 189284195f920d885bc46edf2d6c2c56194d3333448eda64ddd726c901b59c28
  // h("v2") = 86a74b56a4ca89e2a292dc3995a15149a2843b038965d0feabd3d20a663f759f
  // root_hash = h(h("k1") || h("v1") || h("k2") || h("v2")) =
  //           = 57ddbd4f1dcab48ea5a6429091549b3f811e0bbe3d14d1f6a1f129cf1acfdb86
  {
    auto it = update_info.tag_root_hashes->find("t");
    ASSERT_NE(it, update_info.tag_root_hashes->cend());
    ASSERT_THAT(it->second, ContainerEq(Hash{0x57, 0xdd, 0xbd, 0x4f, 0x1d, 0xca, 0xb4, 0x8e, 0xa5, 0xa6, 0x42,
                                             0x90, 0x91, 0x54, 0x9b, 0x3f, 0x81, 0x1e, 0x0b, 0xbe, 0x3d, 0x14,
                                             0xd1, 0xf6, 0xa1, 0xf1, 0x29, 0xcf, 0x1a, 0xcf, 0xdb, 0x86}));
  }

  // Make sure we've persisted the key-values.
  {
    const auto v1 = cat.get("k1"s);
    ASSERT_TRUE(v1);
    ASSERT_EQ(v1->block_id, block_id);
    ASSERT_EQ(v1->data, "v1");
  }
  {
    const auto v2 = cat.get("k2"s);
    ASSERT_TRUE(v2);
    ASSERT_EQ(v2->block_id, block_id);
    ASSERT_EQ(v2->data, "v2");
  }
}

TEST_F(immutable_kv_category, get_proof_multiple_keys) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {"t"}};
  update.kv["k2"] = ImmutableValueUpdate{"v2", {"t"}};
  update.kv["k3"] = ImmutableValueUpdate{"v3", {"t"}};

  const auto block_id = 42;
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

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
    const auto proof = cat.getProof("t", "k1", update_info);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block_id);
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
    const auto proof = cat.getProof("t", "k2", update_info);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block_id);
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
    const auto proof = cat.getProof("t", "k3", update_info);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block_id);
    ASSERT_EQ(proof->key, "k3");
    ASSERT_EQ(proof->value, "v3");
    ASSERT_EQ(proof->key_value_index, 4);  // indexing starts at 0
    ASSERT_THAT(proof->calculateRootHash(),
                ContainerEq(Hash{0xac, 0xc0, 0x73, 0x12, 0xb8, 0x6d, 0xae, 0xc7, 0xd0, 0xb8, 0xb5,
                                 0x3b, 0xd6, 0x7e, 0x49, 0xd7, 0xc5, 0xc1, 0x67, 0x7b, 0xf1, 0x74,
                                 0x90, 0x49, 0x62, 0xa8, 0x36, 0xeb, 0x9d, 0xb4, 0xa8, 0xcf}));
  }
}

TEST_F(immutable_kv_category, get_proof_multiple_tags) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {"t1", "t2"}};
  update.kv["k2"] = ImmutableValueUpdate{"v2", {"t1", "t2"}};
  update.kv["k3"] = ImmutableValueUpdate{"v3", {"t1"}};

  const auto block_id = 42;
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  // Tag t1:
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
    const auto proof = cat.getProof("t1", "k1", update_info);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block_id);
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
    const auto proof = cat.getProof("t1", "k2", update_info);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block_id);
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
    const auto proof = cat.getProof("t1", "k3", update_info);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block_id);
    ASSERT_EQ(proof->key, "k3");
    ASSERT_EQ(proof->value, "v3");
    ASSERT_EQ(proof->key_value_index, 4);  // indexing starts at 0
    ASSERT_THAT(proof->calculateRootHash(),
                ContainerEq(Hash{0xac, 0xc0, 0x73, 0x12, 0xb8, 0x6d, 0xae, 0xc7, 0xd0, 0xb8, 0xb5,
                                 0x3b, 0xd6, 0x7e, 0x49, 0xd7, 0xc5, 0xc1, 0x67, 0x7b, 0xf1, 0x74,
                                 0x90, 0x49, 0x62, 0xa8, 0x36, 0xeb, 0x9d, 0xb4, 0xa8, 0xcf}));
  }

  // Tag t2:
  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // h("k2") = 189284195f920d885bc46edf2d6c2c56194d3333448eda64ddd726c901b59c28
  // h("v2") = 86a74b56a4ca89e2a292dc3995a15149a2843b038965d0feabd3d20a663f759f
  // root_hash = h(h("k1") || h("v1") || h("k2") || h("v2")) =
  //           = 57ddbd4f1dcab48ea5a6429091549b3f811e0bbe3d14d1f6a1f129cf1acfdb86
  {
    const auto proof = cat.getProof("t2", "k1", update_info);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block_id);
    ASSERT_EQ(proof->key, "k1");
    ASSERT_EQ(proof->value, "v1");
    ASSERT_EQ(proof->key_value_index, 0);  // indexing starts at 0
    ASSERT_THAT(proof->calculateRootHash(),
                ContainerEq(Hash{0x57, 0xdd, 0xbd, 0x4f, 0x1d, 0xca, 0xb4, 0x8e, 0xa5, 0xa6, 0x42,
                                 0x90, 0x91, 0x54, 0x9b, 0x3f, 0x81, 0x1e, 0x0b, 0xbe, 0x3d, 0x14,
                                 0xd1, 0xf6, 0xa1, 0xf1, 0x29, 0xcf, 0x1a, 0xcf, 0xdb, 0x86}));
  }
  {
    const auto proof = cat.getProof("t2", "k2", update_info);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block_id);
    ASSERT_EQ(proof->key, "k2");
    ASSERT_EQ(proof->value, "v2");
    ASSERT_EQ(proof->key_value_index, 2);  // indexing starts at 0
    ASSERT_THAT(proof->calculateRootHash(),
                ContainerEq(Hash{0x57, 0xdd, 0xbd, 0x4f, 0x1d, 0xca, 0xb4, 0x8e, 0xa5, 0xa6, 0x42,
                                 0x90, 0x91, 0x54, 0x9b, 0x3f, 0x81, 0x1e, 0x0b, 0xbe, 0x3d, 0x14,
                                 0xd1, 0xf6, 0xa1, 0xf1, 0x29, 0xcf, 0x1a, 0xcf, 0xdb, 0x86}));
  }
}

TEST_F(immutable_kv_category, get_proof_single_key) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {"t"}};

  const auto block_id = 42;
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  // h("k1") = 89a6df64f1536985fcb7326c0e56f03762b581b2253cf9fadc695c8dbb740a96
  // h("v1") = 9c209c84e360d17dd267fc53a46db30009b9f39ce2b905fa29fbd5fd4c44ea17
  // root_hash = h(h("k1") || h("v1")) =
  //           = db58ae726159bc3ef4487002a2169b64c4e968f3ea4938da8a62520aa59d9ddb
  {
    const auto proof = cat.getProof("t", "k1", update_info);
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->block_id, block_id);
    ASSERT_EQ(proof->key, "k1");
    ASSERT_EQ(proof->value, "v1");
    ASSERT_EQ(proof->key_value_index, 0);  // indexing starts at 0
    ASSERT_THAT(proof->calculateRootHash(),
                ContainerEq(Hash{0xdb, 0x58, 0xae, 0x72, 0x61, 0x59, 0xbc, 0x3e, 0xf4, 0x48, 0x70,
                                 0x02, 0xa2, 0x16, 0x9b, 0x64, 0xc4, 0xe9, 0x68, 0xf3, 0xea, 0x49,
                                 0x38, 0xda, 0x8a, 0x62, 0x52, 0x0a, 0xa5, 0x9d, 0x9d, 0xdb}));
  }
}

TEST_F(immutable_kv_category, get_proof_key_not_tagged) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {"t1"}};
  update.kv["k2"] = ImmutableValueUpdate{"v2", {"t1"}};

  const auto block_id = 42;
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  // Keys are not tagged with "t2".
  ASSERT_FALSE(cat.getProof("t2", "k1", update_info));
  ASSERT_FALSE(cat.getProof("t2", "k1", update_info));
}

TEST_F(immutable_kv_category, get_proof_non_existent_key) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k"] = ImmutableValueUpdate{"v", {"t"}};

  const auto block_id = 42;
  auto batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), batch);
  db->write(std::move(batch));

  ASSERT_FALSE(cat.getProof("t", "non-existent", update_info));
}

TEST_F(immutable_kv_category, delete_block) {
  auto cat = ImmutableKeyValueCategory{category_id, db};

  auto update = ImmutableUpdatesData{};
  update.calculate_root_hash = true;
  update.kv["k1"] = ImmutableValueUpdate{"v1", {"t"}};
  update.kv["k2"] = ImmutableValueUpdate{"v2", {"t"}};

  const auto block_id = 42;
  auto update_batch = db->getBatch();
  const auto update_info = cat.add(block_id, std::move(update), update_batch);
  db->write(std::move(update_batch));

  ASSERT_TRUE(db->get(column_family, "k1"sv));
  ASSERT_TRUE(db->get(column_family, "k2"sv));

  auto delete_batch = db->getBatch();
  cat.deleteBlock(update_info, delete_batch);
  ASSERT_EQ(delete_batch.count(), 2);
  db->write(std::move(delete_batch));

  ASSERT_FALSE(db->get(column_family, "k1"sv));
  ASSERT_FALSE(db->get(column_family, "k2"sv));
}

}  // namespace

int main(int argc, char *argv[]) {
  ::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
