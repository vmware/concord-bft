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
#include "kvbc_adapter/replica_adapter.hpp"
#include "categorization/updates.h"
#include "categorization/db_categories.h"
#include "kvbc_key_types.hpp"
#include <iostream>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>
#include "storage/test/storage_test_common.h"
#include "kvbc_adapter/common/state_snapshot_adapter.hpp"

using concord::storage::rocksdb::NativeClient;
using namespace ::testing;

namespace {
class common_kvbc : public Test {
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

  void addPublicState(concord::kvbc::adapter::ReplicaBlockchain& kvbc) {
    auto updates = concord::kvbc::categorization::Updates{};
    auto merkle = concord::kvbc::categorization::BlockMerkleUpdates{};
    merkle.addUpdate("a", "va");
    merkle.addUpdate("b", "vb");
    merkle.addUpdate("c", "vc");
    merkle.addUpdate("d", "vd");
    auto versioned = concord::kvbc::categorization::VersionedUpdates{};
    const auto public_state =
        concord::kvbc::categorization::PublicStateKeys{std::vector<std::string>{"a", "b", "c", "d"}};
    const auto ser_public_state = concord::kvbc::categorization::detail::serialize(public_state);
    versioned.addUpdate(std::string{concord::kvbc::keyTypes::state_public_key_set},
                        std::string{ser_public_state.cbegin(), ser_public_state.cend()});
    updates.add(concord::kvbc::categorization::kExecutionProvableCategory, std::move(merkle));
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(versioned));
    ASSERT_EQ(kvbc.add(std::move(updates)), 1);
  }

  // Public state hash computed via https://emn178.github.io/online-tools/sha3_256.html
  //
  // h0 = hash("") = a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a
  // h1 = hash(h0 || hash("a") || "va") = c407fb7c52596d6d0fa0a013798dd72489dcc00607033089a8a09c73fba8bd7c
  // h2 = hash(h1 || hash("b") || "vb") = 1d911b3b893221358c22f2470fac0864ef145b55eca983d4b17bdc30bf0bda2b
  // h3 = hash(h2 || hash("c") || "vc") = c6314bdd9c82183d2e4e5cb8869826e1a3ace6a86a7b62ebe5ac77b732d3c792
  // h4 = hash(h3 || hash("d") || "vd") = fd4c5ea03da18deaf10365fdf001c216055aaaa796b0a98e4db7c756a426ae81
  void assertPublicStateHash() {
    const auto state_hash_val = db->get(concord::kvbc::bcutil::BlockChainUtils::publicStateHashKey());
    ASSERT_TRUE(state_hash_val.has_value());
    auto state_hash = concord::kvbc::categorization::StateHash{};
    concord::kvbc::categorization::detail::deserialize(*state_hash_val, state_hash);
    ASSERT_EQ(state_hash.block_id, 1);
    // Expect h4.
    ASSERT_THAT(state_hash.hash,
                ContainerEq(concord::kvbc::categorization::Hash{
                    0xfd, 0x4c, 0x5e, 0xa0, 0x3d, 0xa1, 0x8d, 0xea, 0xf1, 0x03, 0x65, 0xfd, 0xf0, 0x01, 0xc2, 0x16,
                    0x05, 0x5a, 0xaa, 0xa7, 0x96, 0xb0, 0xa9, 0x8e, 0x4d, 0xb7, 0xc7, 0x56, 0xa4, 0x26, 0xae, 0x81}));
  }

  std::optional<concord::kvbc::BLOCKCHAIN_VERSION> getBlockchainVersion(int32_t ver) {
    switch (ver) {
      case 1:
        return concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN;
      case 4:
        return concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN;
      default:
        break;
    }
    return std::nullopt;
  }

 protected:
  std::shared_ptr<NativeClient> db;
};

TEST_F(common_kvbc, compute_and_persist_hash_with_no_public_state) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = false;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{db, link_st_chain, cat_map};
          ASSERT_EQ(kvbc.add(concord::kvbc::categorization::Updates{}), 1);
          kvbc.computeAndPersistPublicStateHash(1);
          const auto state_hash_val = db->get(concord::kvbc::bcutil::BlockChainUtils::publicStateHashKey());
          ASSERT_TRUE(state_hash_val.has_value());
          auto state_hash = concord::kvbc::categorization::StateHash{};
          concord::kvbc::categorization::detail::deserialize(*state_hash_val, state_hash);
          ASSERT_EQ(state_hash.block_id, 1);
          // Expect the empty SHA3-256 hash.
          ASSERT_THAT(
              state_hash.hash,
              ContainerEq(concord::kvbc::categorization::Hash{
                  0xa7, 0xff, 0xc6, 0xf8, 0xbf, 0x1e, 0xd7, 0x66, 0x51, 0xc1, 0x47, 0x56, 0xa0, 0x61, 0xd6, 0x62,
                  0xf5, 0x80, 0xff, 0x4d, 0xe4, 0x3b, 0x49, 0xfa, 0x82, 0xd8, 0x0a, 0x4b, 0x80, 0xf8, 0x43, 0x4a}));
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

TEST_F(common_kvbc, compute_and_persist_hash_batch_size_equals_key_count) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = true;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{
              db,
              link_st_chain,
              std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
                  {concord::kvbc::categorization::kExecutionProvableCategory,
                   concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
                  {concord::kvbc::categorization::kConcordInternalCategoryId,
                   concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv}}};
          addPublicState(kvbc);
          bftEngine::ReplicaConfig::instance().stateIterationMultiGetBatchSize = 4;
          kvbc.computeAndPersistPublicStateHash(1);
          assertPublicStateHash();
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

TEST_F(common_kvbc, compute_and_persist_hash_batch_size_bigger_than_key_count_uneven) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = true;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{
              db,
              link_st_chain,
              std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
                  {concord::kvbc::categorization::kExecutionProvableCategory,
                   concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
                  {concord::kvbc::categorization::kConcordInternalCategoryId,
                   concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv}}};
          addPublicState(kvbc);
          bftEngine::ReplicaConfig::instance().stateIterationMultiGetBatchSize = 5;
          kvbc.computeAndPersistPublicStateHash(1);
          assertPublicStateHash();
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

TEST_F(common_kvbc, compute_and_persist_hash_batch_size_bigger_than_key_count_even) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = true;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{
              db,
              link_st_chain,
              std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
                  {concord::kvbc::categorization::kExecutionProvableCategory,
                   concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
                  {concord::kvbc::categorization::kConcordInternalCategoryId,
                   concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv}}};
          addPublicState(kvbc);
          bftEngine::ReplicaConfig::instance().stateIterationMultiGetBatchSize = 6;
          kvbc.computeAndPersistPublicStateHash(1);
          assertPublicStateHash();
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

TEST_F(common_kvbc, compute_and_persist_hash_batch_size_less_than_key_count_uneven) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = true;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{
              db,
              link_st_chain,
              std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
                  {concord::kvbc::categorization::kExecutionProvableCategory,
                   concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
                  {concord::kvbc::categorization::kConcordInternalCategoryId,
                   concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv}}};
          addPublicState(kvbc);
          bftEngine::ReplicaConfig::instance().stateIterationMultiGetBatchSize = 3;
          kvbc.computeAndPersistPublicStateHash(1);
          assertPublicStateHash();
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

TEST_F(common_kvbc, compute_and_persist_hash_batch_size_less_than_key_count_even) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = true;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{
              db,
              link_st_chain,
              std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
                  {concord::kvbc::categorization::kExecutionProvableCategory,
                   concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
                  {concord::kvbc::categorization::kConcordInternalCategoryId,
                   concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv}}};
          addPublicState(kvbc);
          bftEngine::ReplicaConfig::instance().stateIterationMultiGetBatchSize = 2;
          kvbc.computeAndPersistPublicStateHash(1);
          assertPublicStateHash();
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

TEST_F(common_kvbc, iterate_partial_public_state) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = true;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{
              db,
              link_st_chain,
              std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
                  {concord::kvbc::categorization::kExecutionProvableCategory,
                   concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
                  {concord::kvbc::categorization::kConcordInternalCategoryId,
                   concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv}}};
          addPublicState(kvbc);
          auto iterated_key_values = std::vector<std::pair<std::string, std::string>>{};
          ASSERT_TRUE(kvbc.iteratePublicStateKeyValues(
              [&](std::string&& key, std::string&& value) {
                iterated_key_values.push_back(std::make_pair(key, value));
              },
              "b"));
          ASSERT_THAT(iterated_key_values,
                      ContainerEq(std::vector<std::pair<std::string, std::string>>{{"c", "vc"}, {"d", "vd"}}));
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

TEST_F(common_kvbc, iterate_public_state_after_first_key) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = true;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{
              db,
              link_st_chain,
              std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
                  {concord::kvbc::categorization::kExecutionProvableCategory,
                   concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
                  {concord::kvbc::categorization::kConcordInternalCategoryId,
                   concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv}}};
          addPublicState(kvbc);
          auto iterated_key_values = std::vector<std::pair<std::string, std::string>>{};
          ASSERT_TRUE(kvbc.iteratePublicStateKeyValues(
              [&](std::string&& key, std::string&& value) {
                iterated_key_values.push_back(std::make_pair(key, value));
              },
              "a"));
          ASSERT_THAT(
              iterated_key_values,
              ContainerEq(std::vector<std::pair<std::string, std::string>>{{"b", "vb"}, {"c", "vc"}, {"d", "vd"}}));
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

TEST_F(common_kvbc, iterate_public_state_after_last_key) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = true;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{
              db,
              link_st_chain,
              std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
                  {concord::kvbc::categorization::kExecutionProvableCategory,
                   concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
                  {concord::kvbc::categorization::kConcordInternalCategoryId,
                   concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv}}};
          addPublicState(kvbc);
          auto iterated_key_values = std::vector<std::pair<std::string, std::string>>{};
          ASSERT_TRUE(kvbc.iteratePublicStateKeyValues(
              [&](std::string&& key, std::string&& value) {
                iterated_key_values.push_back(std::make_pair(key, value));
              },
              "d"));
          ASSERT_TRUE(iterated_key_values.empty());
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

TEST_F(common_kvbc, iterate_public_state_key_not_found) {
  bool version_is_set = false;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> cat_map;
  for (int32_t ver = 0; ver <= static_cast<int32_t>(concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION);
       ++ver) {
    auto blockchain_version = getBlockchainVersion(ver);
    if (!blockchain_version) {
      continue;
    }
    switch (*blockchain_version) {
      case concord::kvbc::BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace(concord::kvbc::categorization::kExecutionProvableCategory,
                          concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
      case concord::kvbc::BLOCKCHAIN_VERSION::V4_BLOCKCHAIN:
        if (!version_is_set) {
          bftEngine::ReplicaConfig::instance().kvBlockchainVersion = static_cast<uint32_t>(ver);
          version_is_set = true;
          cat_map.emplace("merkle", concord::kvbc::categorization::CATEGORY_TYPE::block_merkle);
          cat_map.emplace("versioned", concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
          cat_map.emplace(concord::kvbc::categorization::kConcordInternalCategoryId,
                          concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv);
        }
        {
          const auto link_st_chain = true;
          auto kvbc = concord::kvbc::adapter::ReplicaBlockchain{
              db,
              link_st_chain,
              std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
                  {concord::kvbc::categorization::kExecutionProvableCategory,
                   concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
                  {concord::kvbc::categorization::kConcordInternalCategoryId,
                   concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv}}};
          addPublicState(kvbc);
          auto iterated_key_values = std::vector<std::pair<std::string, std::string>>{};
          ASSERT_FALSE(kvbc.iteratePublicStateKeyValues(
              [&](std::string&& key, std::string&& value) {
                iterated_key_values.push_back(std::make_pair(key, value));
              },
              "NON-EXISTENT"));
          ASSERT_TRUE(iterated_key_values.empty());
        }
        version_is_set = false;
        break;
      case concord::kvbc::BLOCKCHAIN_VERSION::INVALID_BLOCKCHAIN_VERSION:
        version_is_set = false;
        break;
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}