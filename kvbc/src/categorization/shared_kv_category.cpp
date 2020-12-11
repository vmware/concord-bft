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

#include "categorization/shared_kv_category.h"

#include "assertUtils.hpp"
#include "categorization/column_families.h"
#include "categorization/details.h"

#include <map>
#include <stdexcept>
#include <string_view>
#include <utility>

using namespace std::literals;

namespace concord::kvbc::categorization::detail {

// root_hash = h((h(k1) || h(v1)) || (h(k2) || h(v2)) || ... || (h(k3) || h(v3)))
void updateCategoryHash(const std::string &category_id,
                        const Hash &key_hash,
                        const Hash &value_hash,
                        std::map<std::string, Hasher> &category_hashers) {
  auto [it, inserted] = category_hashers.emplace(category_id, Hasher{});
  if (inserted) {
    it->second.init();
  }
  it->second.update(key_hash.data(), key_hash.size());
  it->second.update(value_hash.data(), value_hash.size());
}

void finishCategoryHashes(std::map<std::string, Hasher> &category_hashers, SharedKeyValueUpdatesInfo &update_info) {
  auto category_hashes = std::map<std::string, Hash>{};
  for (auto &[category_id, hasher] : category_hashers) {
    category_hashes[category_id] = hasher.finish();
  }
  update_info.category_root_hashes = std::move(category_hashes);
}

SharedKeyValueCategory::SharedKeyValueCategory(const std::shared_ptr<storage::rocksdb::NativeClient> &db) : db_{db} {
  createColumnFamilyIfNotExisting(SHARED_KV_DATA_CF, *db_);
  createColumnFamilyIfNotExisting(SHARED_KV_KEY_VERSIONS_CF, *db_);
}

SharedKeyValueUpdatesInfo SharedKeyValueCategory::add(BlockId block_id,
                                                      SharedKeyValueUpdatesData &&update,
                                                      storage::rocksdb::NativeWriteBatch &batch) {
  if (update.kv.empty()) {
    throw std::invalid_argument{"Empty shared key-value updates"};
  }

  auto update_info = SharedKeyValueUpdatesInfo{};
  auto category_hashers = std::map<std::string, Hasher>{};

  for (auto it = update.kv.begin(); it != update.kv.end();) {
    // Save the iterator as extract() invalidates it.
    auto extracted_it = it;
    ++it;
    auto node = update.kv.extract(extracted_it);
    auto &key = node.key();
    auto &shared_value = node.mapped();
    if (shared_value.category_ids.empty()) {
      throw std::invalid_argument{"Empty category ID set for a shared key-value update"};
    }

    const auto versioned_key = versionedKey(key, block_id);
    auto &key_data = update_info.keys.emplace(std::move(key), SharedKeyData{}).first->second;
    auto key_versions = versionsForKey(versioned_key.key_hash.value);
    const auto value_hash = hash(shared_value.value);

    for (auto &&category_id : shared_value.category_ids) {
      if (update.calculate_root_hash) {
        updateCategoryHash(category_id, versioned_key.key_hash.value, value_hash, category_hashers);
      }

      // The value is persisted into the data column family.
      batch.put(SHARED_KV_DATA_CF, serialize(versioned_key), shared_value.value);

      // Append the block ID to the key versions (per category).
      {
        auto &versions_for_cat = key_versions.data[category_id];
        if (!versions_for_cat.empty()) {
          ConcordAssertGT(block_id, versions_for_cat.back());
        }
        versions_for_cat.push_back(block_id);
      }

      // Accumulate category IDs for keys in the update info.
      key_data.categories.push_back(std::move(category_id));
    }

    // Persist the key versions.
    batch.put(SHARED_KV_KEY_VERSIONS_CF, versioned_key.key_hash.value, serialize(key_versions));
  }

  // Finish hash calculation per category.
  if (update.calculate_root_hash) {
    finishCategoryHashes(category_hashers, update_info);
  }
  return update_info;
}

KeyVersionsPerCategory SharedKeyValueCategory::versionsForKey(const Hash &key_hash) const {
  auto key_versions = KeyVersionsPerCategory{};
  const auto key_versions_db_value = db_->get(SHARED_KV_KEY_VERSIONS_CF, key_hash);
  if (key_versions_db_value) {
    deserialize(*key_versions_db_value, key_versions);
  }
  return key_versions;
}

}  // namespace concord::kvbc::categorization::detail
