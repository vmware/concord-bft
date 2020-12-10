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

#include <algorithm>
#include <map>
#include <stdexcept>
#include <utility>

using namespace std::literals;

namespace concord::kvbc::categorization::detail {

const Buffer &version(BlockId block_id) {
  static thread_local auto buf = Buffer{};
  buf.clear();
  serialize(buf, Version{block_id});
  return buf;
}

const Buffer &sharedValuePointer(const std::string &category_id) {
  static thread_local auto buf = Buffer{};
  buf.clear();
  serialize(buf, SharedDbValue{SharedDbValuePointer{category_id}});
  return buf;
}

const Buffer &sharedValueData(std::string &&data) {
  static thread_local auto buf = Buffer{};
  buf.clear();
  serialize(buf, SharedDbValue{SharedDbValueData{std::move(data)}});
  return buf;
}

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

std::string dataCf(const std::string &category_id) { return category_id + SHARED_KV_DATA_CF_SUFFIX; }

std::string latestVersionCf(const std::string &category_id) { return category_id + SHARED_KV_LATEST_KEY_VER_CF_SUFFIX; }

SharedKeyValueCategory::SharedKeyValueCategory(const std::shared_ptr<storage::rocksdb::NativeClient> &db) : db_{db} {}

SharedKeyValueUpdatesInfo SharedKeyValueCategory::add(BlockId block_id,
                                                      SharedKeyValueUpdatesData &&update,
                                                      storage::rocksdb::WriteBatch &batch) {
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

    auto [key_it, inserted_key] = update_info.keys.emplace(std::move(key), SharedKeyData{});
    // Make sure we've inserted the key for the first time.
    ConcordAssert(inserted_key);
    auto &key_data = key_it->second;

    const auto value_hash = hash(shared_value.value);
    auto first_category = std::optional<std::string>{std::nullopt};
    for (auto &&category_id : shared_value.category_ids) {
      const auto data_cf = dataCf(category_id);
      const auto latest_version_cf = latestVersionCf(category_id);
      createColumnFamilyIfNotExisting(data_cf);
      createColumnFamilyIfNotExisting(latest_version_cf);

      if (update.calculate_root_hash) {
        updateCategoryHash(category_id, versioned_key.key_hash.value, value_hash, category_hashers);
      }

      // Versioned key-value.
      if (!first_category) {
        batch.put(data_cf, serialize(versioned_key), sharedValueData(std::move(shared_value.value)));
        first_category = category_id;
      } else {
        batch.put(data_cf, serialize(versioned_key), sharedValuePointer(*first_category));
      }

      // Latest version of a key.
      batch.put(latest_version_cf, serialize(versioned_key.key_hash), version(block_id));

      // Accumulate category IDs for keys in the update info.
      key_data.categories.push_back(std::move(category_id));
    }
  }

  // Finish hash calculation per category.
  if (update.calculate_root_hash) {
    auto category_hashes = std::map<std::string, Hash>{};
    for (auto &[category_id, hasher] : category_hashers) {
      category_hashes[category_id] = hasher.finish();
    }
    update_info.category_root_hashes = std::move(category_hashes);
  }
  return update_info;
}

void SharedKeyValueCategory::createColumnFamilyIfNotExisting(const std::string &cf) const {
  if (!db_->hasColumnFamily(cf)) {
    db_->createColumnFamily(cf);
  }
}

}  // namespace concord::kvbc::categorization::detail
