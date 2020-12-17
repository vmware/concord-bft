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

#include "categorization/immutable_kv_category.h"

#include "assertUtils.hpp"
#include "categorization/column_families.h"
#include "categorization/details.h"

#include <algorithm>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

using namespace std::literals;

namespace concord::kvbc::categorization::detail {

// root_hash = h(h(k1) || h(v1) || h(k2) || h(v2) || ... || h(kn) || h(vn))
void updateTagHash(const std::string &tag,
                   const Hash &key_hash,
                   const Hash &value_hash,
                   std::map<std::string, Hasher> &tag_hashers) {
  auto [it, inserted] = tag_hashers.emplace(tag, Hasher{});
  if (inserted) {
    it->second.init();
  }
  it->second.update(key_hash.data(), key_hash.size());
  it->second.update(value_hash.data(), value_hash.size());
}

void finishTagHashes(std::map<std::string, Hasher> &tag_hashers, ImmutableUpdatesInfo &update_info) {
  if (tag_hashers.empty()) {
    return;
  }
  auto tag_root_hashes = std::map<std::string, Hash>{};
  for (auto &[tag, hasher] : tag_hashers) {
    tag_root_hashes[tag] = hasher.finish();
  }
  update_info.tag_root_hashes = std::move(tag_root_hashes);
}

ImmutableKeyValueCategory::ImmutableKeyValueCategory(const std::string &category_id,
                                                     const std::shared_ptr<storage::rocksdb::NativeClient> &db)
    : cf_{category_id + IMMUTABLE_KV_CF_SUFFIX}, db_{db} {
  createColumnFamilyIfNotExisting(cf_, *db_);
}

ImmutableUpdatesInfo ImmutableKeyValueCategory::add(BlockId block_id,
                                                    ImmutableUpdatesData &&update,
                                                    storage::rocksdb::NativeWriteBatch &batch) {
  auto update_info = ImmutableUpdatesInfo{};
  auto tag_hashers = std::map<std::string, Hasher>{};

  for (auto it = update.kv.begin(); it != update.kv.end();) {
    // Save the iterator as extract() invalidates it.
    auto extracted_it = it;
    ++it;
    auto node = update.kv.extract(extracted_it);
    auto &key = node.key();
    auto &value = node.mapped();

    // Calculate hashes (optionally) before moving them.
    Hash key_hash;
    Hash value_hash;
    if (update.calculate_root_hash && !value.tags.empty()) {
      key_hash = hash(key);
      value_hash = hash(value.data);
    }

    // Persist the key-value.
    batch.put(cf_, key, serialize(ImmutableValue{block_id, std::move(value.data)}));

    // Move the key and the tags to the update info and (optionally) update hashes per tag.
    auto &key_tags = update_info.tagged_keys.emplace(std::move(key), std::vector<std::string>{}).first->second;
    for (auto &&tag : value.tags) {
      if (update.calculate_root_hash) {
        updateTagHash(tag, key_hash, value_hash, tag_hashers);
      }
      key_tags.push_back(std::move(tag));
    }
  }

  // Finish hash calculation per tag.
  if (update.calculate_root_hash) {
    finishTagHashes(tag_hashers, update_info);
  }
  return update_info;
}

void ImmutableKeyValueCategory::deleteGenesisBlock(BlockId,
                                                   const ImmutableUpdatesInfo &updates_info,
                                                   storage::rocksdb::NativeWriteBatch &batch) {
  deleteBlock(updates_info, batch);
}

void ImmutableKeyValueCategory::deleteLastReachableBlock(BlockId,
                                                         const ImmutableUpdatesInfo &updates_info,
                                                         storage::rocksdb::NativeWriteBatch &batch) {
  deleteBlock(updates_info, batch);
}

void ImmutableKeyValueCategory::deleteBlock(const ImmutableUpdatesInfo &updates_info,
                                            storage::rocksdb::NativeWriteBatch &batch) {
  for (const auto &kv : updates_info.tagged_keys) {
    batch.del(cf_, kv.first);
  }
}

std::optional<ImmutableValue> ImmutableKeyValueCategory::get(const std::string &key) const {
  const auto ser = db_->get(cf_, key);
  if (!ser) {
    return std::nullopt;
  }
  auto value = ImmutableValue{};
  deserialize(*ser, value);
  return value;
}

std::optional<KeyValueProof> ImmutableKeyValueCategory::getProof(const std::string &tag,
                                                                 const std::string &key,
                                                                 const ImmutableUpdatesInfo &updates_info) const {
  // If the key is not part of this block, return a null proof.
  auto key_it = updates_info.tagged_keys.find(key);
  if (key_it == updates_info.tagged_keys.cend()) {
    return std::nullopt;
  }

  // If the key is not tagged with `tag`, return a null proof.
  if (const auto &key_tags = key_it->second; std::find(key_tags.cbegin(), key_tags.cend(), tag) == key_tags.cend()) {
    return std::nullopt;
  }

  auto value = get(key);
  ConcordAssert(value.has_value());

  auto proof = KeyValueProof{};
  proof.block_id = value->block_id;
  proof.key = key;
  proof.value = std::move(value->data);

  auto i = std::size_t{0};
  for (const auto &[update_key, update_key_tags] : updates_info.tagged_keys) {
    if (update_key == key) {
      proof.key_value_index = i;
      continue;
    }

    if (std::find(update_key_tags.cbegin(), update_key_tags.cend(), tag) == update_key_tags.cend()) {
      continue;
    }

    const auto update_value = get(update_key);
    ConcordAssert(update_value.has_value());

    proof.ordered_complement_kv_hashes.push_back(hash(update_key));
    proof.ordered_complement_kv_hashes.push_back(hash(update_value->data));

    // Increment by 2 as we push 2 hashes - one of the key and one of the value.
    i += 2;
  }

  return proof;
}

}  // namespace concord::kvbc::categorization::detail
