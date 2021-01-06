// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "categorization/versioned_kv_category.h"

#include "assertUtils.hpp"
#include "categorization/blockchain.h"
#include "categorization/column_families.h"
#include "categorization/details.h"

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <utility>
#include <variant>

namespace concord::kvbc::categorization::detail {

using namespace std::literals;

template <typename Span>
std::optional<std::string> value(const Span &ser) {
  auto db_value = VersionedDbValue{};
  deserialize(ser, db_value);
  if (!std::holds_alternative<DbValue>(db_value.data)) {
    return std::nullopt;
  }
  return std::get<DbValue>(std::move(db_value.data)).data;
}

template <typename Span>
BlockId latestVersion(const Span &ser) {
  auto latest_version = LatestKeyVersion{};
  deserialize(ser, latest_version);
  return latest_version.block_id;
}

VersionedKeyValueCategory::VersionedKeyValueCategory(const std::string &category_id,
                                                     const std::shared_ptr<storage::rocksdb::NativeClient> &db)
    : values_cf_{category_id + VERSIONED_KV_VALUES_CF_SUFFIX},
      latest_ver_cf_{category_id + VERSIONED_KV_LATEST_VER_CF_SUFFIX},
      db_{db} {
  createColumnFamilyIfNotExisting(values_cf_, *db_);
  createColumnFamilyIfNotExisting(latest_ver_cf_, *db_);
}

VersionedOutput VersionedKeyValueCategory::add(BlockId block_id,
                                               VersionedInput &&in,
                                               storage::rocksdb::NativeWriteBatch &batch) {
  auto out = VersionedOutput{};
  addDeletes(block_id, std::move(in.deletes), out, batch);
  addUpdates(block_id, in.calculate_root_hash, std::move(in.kv), out, batch);
  return out;
}

void VersionedKeyValueCategory::addDeletes(BlockId block_id,
                                           std::vector<std::string> &&keys,
                                           VersionedOutput &out,
                                           storage::rocksdb::NativeWriteBatch &batch) {
  const auto deleted = true;
  const auto stale_on_update = false;
  for (auto &&key : keys) {
    auto versioned_key = VersionedRawKey{std::move(key), block_id};

    updateLatestKeyVersion(versioned_key.value, block_id, batch);

    putValue(versioned_key, VersionedDbValue{Tombstone{}}, batch);

    addKeyToUpdateInfo(std::move(versioned_key.value), deleted, stale_on_update, out);
  }
}

void updateRootHash(const std::string &key, const std::string &value, Hasher &hasher) {
  const auto key_hash = hash(key);
  const auto value_hash = hash(value);
  hasher.update(key_hash.data(), key_hash.size());
  hasher.update(value_hash.data(), value_hash.size());
}

void VersionedKeyValueCategory::addUpdates(BlockId block_id,
                                           bool calculate_root_hash,
                                           std::map<std::string, ValueWithFlags> &&updates,
                                           VersionedOutput &out,
                                           storage::rocksdb::NativeWriteBatch &batch) {
  if (updates.empty()) {
    return;
  }

  auto hasher = Hasher{};
  hasher.init();
  const auto deleted = false;
  for (auto it = updates.begin(); it != updates.end();) {
    // Save the iterator as extract() invalidates it.
    auto extracted_it = it;
    ++it;
    auto node = updates.extract(extracted_it);
    auto &key = node.key();
    auto &value = node.mapped();

    if (calculate_root_hash) {
      updateRootHash(key, value.data, hasher);
    }

    auto versioned_key = VersionedRawKey{std::move(key), block_id};

    updateLatestKeyVersion(versioned_key.value, block_id, batch);

    putValue(versioned_key, VersionedDbValue{DbValue{std::move(value.data)}}, batch);

    addKeyToUpdateInfo(std::move(versioned_key.value), deleted, value.stale_on_update, out);
  }

  if (calculate_root_hash) {
    out.root_hash = hasher.finish();
  }
}

void VersionedKeyValueCategory::updateLatestKeyVersion(const std::string &key,
                                                       BlockId version,
                                                       storage::rocksdb::NativeWriteBatch &batch) {
  batch.put(latest_ver_cf_, key, serialize(LatestKeyVersion{version}));
}

void VersionedKeyValueCategory::putValue(const VersionedRawKey &key,
                                         const VersionedDbValue &value,
                                         storage::rocksdb::NativeWriteBatch &batch) {
  batch.put(values_cf_, serialize(key), serialize(value));
}

void VersionedKeyValueCategory::addKeyToUpdateInfo(std::string &&key,
                                                   bool deleted,
                                                   bool stale_on_update,
                                                   VersionedOutput &out) {
  out.keys.emplace(std::move(key), VersionedKeyFlags{deleted, stale_on_update});
}

void VersionedKeyValueCategory::deleteGenesisBlock(BlockId block_id,
                                                   const VersionedOutput &out,
                                                   storage::rocksdb::NativeWriteBatch &batch) {
  for (const auto &[key, flags] : out.keys) {
    const auto latest_version = getLatestVersion(key);
    ConcordAssert(latest_version.has_value());
    // Note: Deleted keys cannot be marked as stale on update.
    if (flags.stale_on_update) {
      // This key is marked stale-on-update and, therefore, we can remove its value at `block_id`.
      batch.del(values_cf_, serialize(VersionedRawKey{key, block_id}));
      // If there are no new versions of a key that was marked as stale-on-update at `block_id`, remove the latest
      // version too.
      if (*latest_version == block_id) {
        batch.del(latest_ver_cf_, key);
      }
    } else if (flags.deleted && *latest_version == block_id) {
      // If the key was deleted at this block and there are no new versions, delete both the value and the latest
      // version.
      batch.del(latest_ver_cf_, key);
      batch.del(values_cf_, serialize(VersionedRawKey{key, block_id}));
    } else if (*latest_version > block_id) {
      // If this key is stale as of `block_id` (meaning it has a newer version), we can remove its value at `block_id`.
      batch.del(values_cf_, serialize(VersionedRawKey{key, block_id}));
    }
  }
}

void VersionedKeyValueCategory::deleteLastReachableBlock(BlockId block_id,
                                                         const VersionedOutput &out,
                                                         storage::rocksdb::NativeWriteBatch &batch) {
  for (const auto &[key, _] : out.keys) {
    (void)_;
    const auto versioned_key = serialize(VersionedRawKey{key, block_id});

    // Find the previous version of the key and set it as a last version. Exploit the fact that CMF
    // serializes strings prefixed by their length (in big-endian). Therefore, VersionedRawKeys will be ordered by key
    // size, key and version. If not found, then this is the only version of the key and we can remove the latest
    // version index too.
    auto iter = db_->getIterator(values_cf_);
    iter.seekAtMost(versioned_key);
    ConcordAssert(iter);
    iter.prev();
    if (iter) {
      auto prev_key = VersionedRawKey{};
      deserialize(iter.keyView(), prev_key);
      if (prev_key.value == key) {
        updateLatestKeyVersion(key, prev_key.version, batch);
      } else {
        // This is the only version of the key - remove the latest version index too.
        batch.del(latest_ver_cf_, key);
      }
    } else {
      // No previous keys means this is the only version of the key - remove the latest version index too.
      batch.del(latest_ver_cf_, key);
    }

    // Remove the value for the key at `block_id`.
    batch.del(values_cf_, versioned_key);
  }
}

std::optional<Value> VersionedKeyValueCategory::get(const std::string &key, BlockId block_id) const {
  const auto ser = db_->getSlice(values_cf_, serialize(VersionedRawKey{key, block_id}));
  if (!ser) {
    return std::nullopt;
  }
  auto v = value(*ser);
  if (!v) {
    return std::nullopt;
  }
  return VersionedValue{{block_id, *std::move(v)}};
}

std::optional<Value> VersionedKeyValueCategory::getLatest(const std::string &key) const {
  const auto latest_version = getLatestVersion(key);
  if (!latest_version) {
    return std::nullopt;
  }
  return get(key, *latest_version);
}

void VersionedKeyValueCategory::multiGet(const std::vector<std::string> &keys,
                                         const std::vector<BlockId> &versions,
                                         std::vector<std::optional<Value>> &values) const {
  ConcordAssertEQ(keys.size(), versions.size());

  auto slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};
  auto versioned_keys = std::vector<std::vector<std::uint8_t>>{};
  versioned_keys.reserve(keys.size());

  for (auto i = 0ull; i < keys.size(); ++i) {
    versioned_keys.push_back(serialize(VersionedRawKey{keys[i], versions[i]}));
  }

  db_->multiGet(values_cf_, versioned_keys, slices, statuses);

  values.clear();
  for (auto i = 0ull; i < slices.size(); ++i) {
    const auto &status = statuses[i];
    const auto &slice = slices[i];
    const auto version = versions[i];
    if (status.ok()) {
      auto v = value(slice);
      if (v) {
        values.push_back(VersionedValue{{version, *std::move(v)}});
      } else {
        values.push_back(std::nullopt);
      }
    } else if (status.IsNotFound()) {
      values.push_back(std::nullopt);
    } else {
      throw std::runtime_error{"VersionedKeyValueCategory multiGet() failure: " + status.ToString()};
    }
  }
}

void VersionedKeyValueCategory::multiGetLatest(const std::vector<std::string> &keys,
                                               std::vector<std::optional<Value>> &values) const {
  auto versions = std::vector<BlockId>{};
  versions.reserve(keys.size());
  auto slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};
  db_->multiGet(latest_ver_cf_, keys, slices, statuses);
  for (auto i = 0ull; i < slices.size(); ++i) {
    const auto &status = statuses[i];
    const auto &slice = slices[i];
    if (status.ok()) {
      versions.push_back(latestVersion(slice));
    } else if (status.IsNotFound()) {
      versions.push_back(Blockchain::INVALID_BLOCK_ID);
    } else {
      throw std::runtime_error{"VersionedKeyValueCategory multiGetLatest() failure: " + status.ToString()};
    }
  }
  multiGet(keys, versions, values);
}

std::optional<BlockId> VersionedKeyValueCategory::getLatestVersion(const std::string &key) const {
  const auto ser = db_->getSlice(latest_ver_cf_, key);
  if (!ser) {
    return std::nullopt;
  }
  return latestVersion(*ser);
}

void VersionedKeyValueCategory::multiGetLatestVersion(const std::vector<std::string> &keys,
                                                      std::vector<std::optional<BlockId>> &versions) const {
  auto slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};
  db_->multiGet(latest_ver_cf_, keys, slices, statuses);
  for (auto i = 0ull; i < slices.size(); ++i) {
    const auto &status = statuses[i];
    const auto &slice = slices[i];
    if (status.ok()) {
      versions.push_back(latestVersion(slice));
    } else if (status.IsNotFound()) {
      versions.push_back(std::nullopt);
    } else {
      throw std::runtime_error{"VersionedKeyValueCategory multiGetLatestVersion() failure: " + status.ToString()};
    }
  }
}

std::optional<KeyValueProof> VersionedKeyValueCategory::getProof(BlockId block_id,
                                                                 const std::string &key,
                                                                 const VersionedOutput &out) const {
  // If the key is not part of this block, return a null proof.
  auto key_it = out.keys.find(key);
  if (key_it == out.keys.cend()) {
    return std::nullopt;
  }

  auto value = get(key, block_id);
  if (!value) {
    return std::nullopt;
  }
  auto &ver_value = asVersioned(value);

  auto proof = KeyValueProof{};
  proof.block_id = ver_value.block_id;
  proof.key = key;
  proof.value = std::move(ver_value.data);

  auto i = std::size_t{0};
  for (const auto &[update_key, _] : out.keys) {
    (void)_;
    if (update_key == key) {
      proof.key_value_index = i;
      continue;
    }

    const auto update_value = get(update_key, block_id);
    ConcordAssert(update_value.has_value());

    proof.ordered_complement_kv_hashes.push_back(hash(update_key));
    proof.ordered_complement_kv_hashes.push_back(hash(asVersioned(update_value).data));

    // Increment by 2 as we push 2 hashes - one of the key and one of the value.
    i += 2;
  }

  return proof;
}

}  // namespace concord::kvbc::categorization::detail
