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

#pragma once

#include "kv_types.hpp"
#include "rocksdb/native_client.h"

#include "base_types.h"
#include "categorized_kvbc_msgs.cmf.hpp"

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include "details.h"

namespace concord::kvbc::categorization::detail {

// VersionedKeyValueCategory stores versioned keys directly in RocksDB. Explicit key deletions and marking
// keys stale immediately on update are supported.
//
// Keys are stored as is, without being hashed.
//
// Proofs for keys in a block are supported in the form of a root hash that is defined as:
//   root_hash = h(h(k1) || h(v1) || h(k2) || h(v2) || ... || h(kn) || h(vn))
// A proof for some key in a block is just the hashes of all the other keys and values in the block.
//
// There is an option to turn off proofs (root hash calculation) per block.
class VersionedKeyValueCategory {
 public:
  VersionedKeyValueCategory() = default;  // for testing only
  VersionedKeyValueCategory(const std::string &category_id, const std::shared_ptr<storage::rocksdb::NativeClient> &);

  VersionedOutput add(BlockId, VersionedInput &&, storage::rocksdb::NativeWriteBatch &);

  // Delete the given block ID as a genesis one.
  // Precondition: The given block ID must be the genesis one.
  // Return the number of deleted keys from the DB.
  std::size_t deleteGenesisBlock(BlockId, const VersionedOutput &, detail::LocalWriteBatch &);

  // Delete the given block ID as a last reachable one.
  // Precondition: The given block ID must be the last reachable one.
  void deleteLastReachableBlock(BlockId, const VersionedOutput &, storage::rocksdb::NativeWriteBatch &);

  // Get the value of a versioned key in `block_id`.
  // Return std::nullopt if `key` doesn't exist in `block_id`.
  std::optional<Value> get(const std::string &key, BlockId block_id) const;

  // Get the latest value of `key`.
  // Return std::nullopt if the key doesn't exist.
  std::optional<Value> getLatest(const std::string &key) const;

  // Get values for keys at specific versions.
  // `keys` and `versions` must be the same size.
  // If a key is missing at the specified version, std::nullopt is returned for it.
  void multiGet(const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<Value>> &values) const;

  // Get the latest values of a list of keys.
  // If a key is missing, std::nullopt is returned for it.
  void multiGetLatest(const std::vector<std::string> &keys, std::vector<std::optional<Value>> &values) const;

  // Get the latest version of `key`.
  // Return std::nullopt if the key doesn't exist.
  std::optional<TaggedVersion> getLatestVersion(const std::string &key) const;

  // Get the latest versions of the given keys.
  // If a key is missing, std::nullopt is returned for its version.
  void multiGetLatestVersion(const std::vector<std::string> &keys,
                             std::vector<std::optional<TaggedVersion>> &versions) const;

  // Get the value of `key` and a proof for it at `block_id`.
  // Return std::nullopt if the key doesn't exist.
  std::optional<KeyValueProof> getProof(BlockId block_id, const std::string &key, const VersionedOutput &) const;

  // Get all stale keys as of `block_id`.
  std::vector<std::string> getBlockStaleKeys(BlockId block_id, const VersionedOutput &) const;
  // Get all stale keys from active keys as of `block_id`.
  std::set<std::string> getStaleActiveKeys(BlockId block_id, const VersionedOutput &) const;

 private:
  void addDeletes(BlockId, std::vector<std::string> &&keys, VersionedOutput &, storage::rocksdb::NativeWriteBatch &);

  void addUpdates(BlockId,
                  bool calculate_root_hash,
                  std::map<std::string, ValueWithFlags> &&,
                  VersionedOutput &,
                  storage::rocksdb::NativeWriteBatch &);

  void updateLatestKeyVersion(const std::string &key, TaggedVersion version, storage::rocksdb::NativeWriteBatch &);

  void putValue(const VersionedRawKey &, bool deleted, std::string_view value, storage::rocksdb::NativeWriteBatch &);

  void addKeyToUpdateInfo(std::string &&key, bool deleted, bool stale_on_update, VersionedOutput &);

  std::unordered_map<BlockId, std::vector<std::string>> activeKeysFromPrunedBlocks(
      const std::map<std::string, VersionedKeyFlags> &kv) const;

 private:
  std::string values_cf_;
  std::string latest_ver_cf_;
  std::string active_cf_;
  std::shared_ptr<storage::rocksdb::NativeClient> db_;
};

inline const VersionedValue &asVersioned(const Value &v) { return std::get<VersionedValue>(v); }
inline VersionedValue &asVersioned(Value &v) { return std::get<VersionedValue>(v); }

// Optional overloads assume the optional contains a value.
inline const VersionedValue &asVersioned(const std::optional<Value> &v) { return asVersioned(*v); }
inline VersionedValue &asVersioned(std::optional<Value> &v) { return asVersioned(*v); }

}  // namespace concord::kvbc::categorization::detail
