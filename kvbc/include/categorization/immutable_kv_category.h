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

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>
#include "details.h"

namespace concord::kvbc::categorization::detail {

// ImmutableKeyValueCategory stores tagged keys directly in RocksDB. Keys are not versioned. Updating the value or the
// tags of a key is undefined behavior. Explicit deletes are not supported.
//
// Key-values in the immutable category are automatically made stale on addition.
//
// Proofs for keys tagged with the same tag in a block are supported in the form of a root hash that
// is defined as:
//   root_hash = h(h(k1) || h(v1) || h(k2) || h(v2) || ... || h(kn) || h(vn))
// A proof for some key per tag is just the hashes of all the other keys and values with the same tag.
//
// There is an option to turn off proofs (root hash calculation) per block.
class ImmutableKeyValueCategory {
 public:
  ImmutableKeyValueCategory() = default;  // for testing only
  ImmutableKeyValueCategory(const std::string &category_id, const std::shared_ptr<storage::rocksdb::NativeClient> &);

  // Add the given block updates and return the information that needs to be persisted in the block.
  // Adding keys that already exist in this category is undefined behavior.
  ImmutableOutput add(BlockId, ImmutableInput &&, storage::rocksdb::NativeWriteBatch &);

  std::vector<std::string> getBlockStaleKeys(BlockId, const ImmutableOutput &) const;
  std::set<std::string> getStaleActiveKeys(BlockId, const ImmutableOutput &) const { return {}; }

  // Delete the genesis block. Implemented by directly calling deleteBlock().
  std::size_t deleteGenesisBlock(BlockId, const ImmutableOutput &, detail::LocalWriteBatch &);

  // Delete the last reachable block. Implemented by directly calling deleteBlock().
  void deleteLastReachableBlock(BlockId, const ImmutableOutput &, storage::rocksdb::NativeWriteBatch &);

  // Deletes the keys for the passed updates info.
  template <typename Batch>
  void deleteBlock(const ImmutableOutput &updates_info, Batch &batch) {
    for (const auto &kv : updates_info.tagged_keys) {
      batch.del(cf_, kv.first);
    }
  }

  // Get the value of an immutable key in `block_id`.
  // Return std::nullopt if `key` doesn't exist in `block_id`.
  std::optional<Value> get(const std::string &key, BlockId block_id) const;

  // Get the value of an immutable key.
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

  // Get the version of an immutable key.
  // Return std::nullopt if the key doesn't exist.
  std::optional<TaggedVersion> getLatestVersion(const std::string &key) const;

  // Get the latest versions of the given keys.
  // If a key is missing, std::nullopt is returned for its version.
  void multiGetLatestVersion(const std::vector<std::string> &keys,
                             std::vector<std::optional<TaggedVersion>> &versions) const;

  // Get the value of a key and a proof for it in `tag`.
  // Return std::nullopt if the key doesn't exist.
  std::optional<KeyValueProof> getProof(const std::string &tag,
                                        const std::string &key,
                                        const ImmutableOutput &updates_info) const;

 private:
  std::string cf_;
  std::shared_ptr<storage::rocksdb::NativeClient> db_;
};

inline const ImmutableValue &asImmutable(const Value &v) { return std::get<ImmutableValue>(v); }
inline ImmutableValue &asImmutable(Value &v) { return std::get<ImmutableValue>(v); }

// Optional overloads assume the optional contains a value.
inline const ImmutableValue &asImmutable(const std::optional<Value> &v) { return asImmutable(*v); }
inline ImmutableValue &asImmutable(std::optional<Value> &v) { return asImmutable(*v); }

}  // namespace concord::kvbc::categorization::detail
