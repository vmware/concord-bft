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

#pragma once

#include "kv_types.hpp"
#include "rocksdb/native_client.h"

#include "base_types.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "details.h"

#include <memory>
#include <set>
#include <string>

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
class ImmutableKeyValueCategory {
 public:
  ImmutableKeyValueCategory(const std::string &category_id, const std::shared_ptr<storage::rocksdb::NativeClient> &);

  // Add the given block updates and return the information that needs to be persisted in the block.
  // Adding keys that already exist in this category is undefined behavior.
  ImmutableUpdatesInfo add(BlockId, ImmutableUpdatesData &&update, storage::rocksdb::NativeWriteBatch &);

  // Delete the genesis block. Implemented by directly calling deleteBlock().
  void deleteGenesisBlock(BlockId block_id,
                          const ImmutableUpdatesInfo &updates_info,
                          storage::rocksdb::NativeWriteBatch &batch);

  // Delete the last reachable block. Implemented by directly calling deleteBlock().
  void deleteLastReachableBlock(BlockId block_id,
                                const ImmutableUpdatesInfo &updates_info,
                                storage::rocksdb::NativeWriteBatch &batch);

  // Deletes the keys for the passed updates info.
  void deleteBlock(const ImmutableUpdatesInfo &updates_info, storage::rocksdb::NativeWriteBatch &batch);

  // Get the value of an immutable key.
  // Return std::nullopt if the key doesn't exist.
  std::optional<ImmutableValue> get(const std::string &key) const;

  // Get the value of a key and a proof for it in `tag`.
  // `updates_info` must be the updates for `block_id`.
  // Return std::nullopt if the key doesn't exist.
  std::optional<KeyValueProof> getProof(const std::string &tag,
                                        const std::string &key,
                                        const ImmutableUpdatesInfo &updates_info) const;

 private:
  std::string cf_;
  const std::shared_ptr<storage::rocksdb::NativeClient> db_;
};

}  // namespace concord::kvbc::categorization::detail
