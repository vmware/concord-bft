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

#include <memory>
#include <string>

namespace concord::kvbc::categorization::detail {

// The shared key-value category persists key-values across multiple categories. A category is implemented as a pair of
// RocksDB column families:
//  * `data` column family - contains versioned key-values for the given category
//  * `latest version` column family - contains the latest version of a key for the given category. The rationale behind
//    storing the latest version of a key is to support latest version key lookup without using RocksdDB iterators.
//    Instead, we can use two point lookups - one to get the latest version and one to get the actual value. This is
//    done for performance reasons.
//
// If a key-value is part of multiple categories in a single block update, the value will be physically stored once only
// in one of the categories and other categories will contain a key with a `pointer` to the value.
//
// Key-values in the shared category are automatically made stale on addition.
//
// Key-value proofs per category and per block are supported in the form of a root hash that describes the block
// updates.
class SharedKeyValueCategory {
 public:
  SharedKeyValueCategory(const std::shared_ptr<storage::rocksdb::NativeClient> &);

  // Add the given block updates and return the information that needs to be persisted in the block.
  SharedKeyValueUpdatesInfo add(BlockId block_id, SharedKeyValueUpdatesData &&update, storage::rocksdb::WriteBatch &);

  // Delete the genesis block.
  // `updates_info` must be the updates for `block_id`.
  // Precondition: `block_id` is the current genesis block ID.
  void deleteGenesisBlock(BlockId block_id,
                          const SharedKeyValueUpdatesInfo &updates_info,
                          storage::rocksdb::WriteBatch &);

  // Delete the last reachable block.
  // `updates_info` must be the updates for `block_id`.
  // Precondition: `block_id` is the current last reachable block ID.
  void deleteLastReachableBlock(BlockId block_id,
                                const SharedKeyValueUpdatesInfo &updates_info,
                                storage::rocksdb::WriteBatch &);

  // Get the value of a key in `category_id` with a block version up to `max_block_id`.
  // Return std::nullopt if the key doesn't exist in any earlier blocks than `max_block_id`.
  std::optional<Value> getValueUpToBlock(const std::string &category_id,
                                         const std::string &key,
                                         BlockId max_block_id) const;

  // Get the value of a key in `category_id` at `block_id`.
  // Return std::nullopt if the key doesn't exist at `block_id`.
  std::optional<Value> getValueAtBlock(const std::string &category_id, const std::string &key, BlockId block_id) const;

  // Get the latest value of a key in `category_id`.
  // Return std::nullopt if the key doesn't exist.
  std::optional<Value> getLatestValue(const std::string &category_id, const std::string &key) const;

  // Get the value of a key and a proof for it in `category_id` at the given `block_id`.
  // `updates_info` must be the updates for `block_id`.
  // Return std::nullopt if the key doesn't exist at `block_id`.
  std::optional<KeyValueProof> getValueProofAtBlock(const std::string &category_id,
                                                    const std::string &key,
                                                    BlockId block_id,
                                                    const SharedKeyValueUpdatesInfo &updates_info) const;

 private:
  void createColumnFamilyIfNotExisting(const std::string &category_id) const;

 private:
  const std::shared_ptr<storage::rocksdb::NativeClient> db_;
};

}  // namespace concord::kvbc::categorization::detail
