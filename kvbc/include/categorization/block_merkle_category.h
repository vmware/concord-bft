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

#ifdef USE_ROCKSDB

#include "Logger.hpp"
#include "rocksdb/native_client.h"
#include "sparse_merkle/tree.h"

#include "base_types.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "details.h"

namespace concord::kvbc::categorization::detail {

// This category puts only block relevant information into the sparse merkle tree. This drastically
// reduces the storage load and merkle tree overhead, but still allows similar proof guarantees.
// The `key` going into the merkle tree is the block version, while the value consists of:
//     * The root hash of the merkle provable keys and values for the block
//     * The hash of each active provable key in the block after the block is pruned
//
class BlockMerkleCategory {
 public:
  BlockMerkleCategory() = default;  // Gtest usage only
  BlockMerkleCategory(const std::shared_ptr<storage::rocksdb::NativeClient>&);

  // Add the given block updates and return the information that needs to be persisted in the block.
  BlockMerkleOutput add(BlockId block_id, BlockMerkleInput&& update, storage::rocksdb::NativeWriteBatch&);

  // Return the value of `key` at `block_id`.
  // Return std::nullopt if the key doesn't exist at `block_id`.
  std::optional<Value> get(const std::string& key, BlockId block_id) const;
  std::optional<Value> get(const Hash& hashed_key, BlockId block_id) const;

  // Return the value of `key` at its most recent block version.
  // Return std::nullopt if the key doesn't exist.
  std::optional<Value> getLatest(const std::string& key) const;

  // Returns the latest *block* version of a key.
  // Returns std::nullopt if the key doesn't exist.
  std::optional<TaggedVersion> getLatestVersion(const std::string& key) const;
  std::optional<TaggedVersion> getLatestVersion(const Hash& key) const;

  // Get values for keys at specific versions.
  // `keys` and `versions` must be the same size.
  // If a key is missing at the specified version, std::nullopt is returned for it.
  void multiGet(const std::vector<std::string>& keys,
                const std::vector<BlockId>& versions,
                std::vector<std::optional<Value>>& values) const;

  // Get the latest values of a list of keys.
  // If a key is missing, std::nullopt is returned for it.
  void multiGetLatest(const std::vector<std::string>& keys, std::vector<std::optional<Value>>& values) const;

  // Get the latest versions of the given keys.
  // If a key is missing, std::nullopt is returned for its version.
  void multiGetLatestVersion(const std::vector<std::string>& keys,
                             std::vector<std::optional<TaggedVersion>>& versions) const;
  void multiGetLatestVersion(const std::vector<Hash>& keys, std::vector<std::optional<TaggedVersion>>& versions) const;

 private:
  void multiGet(const std::vector<Buffer>& versioned_keys,
                const std::vector<BlockId>& versions,
                std::vector<std::optional<Value>>& values) const;

  void putKeys(storage::rocksdb::NativeWriteBatch& batch,
               uint64_t block_id,
               std::vector<KeyHash>&& hashed_added_keys,
               std::vector<KeyHash>&& hashed_deleted_keys,
               BlockMerkleInput& updates);

  void putMerkleNodes(storage::rocksdb::NativeWriteBatch& batch,
                      sparse_merkle::UpdateBatch&& update_batch,
                      uint64_t tree_version);

 private:
  class Reader : public sparse_merkle::IDBReader {
   public:
    Reader(const storage::rocksdb::NativeClient& db) : db_{db} {}

    // Return the latest root node in the system.
    sparse_merkle::BatchedInternalNode get_latest_root() const override;

    // Retrieve a BatchedInternalNode given an InternalNodeKey.
    //
    // Throws a std::out_of_range exception if the internal node does not exist.
    sparse_merkle::BatchedInternalNode get_internal(const sparse_merkle::InternalNodeKey&) const override;

   private:
    // The lifetime of this reference is shorter than the lifetime of the tree which is shorter than
    // the lifetime of the category.
    const storage::rocksdb::NativeClient& db_;
  };

 private:
  std::shared_ptr<storage::rocksdb::NativeClient> db_;

  logging::Logger logger_;
  sparse_merkle::Tree tree_;
};

inline const MerkleValue& asMerkle(const Value& v) { return std::get<MerkleValue>(v); }
inline MerkleValue& asMerkle(Value& v) { return std::get<MerkleValue>(v); }

}  // namespace concord::kvbc::categorization::detail

#endif
