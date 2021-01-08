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
  std::optional<MerkleValue> get(const std::string& key, BlockId block_id) const;
  std::optional<MerkleValue> get(const Hash& hashed_key, BlockId block_id) const;

  // Return the value of `key` at its most recent block version.
  // Return std::nullopt if the key doesn't exist.
  std::optional<MerkleValue> getLatest(const std::string& key) const;

  // Returns the latest *block* version of a key.
  // Returns std::nullopt if the key doesn't exist.
  std::optional<TaggedVersion> getLatestVersion(const std::string& key) const;
  std::optional<TaggedVersion> getLatestVersion(const Hash& key) const;

  // Get values for keys at specific versions.
  // `keys` and `versions` must be the same size.
  // If a key is missing at the specified version, std::nullopt is returned for it.
  void multiGet(const std::vector<std::string>& keys,
                const std::vector<BlockId>& versions,
                std::vector<std::optional<MerkleValue>>& values) const;

  // Get the latest values of a list of keys.
  // If a key is missing, std::nullopt is returned for it.
  void multiGetLatest(const std::vector<std::string>& keys, std::vector<std::optional<MerkleValue>>& values) const;

  // Get the latest versions of the given keys.
  // If a key is missing, std::nullopt is returned for its version.
  void multiGetLatestVersion(const std::vector<std::string>& keys,
                             std::vector<std::optional<TaggedVersion>>& versions) const;
  void multiGetLatestVersion(const std::vector<Hash>& keys, std::vector<std::optional<TaggedVersion>>& versions) const;

  // Delete the given block ID as a genesis one.
  // Precondition: The given block ID must be the genesis one.
  void deleteGenesisBlock(BlockId, const BlockMerkleOutput&, storage::rocksdb::NativeWriteBatch&);

  // Delete the given block ID as a last reachable one.
  // Precondition: The given block ID must be the last reachable one.
  void deleteLastReachableBlock(BlockId, const BlockMerkleOutput&, storage::rocksdb::NativeWriteBatch&);

  uint64_t getLatestTreeVersion() const;
  uint64_t getLastDeletedTreeVersion() const;

 private:
  void multiGet(const std::vector<Buffer>& versioned_keys,
                const std::vector<BlockId>& versions,
                std::vector<std::optional<MerkleValue>>& values) const;

  sparse_merkle::UpdateBatch updateTree(BlockId, MerkleBlockValue&&);

  // The last deleted tree version is stored at key `0` in BLOCK_MERKLE_STALE_CF
  void putLastDeletedTreeVersion(uint64_t tree_version, storage::rocksdb::NativeWriteBatch&);

  // During pruning of a genesis block, rewrite the merkle tree value if some keys are still active.
  // This is necessary for proofs.
  //
  // Retrieve the value of all active keys so we can recalculate the root hash for the modified block.
  // Write the updated block to the merkle tree.
  void rewriteMerkleBlock(BlockId, std::vector<KeyHash>&& active_keys, storage::rocksdb::NativeWriteBatch&);

  // When a genesis block is pruned, we must delete all data that is stale as of `tree_version`.
  //
  // This includes data that may be written in prior tree versions as a result of prior pruning
  // operations that generate new tree versions and stale data, but not new blocks. There can be a
  // large amount of these prior versions, as each pruned block generates a new tree version. If we
  // prune X blocks in a row before we add a new block, Y, then when we prune Block Y, we will have
  // to lookup all the indexes for those pruned X tree versions and delete the internal and leaf
  // nodes in the indexes.
  void deleteStaleData(uint64_t tree_version, storage::rocksdb::NativeWriteBatch&);

  // Retrieve the latest versions for all raw keys in a block and return them along with the hashed keys.
  std::pair<std::vector<Hash>, std::vector<std::optional<TaggedVersion>>> getLatestVersions(
      const BlockMerkleOutput& out);

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

}  // namespace concord::kvbc::categorization::detail

#endif
