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

  std::vector<std::string> getBlockStaleKeys(BlockId, const BlockMerkleOutput&) const;
  std::set<std::string> getStaleActiveKeys(BlockId, const BlockMerkleOutput&) const;
  // Delete the given block ID as a genesis one.
  // Precondition: The given block ID must be the genesis one.
  std::size_t deleteGenesisBlock(BlockId, const BlockMerkleOutput&, detail::LocalWriteBatch&);

  // Delete the given block ID as a last reachable one.
  // Precondition: The given block ID must be the last reachable one.
  // Precondition: We cannot call deleteLastReachable on a pruned block.
  //   Pruning a block is a one way operation for many reasons including:
  //     1. Deleting old tree versions as a result of prior prunings
  //     2. Creating new block versions for a leaf + Pruned block + active keys
  //       Reversing step 1 is impossible. Reversing step 2 can be very expensive (for active keys).
  //
  //   The kv_blockchain code *must* ensure `deleteLastReachable` is not called on pruned blocks. This
  //   isn't onerous for the caller, and actually it doesn't really make any sense to do so anyway. In
  //   short, even without explicit protection in the caller this should be next to impossible to
  //   happen for the following reasons:
  //     * Blocks are pruned after creation on the order of days, weeks, or months
  //     * DeleteLastReachable will only delete blocks that have not been synced in the concord-bft
  //       internal metadata and during which a restart happened. This happens on the order of
  //        microseconds to milliseconds.
  //   In any event, blocks that aren't synced shouldn't even be eligible for pruning at an even higher level.
  void deleteLastReachableBlock(BlockId, const BlockMerkleOutput&, storage::rocksdb::NativeWriteBatch&);

  //
  // Accessors useful for tests or tools
  //
  uint64_t getLatestTreeVersion() const;
  uint64_t getLastDeletedTreeVersion() const;

 private:
  void multiGet(const std::vector<Buffer>& versioned_keys,
                const std::vector<BlockId>& versions,
                std::vector<std::optional<Value>>& values) const;

  // The last deleted tree version is stored at key `0` in BLOCK_MERKLE_STALE_CF
  template <typename Batch>
  void putLastDeletedTreeVersion(uint64_t tree_version, Batch&);

  // During initial pruning of a genesis block, rewrite the merkle tree value if some keys are still active.
  // This is necessary for proofs.
  //
  // The data written in this function is the data that gets rewritten or removed in
  // `rewriteAlreadyPrunedBlocks`.
  //
  // Retrieve the value of all active keys so we can recalculate the root hash for the modified block.
  // Write the updated block to the merkle tree.
  // Also write corrsponding active key indexes and pruned block index to their respective column families.
  //
  // Return the serialized `BlockMerkleValue` so we only update the tree once in `deleteGenesisBlock`.
  concordUtils::Sliver writePrunedBlock(BlockId, std::vector<KeyHash>&& active_keys, detail::LocalWriteBatch&);

  // When a block gets pruned, any keys that are still `active` (latest version of a key) are
  // tracked in a `PrunedBlock`. PrunedBlocks are kept in their own column family. Additionally, a
  // new root hash is calculated for all remaining active keys in the block, and a new version of
  // the `MerkleBlockValue` is written into the block merkle tree. Lastly, each active key hash is
  // written into a separate column family with the pruned block_id as the value. These latter
  // active keys are necessary for garbage collection, while the `PrunedBlock` and `MerkleBlockValue`
  // are used to provide (and verify) proofs for active keys in pruned blocks.
  //
  // Garbage collection occurs when the block that first updated or deleted an active key from a
  // prior prune block is itself pruned. All keys in the current block being pruned are checked
  // during pruning to see if there were prior active keys from pruned blocks. If so, those prior
  // versions of the keys are deleted along with their active references used for garbage
  // collection. If any active keys remain for a pruned block, a new root hash is calculated, a new
  // PrunedBlock written, and the merkle tree is updated yet again for that block. If no active keys
  // remain for that pruned block, then the pruned block is deleted and the block is finally removed
  // from the merkle tree, as there are no longer any keys from that block eligible for proofs. This
  // garbage collection and update process is what is performed by this function.
  //
  // To reiterate: pruning (calling `deleteGenesisBlock`) does not always fully delete all data in a
  // block. If a key is at the latest version when the block is deleted, it is still `active` and
  // users can retrieve its data and prove that it is part of the blockchain. Any active keys become
  // `deactivated` when a new version of the key is written in a later block, or the key is deleted
  // in a later block. However, even though the key is no longer the latest version, it is not
  // deleted in this step. Final deletion from the database only occurs when the overwriting block
  // is itself pruned. This tradeoff is made to defer work to the pruning process and not cause
  // block addition to slow down.
  //
  // Returns added blocks and deleted keys so we only update the tree a single time in `deleteGenesisBlock`.
  std::pair<SetOfKeyValuePairs, KeysVector> rewriteAlreadyPrunedBlocks(
      std::unordered_map<BlockId, std::vector<KeyHash>>& deleted_keys, detail::LocalWriteBatch& batch);

  // When a genesis block is pruned, we must delete all data that is stale as of `tree_version`.
  //
  // This includes data that may be written in prior tree versions as a result of prior pruning
  // operations that generate new tree versions and stale data, but not new blocks. There can be a
  // large amount of these prior versions, as each pruned block generates a new tree version. If we
  // prune N blocks in a row before we add a new block, X, then when we prune Block X, we will have
  // to lookup all the indexes for those pruned N tree versions and delete the internal and leaf
  // nodes in the indexes.
  void deleteStaleData(uint64_t tree_version, detail::LocalWriteBatch&);

  // Delete a batch of stale merkle nodes as part of `deleteStaleData`.
  //
  // 1. Multiget a batch of stale indexes for tree versions in the range [`start`,`end`).
  // 2. Create a WriteBatch of all the deletions for the keys in those indexes, as well as the stale
  //    index keys themselves, and the last deleted tree version for this batch.
  // 3. Atomically write the batch to the database.
  void deleteStaleBatch(uint64_t start, uint64_t end);

  // Retrieve the latest versions for all raw keys in a block and return them along with the keys and the hashed keys.
  // Returned tuple contains (list_of_key_hashes, list_of_keys, list_of_versions).
  std::tuple<std::vector<Hash>, std::vector<std::string>, std::vector<std::optional<TaggedVersion>>> getLatestVersions(
      const BlockMerkleOutput& out) const;

  // Return a map from block id to all hashed keys that were still active in previously pruned blocks.
  //
  // This is used during pruning, when we must garbage collect keys from prior pruned blocks that
  // have been overwritten in a new block. When no active keys for the prior pruned block remain we
  // can remove the block from the merkle tree.
  //
  // In the common case, there should not be very many still active keys from pruned blocks. A good
  // goal for system builders is to try to ensure that when a block is pruned, none of its keys
  // remain active. This minimizes overhead prune overhead.
  std::unordered_map<BlockId, std::vector<KeyHash>> findActiveKeysFromPrunedBlocks(
      const std::vector<Hash>& hashed_keys) const;

  // Get a pruned block from the database, deserialize it, and return it.
  // Precondition: The pruned block exists
  PrunedBlock getPrunedBlock(const Buffer& block_key);

  MerkleBlockValue computeRootHash(BlockId block_id,
                                   const std::vector<KeyHash>& active_keys,
                                   bool write_active_key,
                                   detail::LocalWriteBatch&);

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

  sparse_merkle::Tree tree_;
};

inline const MerkleValue& asMerkle(const Value& v) { return std::get<MerkleValue>(v); }
inline MerkleValue& asMerkle(Value& v) { return std::get<MerkleValue>(v); }

}  // namespace concord::kvbc::categorization::detail

#endif
