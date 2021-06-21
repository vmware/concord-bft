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

#include "assertUtils.hpp"
#include "db_adapter_interface.h"
#include "kv_types.hpp"
#include "Logger.hpp"
#include "merkle_tree_block.h"
#include "sliver.hpp"
#include "sparse_merkle/tree.h"
#include "storage/db_interface.h"
#include "Statistics.hpp"
#include "PerformanceManager.hpp"

#include <future>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>

namespace concord::kvbc::v2MerkleTree {

// The DBAdapter class provides facilities for managing a key/value blockchain on top of a key/value store in the form
// of a sparse merkle tree.
//
// DBAdapter supports the notion of a blockchain with blocks linked to each other by a parent hash. The blockchain spans
// from block ID = 1 to block ID = getLastReachableBlockId(). Block outside of this range are considered temporary state
// transfer (ST) blocks that are transferred until blocks in the range [getLastReachableBlockId() + 1,
// getLatestBlockId()] are available. At the end of state transfer, getLastReachableBlockId() becomes equal to
// getLatestBlockId(), meaning that the two chains are linked. Temporary blocks are deleted once added to the
// blockchain. It can be represented visually as:
// ---------------------------------------------------------------------------
// | 1 ... getLastReachableBlockId(), ST temp blocks ..., getLatestBlockId() |
// ---------------------------------------------------------------------------
// NOTE: ST blocks don't need to be added in reverse order - they can be added in any order. When the
// getLastReachableBlockId() + 1 block is added, DBAdapter will link the chain in the range
// [getLastReachableBlockId() + 1, ReachableSTBlock], where ReachableSTBlock is a block that is reachable from block
// getLastReachableBlockId() + 1. Additionally, ReachableSTBlock <= getLatestBlockId().
class DBAdapter : public IDbAdapter {
 public:
  using NonProvableKeySet = std::unordered_set<Key>;

  // Unless explicitly turned off, the constructor will try to link the blockchain with any blocks in the temporary
  // state transfer chain. This is done so that the DBAdapter will operate correctly in case a crash or an abnormal
  // shutdown has occurred prior to startup (construction). Only a single DBAdapter instance should operate on a
  // database and access to all methods should be either done from a single thread or serialized via a mutex or another
  // mechanism. The constructor throws if an error occurs.
  // Note1: The passed DB client must be initialized beforehand.
  // Note2: The 'linkTempSTChain' parameter turns of chain linking and is used for testing/tooling purposes.
  // Note3: The key provided via 'nonProvableKeySet' parameter will be stored outside of the Merkle Tree,
  // the keys must have the same size!
  // Note4: Non-provable keys cannot be deleted for now.
  DBAdapter(const std::shared_ptr<concord::storage::IDBClient> &db,
            bool linkTempSTChain = true,
            const NonProvableKeySet &nonProvableKeySet = NonProvableKeySet{},
            const std::shared_ptr<concord::performance::PerformanceManager> &pm_ =
                std::make_shared<concord::performance::PerformanceManager>());

  // Make the adapter non-copyable.
  DBAdapter(const DBAdapter &) = delete;
  DBAdapter &operator=(const DBAdapter &) = delete;

  // Adds a block to the end of the blockchain from a set of key/value pairs and a set of keys to delete. If a key is
  // present in both 'updates' and 'deletes' parameters, it will be present in the block with the value passed in
  // 'updates'.
  // Empty blocks (i.e. both 'updates' and 'deletes' parameters being empty) are supported.
  // Returns the added block ID.
  BlockId addBlock(const SetOfKeyValuePairs &updates, const OrderedKeysSet &deletes);

  // Adds a block to the end of the blockchain from a set of key/value pairs.
  // Empty blocks (i.e. an empty 'updates' set) are supported.
  // Returns the added block ID.
  BlockId addBlock(const SetOfKeyValuePairs &updates) override;

  // Adds a block to the end of the blockchain from a set of keys to delete.
  // Empty blocks (i.e. an empty 'deletes' set) are supported.
  // Returns the added block ID.
  BlockId addBlock(const OrderedKeysSet &deletes);

  // Adds a block from its raw representation and a block ID.
  // Typically called by state transfer when a block is received.
  // If adding the next block (i.e. getLastReachableBlockId() + 1), it is done so through the merkle tree. If it is not
  // the next block, a temporary state transfer block is added instead.
  void addRawBlock(const RawBlock &rawBlock, const BlockId &blockId) override;

  // Gets a raw block by its ID.
  // An exception is thrown if an error occurs.
  // A ::concord::kvbc::NotFoundException is thrown if the block doesn't exist.
  // Note: Takes both blocks from the blockchain and temporary ST blocks into account.
  RawBlock getRawBlock(const BlockId &blockId) const override;

  // Gets the value of a key by blockVersion . If the key exists at blockVersion, its value at blockVersion will be
  // returned. If the key doesn't exist at blockVersion, but exists at an earlier version, its value at the
  // earlier version will be returned (including values from deleted blocks). Otherwise, a
  // ::concord::kvbc::NotFoundException will be thrown.
  // Note1: The returned version is the block version the key was written at and is less than or equal to the
  // passed blockVersion .
  // Note2: Operates on the blockchain only, meaning that it will not take blocks with ID > getLastReachableBlockId()
  // into account.
  std::pair<Value, BlockId> getValue(const Key &key, const BlockId &blockVersion) const override;

  // Returns the genesis (first) block ID in the system. If the blockchain is empty or if there are no reachable blocks
  // (getLastReachableBlock() == 0), 0 is returned.
  // Throws on errors.
  BlockId getGenesisBlockId() const override;

  // Returns the ID of the latest block that is part of the blockchain. Returns 0 if there are no blocks in the system.
  BlockId getLastReachableBlockId() const override;

  // Returns the ID of the latest block saved in the DB. It might be either:
  //  - the last reachable block if no state transfer is in progress, i.e. getLastReachableBlockId() ==
  //    getLatestBlockId()
  //  - if state transfer is in progress, the block with the biggest(latest) ID that state transfer has added. In this
  //    case, the block is not part of the blockchain yet.
  BlockId getLatestBlockId() const override;

  // Deletes the block with the passed ID.
  // If the passed block ID doesn't exist, the call has no effect.
  // Throws on any of the following:
  //  - an error occurs
  //  - the passed block ID is part of the blockchain and is neither the genesis block nor the last reachable block,
  //    meaning blocks in the middle of the blockchain cannot be deleted
  //  - the passed ID is the only blockchain block in the system, meaning blocks can only be deleted if the blockchain
  //    size is greater or equal to 2. State transfer blocks can still be deleted, even if the blockchain is empty or
  //    only has a single blockchain block.
  void deleteBlock(const BlockId &blockId) override;

  // Deletes the last reachable block.
  // If the blockchain is empty, the call has no effect.
  // If there is only 1 block in the blockchain, it will be deleted (as opposed to calling deleteBlock()).
  // Throws if an error occurs.
  void deleteLastReachableBlock() override;

  // Returns the block data in the form of a set of key/value pairs.
  SetOfKeyValuePairs getBlockData(const RawBlock &rawBlock) const override;

  // Returns the parent digest of the passed block.
  BlockDigest getParentDigest(const RawBlock &rawBlock) const override;

  std::shared_ptr<storage::IDBClient> getDb() const override { return db_; }

  // Returns true if the block exists (including temporary ST blocks). Throws on errors.
  bool hasBlock(const BlockId &blockId) const override;

  // Returns the current state hash of the internal sparse merkle tree.
  const sparse_merkle::Hash &getStateHash() const { return smTree_.get_root_hash(); }

  // Returns the current version of the internal sparse merkle tree.
  sparse_merkle::Version getStateVersion() const { return smTree_.get_version(); };

  // Gets a future to the asynchronously-computed parent block digest.
  std::future<BlockDigest> computeParentBlockDigest(BlockId blockId) const;

  // Returns a set of key/value pairs that represent the needed DB updates for adding a block as part of the blockchain.
  // This method is made public for testing purposes only. It is meant to be used internally.
  SetOfKeyValuePairs lastReachableBlockDbUpdates(const SetOfKeyValuePairs &updates,
                                                 const OrderedKeysSet &deletes,
                                                 BlockId blockId);

  // Execute an update to the tree without persisting the result. This method is made public for testing purposes only.
  std::pair<sparse_merkle::UpdateBatch, sparse_merkle::detail::UpdateCache> updateTree(
      const SetOfKeyValuePairs &updates, const OrderedKeysSet &deletes);

  void WriteMetadata(const std::string &key, const std::string &val) override {}
  void GetMetadata(const std::string &key, std::string &val) override {}

 private:
  concordUtils::Sliver createBlockNode(const SetOfKeyValuePairs &updates,
                                       const OrderedKeysSet &deletes,
                                       BlockId blockId,
                                       const BlockDigest &parentBlockDigest) const;

  // Try to link the ST temporary chain to the blockchain from the passed blockId up to getLatestBlock().
  void linkSTChainFrom(BlockId blockId);

  void writeSTLinkTransaction(const Key &sTBlockKey, const concordUtils::Sliver &block, BlockId blockId);

  block::detail::Node getBlockNode(BlockId blockId) const;

  KeysVector staleIndexNonProvableKeysForBlock(BlockId blockId) const;

  KeysVector staleIndexProvableKeysForVersion(const sparse_merkle::Version &version) const;

  KeysVector internalProvableKeysForVersion(const sparse_merkle::Version &version) const;

  KeysVector lastReachableBlockKeyDeletes(BlockId blockId) const;

  KeysVector genesisBlockKeyDeletes(BlockId blockId) const;

  std::optional<std::pair<Key, Value>> getLeafKeyValAtMostVersion(const Key &key,
                                                                  const sparse_merkle::Version &version) const;

  void deleteGenesisBlock();

  void deleteKeysForBlock(const KeysVector &keys, BlockId blockId) const;

  BlockId loadGenesisBlockId() const;

  BlockId loadLastReachableBlockId() const;

  // Return std::nullopt if no temporary ST blocks are present.
  std::optional<BlockId> loadLatestTempSTBlockId() const;

  std::optional<std::pair<Value, BlockId>> getValueForNonProvableKey(const Key &key, const BlockId &blockVersion) const;

  class Reader : public sparse_merkle::IDBReader {
   public:
    Reader(const DBAdapter &adapter) : adapter_{adapter} {}

    // Return the latest root node in the system.
    sparse_merkle::BatchedInternalNode get_latest_root() const override;

    // Retrieve a BatchedInternalNode given an InternalNodeKey.
    //
    // Throws a std::out_of_range exception if the internal node does not exist.
    sparse_merkle::BatchedInternalNode get_internal(const sparse_merkle::InternalNodeKey &) const override;

   private:
    const DBAdapter &adapter_;
  };

  logging::Logger logger_;
  std::shared_ptr<storage::IDBClient> db_;
  BlockId genesisBlockId_{0};
  BlockId lastReachableBlockId_{0};
  // The latest ST temporary block ID. Not set if no ST temporary blocks are present in the system.
  std::optional<BlockId> latestSTTempBlockId_;
  sparse_merkle::Tree smTree_;
  const NonProvableKeySet nonProvableKeySet_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
};

namespace detail {

// Used for serialization purposes.
inline constexpr auto INVALID_BLOCK_ID = BlockId{0};
static_assert(INVALID_BLOCK_ID < INITIAL_GENESIS_BLOCK_ID);

// Serialize leafs in the DB as the the value itself plus some additional metadata.
struct DatabaseLeafValue {
  using BlockIdType = BlockId;

  // 'addedInBlockId' and 'deletedInBlockId' are mandatory when serializing.
  static constexpr auto MIN_SIZE = sizeof(BlockIdType) * 2;

  DatabaseLeafValue() = default;
  DatabaseLeafValue(BlockIdType pAddedInBlockId, const sparse_merkle::LeafNode &pLeafNode)
      : addedInBlockId{pAddedInBlockId}, leafNode{pLeafNode} {}
  DatabaseLeafValue(BlockIdType pAddedInBlockId,
                    const sparse_merkle::LeafNode &pLeafNode,
                    BlockIdType pDeletedInBlockId)
      : addedInBlockId{pAddedInBlockId}, leafNode{pLeafNode}, deletedInBlockId{pDeletedInBlockId} {
    ConcordAssert(pDeletedInBlockId != INVALID_BLOCK_ID);
  }

  // The block ID this value was added in.
  BlockIdType addedInBlockId{INVALID_BLOCK_ID};

  // The actual leaf node (value).
  sparse_merkle::LeafNode leafNode;

  // The block ID this value was deleted in. Not set if this value has not been deleted.
  std::optional<BlockIdType> deletedInBlockId;

  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
};

inline bool operator==(const DatabaseLeafValue &lhs, const DatabaseLeafValue &rhs) {
  return (lhs.addedInBlockId == rhs.addedInBlockId && lhs.leafNode == rhs.leafNode &&
          lhs.deletedInBlockId == rhs.deletedInBlockId);
}

}  // namespace detail

}  // namespace concord::kvbc::v2MerkleTree
