// Copyright 2020 VMware, all rights reserved
//
// Contains functionality for working with merkle tree based keys and
// using them to perform basic blockchain operations.

#pragma once

#include "db_adapter_interface.h"
#include "kv_types.hpp"
#include "Logger.hpp"
#include "merkle_tree_block.h"
#include "sliver.hpp"
#include "sparse_merkle/tree.h"
#include "storage/db_interface.h"

#include <memory>

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
  // The constructor will try to link the blockchain with any blocks in the temporary state transfer chain. This is done
  // so that the DBAdapter will operate correctly in case a crash or an abnormal shutdown has occurred prior to startup
  // (construction). Only a single DBAdapter instance should operate on a database and access to all methods
  // should be either done from a single thread or serialized via a mutex or another mechanism. The constructor throws
  // if an error occurs.
  // Note: The passed DB client must be initialized beforehand.
  DBAdapter(const std::shared_ptr<concord::storage::IDBClient> &db);

  // Adds a block to the end of the blockchain from a set of key/value pairs.
  // Typically called by the application when adding a new block.
  // Empty values are not supported and an exception will be thrown if one is passed.
  // Empty blocks (i.e. an empty 'updates' set) are supported.
  // Returns the added block ID.
  BlockId addBlock(const SetOfKeyValuePairs &updates) override;

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

  // Returns the block data in the form of a set of key/value pairs.
  SetOfKeyValuePairs getBlockData(const RawBlock &rawBlock) const override;

  // Returns the parent digest of the passed block.
  BlockDigest getParentDigest(const RawBlock &rawBlock) const override;

  std::shared_ptr<storage::IDBClient> getDb() const override { return db_; }

  // Returns true if the block exists (including temporary ST blocks). Throws on errors.
  bool hasBlock(const BlockId &blockId) const override;

  // Returns the current state hash from the internal merkle tree implementation.
  const sparse_merkle::Hash &getStateHash() const { return smTree_.get_root_hash(); }

  // Returns a set of key/value pairs that represent the needed DB updates for adding a block as part of the blockchain.
  // This method is made public for testing purposes only. It is meant to be used internally.
  SetOfKeyValuePairs lastReachableBlockDbUpdates(const SetOfKeyValuePairs &updates, BlockId blockId);

 private:
  concordUtils::Sliver createBlockNode(const SetOfKeyValuePairs &updates, BlockId blockId) const;

  // Try to link the ST temporary chain to the blockchain from the passed blockId up to getLatestBlock().
  void linkSTChainFrom(BlockId blockId);

  void writeSTLinkTransaction(const Key &sTBlockKey, const concordUtils::Sliver &block, BlockId blockId);

  block::detail::Node getBlockNode(BlockId blockId) const;

  KeysVector staleIndexKeysForVersion(const sparse_merkle::Version &version) const;

  KeysVector internalKeysForVersion(const sparse_merkle::Version &version) const;

  KeysVector lastReachableBlockKeyDeletes(BlockId blockId) const;

  KeysVector genesisBlockKeyDeletes(BlockId blockId) const;

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

  concordlogger::Logger logger_;
  std::shared_ptr<storage::IDBClient> db_;
  sparse_merkle::Tree smTree_;
};

namespace detail {

// Serialize leafs in the DB as the block ID the value was saved at and the value itself.
struct DatabaseLeafValue {
  BlockId blockId;
  sparse_merkle::LeafNode leafNode;
};

inline bool operator==(const DatabaseLeafValue &lhs, const DatabaseLeafValue &rhs) {
  return (lhs.blockId == rhs.blockId && lhs.leafNode == rhs.leafNode);
}

}  // namespace detail

}  // namespace concord::kvbc::v2MerkleTree
