// Copyright 2020 VMware, all rights reserved
//
// Contains functionality for working with merkle tree based keys and
// using them to perform basic blockchain operations.

#pragma once

#include "base_db_adapter.h"
#include "db_adapter.h"
#include "kv_types.hpp"
#include "sliver.hpp"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/tree.h"
#include "storage/db_interface.h"

#include <memory>
#include "storage/db_types.h"

namespace concord::kvbc::v2MerkleTree {

class DBKeyManipulator {
 public:
  static concordUtils::Sliver genBlockDbKey(BlockId version);
  static concordUtils::Sliver genDataDbKey(const sparse_merkle::LeafKey &key);
  static concordUtils::Sliver genDataDbKey(const concordUtils::Sliver &key, kvbc::BlockId version);
  static concordUtils::Sliver genInternalDbKey(const sparse_merkle::InternalNodeKey &key);
  static concordUtils::Sliver genStaleDbKey(const sparse_merkle::InternalNodeKey &key, kvbc::BlockId staleSinceVersion);
  static concordUtils::Sliver genStaleDbKey(const sparse_merkle::LeafKey &key, kvbc::BlockId staleSinceVersion);
  static concordUtils::Sliver generateMetadataKey(storage::ObjectId objectId);
  static concordUtils::Sliver generateStateTransferKey(storage::ObjectId objectId);
  static concordUtils::Sliver generateSTPendingPageKey(uint32_t pageId);
  static concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageStaticKey(uint32_t pageId, uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt);
  static concordUtils::Sliver generateSTTempBlockKey(BlockId blockId);

  // Extract the block ID from a EDBKeyType::Block key or from a EKeySubtype::Leaf key.
  static BlockId extractBlockIdFromKey(const Key &key);

  // Extract the hash from a leaf key.
  static sparse_merkle::Hash extractHashFromLeafKey(const Key &key);

 protected:
  static concordlogger::Logger &logger() {
    static auto logger_ = concordlogger::Log::getLogger("concord.kvbc.DBKeyManipulator");
    return logger_;
  }
};

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
class DBAdapter : public IDbAdapter, private DBAdapterBase {
 public:
  // The constructor will try to link the blockchain with any blocks in the temporary state transfer chain. This is done
  // so that the DBAdapter will operate correctly in case a crash or an abnormal shutdown has occurred prior to startup
  // (construction). Note that only a single DBAdapter instance should operate on a database and access to all methods
  // should be either done from a single thread or serialized via a mutex or another mechanism. The constructor throws
  // if an error occurs.
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

  // Gets the value of a key by its block version. If the key doesn't exist at version blockVersion and an earlier
  // version exists, it will be returned. Otherwise, a ::concord::kvbc::NotFoundException will be thrown.
  // An exception is thrown if an error occurs.
  // Note: Operates on the blockchain only, meaning that it will not take blocks with ID > getLastReachableBlockId()
  // into account.
  std::pair<Value, BlockId> getValue(const Key &key, const BlockId &blockVersion) const override;

  // Returns the ID of the latest block that is part of the blockchain. Returns 0 if there are no blocks in the system.
  BlockId getLastReachableBlockId() const override;

  // Returns the ID of the latest block saved in the DB. It might be either:
  //  - the last reachable block if no state transfer is in progress, i.e. getLastReachableBlockId() ==
  //    getLatestBlockId()
  //  - if state transfer is in progress, the block with the biggest(latest) ID that state transfer has added. In this
  //    case, the block is not part of the blockchain yet.
  BlockId getLatestBlockId() const override;

  // Deletes the block with the passed ID.
  // Throws if an error occurs. Throws a ::concord::kvbc::NotFoundException if the block doesn't exist.
  void deleteBlock(const BlockId &blockId) override;

  std::shared_ptr<storage::IDBClient> getDb() const override { return db_; }

  // Returns the current state hash from the internal merkle tree implementation.
  const sparse_merkle::Hash &getStateHash() const { return smTree_.get_root_hash(); }

 private:
  concordUtils::Sliver createBlockNode(const SetOfKeyValuePairs &updates, BlockId blockId) const;

  // Returns a set of key/value pairs that represent the needed DB updates for adding a block as part of the blockchain.
  SetOfKeyValuePairs lastReachableBlockDbUpdates(const SetOfKeyValuePairs &updates, BlockId blockId);

  // Try to link the ST temporary chain to the blockchain from the passed blockId up to getLatestBlock().
  void linkSTChainFrom(BlockId blockId);

  void writeSTLinkTransaction(const Key &sTBlockKey, const concordUtils::Sliver &block, BlockId blockId);

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
