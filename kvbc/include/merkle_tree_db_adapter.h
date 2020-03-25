// Copyright 2020 VMware, all rights reserved
//
// Contains functionality for working with merkle tree based keys and
// using them to perform basic blockchain operations.

#pragma once

#include "base_db_adapter.h"
#include "kv_types.hpp"
#include "sliver.hpp"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/tree.h"
#include "storage/db_interface.h"

#include <memory>
#include "storage/db_types.h"

namespace concord {
namespace storage {
namespace blockchain {
namespace v2MerkleTree {

class DBKeyManipulator {
 public:
  static concordUtils::Sliver genBlockDbKey(kvbc::BlockId version);
  static concordUtils::Sliver genDataDbKey(const sparse_merkle::LeafKey &key);
  static concordUtils::Sliver genDataDbKey(const concordUtils::Sliver &key, kvbc::BlockId version);
  static concordUtils::Sliver genInternalDbKey(const sparse_merkle::InternalNodeKey &key);
  static concordUtils::Sliver genStaleDbKey(const sparse_merkle::InternalNodeKey &key, kvbc::BlockId staleSinceVersion);
  static concordUtils::Sliver genStaleDbKey(const sparse_merkle::LeafKey &key, kvbc::BlockId staleSinceVersion);
  static concordUtils::Sliver generateMetadataKey(ObjectId objectId);
  static concordUtils::Sliver generateStateTransferKey(ObjectId objectId);
  static concordUtils::Sliver generateSTPendingPageKey(uint32_t pageId);
  static concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageStaticKey(uint32_t pageId, uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt);
  static concordUtils::Sliver generateSTTempBlockKey(kvbc::BlockId blockId);

  // Extract the block ID from a EDBKeyType::Block key or from a EKeySubtype::Leaf key.
  static kvbc::BlockId extractBlockIdFromKey(const kvbc::Key &key);

  // Extract the hash from a leaf key.
  static sparse_merkle::Hash extractHashFromLeafKey(const kvbc::Key &key);

 protected:
  static concordlogger::Logger &logger() {
    static concordlogger::Logger logger_ = concordlogger::Log::getLogger("concord.storage.blockchain.DBKeyManipulator");
    return logger_;
  }
};

// The DBAdapter class provides facilities for managing a key/value blockchain on top of a key/value store.
//
// DBAdapter supports the notion of a blockchain with blocks linked to each other by a parent hash. The blockchain spans
// from block ID = 1 to block ID = getLastReachableBlock(). Block outside of this range are considered temporary state
// transfer (ST) blocks that are transferred until blocks in the range [getLastReachableBlock() + 1, getLatestBlock()]
// are available. At the end of state transfer, getLastReachableBlock() becomes equal to getLastBlock(), meaning that
// the two chains are linked. Temporary blocks are deleted once added to the blockchain. It can be represented visually
// as:
// ---------------------------------------------------------------------
// | 1 ... getLastReachableBlock(), ST temp blocks ..., getLastBlock() |
// ---------------------------------------------------------------------
// NOTE: ST blocks don't need to be added in reverse order - they can be added in any order. When the
// getLastReachableBlock() + 1 block is added, DBAdapter will link the chain in the range
// [getLastReachableBlock() + 1, ReachableSTBlock], where ReachableSTBlock is a block that is reachable from block
// getLastReachableBlock() + 1. Additionally, ReachableSTBlock <= getLastBlock().
class DBAdapter : public kvbc::DBAdapterBase {
 public:
  // The constructor will try to link the blockchain with any blocks in the temporary state transfer chain. This is done
  // so that the DBAdapter will operate correctly in case a crash or an abnormal shutdown has occurred prior to startup
  // (construction). Note that only a single DBAdapter instance should operate on a database and access to all methods
  // should be either done from a single thread or serialized via a mutex or another mechanism. The constructor throws
  // on errors.
  DBAdapter(const std::shared_ptr<IDBClient> &db);

  // Adds a block to the end of the blockchain from a set of key/value pairs. Includes:
  // - adding the key/value pairs in separate keys
  // - adding the whole block (raw block) in its own key
  // - calculating and filling in the parent digest.
  // Typically called by the application when adding a new block.
  // Empty values are not supported and an error status will be returned if one is passed.
  // Empty blocks (i.e. an empty 'updates' set) are supported.
  concordUtils::Status addLastReachableBlock(const kvbc::SetOfKeyValuePairs &updates);

  // Adds a block from its raw representation and a block ID.
  // Typically called by state transfer when a block is received.
  // If adding the next block (i.e. getLastReachableBlock() + 1), it is done so through the merkle tree. If it is not
  // the next block, a temporary state transfer block is added instead.
  concordUtils::Status addBlock(const concordUtils::Sliver &block, kvbc::BlockId blockId);

  // Gets the value of a key by its block version. The actual block version is written to the actualBlockVersion output
  // variable. If the requested version is not found, the most recent earlier one will be returned. If no such one, an
  // empty outValue and an actualBlockVersion of 0 will be returned.
  // Note: This method operates on the blockchain only, meaning that it will not take blocks with ID >
  // getLastReachableBlock() into account.
  concordUtils::Status getKeyByReadVersion(kvbc::BlockId blockVersion,
                                           const kvbc::Key &key,
                                           concordUtils::Sliver &outValue,
                                           kvbc::BlockId &actualBlockVersion) const;

  // Returns the ID of the latest block that is part of the blockchain. Returns 0 if there are no blocks in the system.
  kvbc::BlockId getLastReachableBlock() const;

  // Returns the ID of the latest block saved in the DB. It might be either:
  //  - the last reachable block if no state transfer is in progress, i.e. getLastReachableBlock() == getLatestBlock()
  //  - if state transfer is in progress, the block with the biggest(latest) ID that state transfer has added. In this
  //  case, the block is not part of the blockchain yet.
  kvbc::BlockId getLatestBlock() const;

  // Gets a block by its ID. The returned block buffer can be inspected with functions from the block::v2MerkleTree
  // namespace.
  // If successful, Status::OK() is returned. If the block is not found, Status::NotFound() is returned. Else, a status
  // describing the error is returned.
  // Note: Takes both blocks from the blockchain and temporary ST blocks into account.
  concordUtils::Status getBlockById(kvbc::BlockId blockId, concordUtils::Sliver &block) const;

  // Returns the current state hash from the internal merkle tree implementation.
  const sparse_merkle::Hash &getStateHash() const { return smTree_.get_root_hash(); }

 private:
  concordUtils::Sliver createBlockNode(const kvbc::SetOfKeyValuePairs &updates, kvbc::BlockId blockId) const;

  // Returns a set of key/value pairs that represent the needed DB updates for adding a block as part of the blockchain.
  kvbc::SetOfKeyValuePairs lastReachableBlockDbUpdates(const kvbc::SetOfKeyValuePairs &updates, kvbc::BlockId blockId);

  // Try to link the ST temporary chain to the blockchain from the passed blockId up to getLatestBlock().
  concordUtils::Status linkSTChainFrom(kvbc::BlockId blockId);

  concordUtils::Status writeSTLinkTransaction(const kvbc::Key &sTBlockKey,
                                              const concordUtils::Sliver &block,
                                              kvbc::BlockId blockId);

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
  kvbc::BlockId blockId;
  sparse_merkle::LeafNode leafNode;
};

inline bool operator==(const DatabaseLeafValue &lhs, const DatabaseLeafValue &rhs) {
  return (lhs.blockId == rhs.blockId && lhs.leafNode == rhs.leafNode);
}

}  // namespace detail

}  // namespace v2MerkleTree
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
