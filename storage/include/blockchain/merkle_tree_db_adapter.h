// Copyright 2020 VMware, all rights reserved
//
// Contains functionality for working with merkle tree based keys and
// using them to perform basic blockchain operations.

#pragma once

#include "blockchain/base_db_adapter.h"
#include "blockchain/db_types.h"
#include "kv_types.hpp"
#include "sliver.hpp"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/tree.h"
#include "storage/db_interface.h"

#include <memory>

namespace concord {
namespace storage {
namespace blockchain {
namespace v2MerkleTree {

class DBKeyManipulator : public DBKeyManipulatorBase {
 public:
  static concordUtils::Sliver genBlockDbKey(BlockId version);
  static concordUtils::Sliver genDataDbKey(const sparse_merkle::LeafKey &key);
  static concordUtils::Sliver genDataDbKey(const concordUtils::Sliver &key, BlockId version);
  static concordUtils::Sliver genInternalDbKey(const sparse_merkle::InternalNodeKey &key);
  static concordUtils::Sliver genStaleDbKey(const sparse_merkle::InternalNodeKey &key, BlockId staleSinceVersion);
  static concordUtils::Sliver genStaleDbKey(const sparse_merkle::LeafKey &key, BlockId staleSinceVersion);
  static concordUtils::Sliver generateMetadataKey(ObjectId objectId);
  static concordUtils::Sliver generateStateTransferKey(ObjectId objectId);
  static concordUtils::Sliver generateSTPendingPageKey(uint32_t pageId);
  static concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageStaticKey(uint32_t pageId, uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt);
  static concordUtils::Sliver generateSTTempBlockKey(BlockId blockId);

  // Extract the block ID from a EDBKeyType::Block key or from a EKeySubtype::Leaf key.
  static BlockId extractBlockIdFromKey(const concordUtils::Key &key);

  // Extract the hash from a leaf key.
  static sparse_merkle::Hash extractHashFromLeafKey(const concordUtils::Key &key);
};

// The DBAdapter class provides facilities for managing a key/value blockchain on top of a key/value store.
//
// DBAdapter supports the notion of a blockchain with blocks linked to each other by a parent hash. The blockchain spans
// from block ID = 1 to block ID = getLastReachableBlock(). Block outside of this range are considered temporary state
// transfer (ST) blocks that are transferred until blocks in the range [getLastReachableBlock() + 1, getLatestBlock()]
// are available. At that point, the blocks in this range are added to the blockchain and getLastReachableBlock()
// becomes equal to getLastBlock(), meaning that the two chains are linked. Temporary blocks are deleted once added to
// the blockchain. It can be represented visually as:
// ---------------------------------------------------------------------
// | 1 ... getLastReachableBlock(), ST temp blocks ..., getLastBlock() |
// ---------------------------------------------------------------------
// NOTE: ST blocks don't need to be added in reverse order - they can be added in any order. Once the
// getLastReachableBlock() + 1 block is added, DBAdapter will link the chain in the range
// [getLastReachableBlock() + 1, getLatestBlock()], thus making it part of the blockchain.
class DBAdapter : public DBAdapterBase {
 public:
  DBAdapter(const std::shared_ptr<IDBClient> &db, bool readOnly = false);

  // Adds a block to the end of the blockchain from a set of key/value pairs. Includes:
  // - adding the key/value pairs in separate keys
  // - adding the whole block (raw block) in its own key
  // - calculating and filling in the parent digest.
  // Typically called by the application when adding a new block.
  // Empty blocks are not allowed - this method will return IllegalOperation if an empty set is passed.
  concordUtils::Status addLastReachableBlock(const concordUtils::SetOfKeyValuePairs &updates);

  // Adds a block from its raw representation and a block ID.
  // Typically called by state transfer when a block is received.
  // If adding the next block (i.e. getLastReachableBlock() + 1), it is done so through the merkle tree. If it is not
  // the next block, a temporary state transfer block is added instead.
  concordUtils::Status addBlock(const concordUtils::Sliver &block, BlockId blockId);

  // Gets the value of a key by its version. The actual version is written to the actualVersion output variable.
  // If the requested version is not found, the most recent earlier one will be returned. If no such one, an empty
  // outValue and an actualVersion of 0 will be returned.
  // Returns Status::OK() .
  // Note: This method operates on the blockchain only, meaning that it will not take blocks with ID >
  // getLastReachableBlock() into account.
  concordUtils::Status getKeyByReadVersion(BlockId version,
                                           const concordUtils::Key &key,
                                           concordUtils::Sliver &outValue,
                                           BlockId &actualVersion) const;

  // Returns the ID of the latest block that is part of the blockchain. Returns 0 if there are no blocks in the system.
  BlockId getLastReachableBlock() const;

  // Returns the ID of the latest block saved in the DB. It might be either:
  //  - the last reachable block if no state transfer is in progress, i.e. getLastReachableBlock() == getLatestBlock()
  //  - if state transfer is in progress, the block with the biggest(latest) ID that state transfer has added. In this
  //  case, the block is not part of the blockchain yet.
  BlockId getLatestBlock() const;

  // Gets a block by its ID. The returned block buffer can be inspected with functions from the block::v2MerkleTree
  // namespace. Returns the status of the operation. If the block is not found, Status::OK() is returned and the found
  // output variable is set to false.
  // Note: Takes both blocks from the blockchain and temporary ST blocks into account.
  concordUtils::Status getBlockById(BlockId blockId, concordUtils::Sliver &block, bool &found) const;

  // Returns the current state hash from the internal merkle tree implementation.
  const sparse_merkle::Hash &getStateHash() const { return smTree_.get_root_hash(); }

 private:
  concordUtils::Sliver createBlockNode(const concordUtils::SetOfKeyValuePairs &updates,
                                       BlockId blockId,
                                       const sparse_merkle::Version &stateRootVersion) const;

  // Returns a set of key/value pairs that represent the needed DB updates for adding a block as part of the blockchain.
  concordUtils::SetOfKeyValuePairs lastReachableBlockkDbUpdates(const concordUtils::SetOfKeyValuePairs &updates,
                                                                BlockId blockId);

  // Try to link the ST temporary chain to the blockchain from the passed blockId up to getLatestBlock().
  concordUtils::Status linkSTChainFrom(BlockId blockId);

  concordUtils::Status writeSTLinkTransaction(const concordUtils::Key &sTBlockKey,
                                              const concordUtils::Sliver &block,
                                              BlockId blockId);

  class Reader : public sparse_merkle::IDBReader {
   public:
    Reader(const DBAdapter &adapter) : adapter_{adapter} {}

    // Return the latest root node in the system
    sparse_merkle::BatchedInternalNode get_latest_root() const override;

    // Retrieve a BatchedInternalNode given an InternalNodeKey.
    //
    // Throws a std::out_of_range exception if the internal node does not exist.
    sparse_merkle::BatchedInternalNode get_internal(const sparse_merkle::InternalNodeKey &) const override;

    // Retrieve a LeafNode given a LeafKey.
    //
    // Throws a std::out_of_range exception if the leaf does not exist.
    sparse_merkle::LeafNode get_leaf(const sparse_merkle::LeafKey &) const override;

   private:
    const DBAdapter &adapter_;
  };

  sparse_merkle::Tree smTree_;
};

}  // namespace v2MerkleTree
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
