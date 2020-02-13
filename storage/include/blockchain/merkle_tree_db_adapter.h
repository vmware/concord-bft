// Copyright 2020 VMware, all rights reserved
//
// Contains functionality for working with merkle tree based keys and
// using them to perform basic blockchain operations.

#pragma once

#include "blockchain/base_db_adapter.h"
#include "blockchain/db_types.h"
#include "kv_types.hpp"
#include "sliver.hpp"
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

  // Extract the block ID from a EDBKeyType::Block key or from a EKeySubtype::Leaf key.
  static BlockId extractBlockIdFromKey(const concordUtils::Key &key);
};

class DBAdapter : public DBAdapterBase {
 public:
  DBAdapter(const std::shared_ptr<IDBClient> &db, bool readOnly = false);

  // Adds a block from a set of key/value pairs and a block ID. Includes:
  // - adding the key/value pairs in separate keys
  // - adding the whole block (raw block) in its own key
  // - calculating and filling in the parent digest.
  // Typically called by the application when adding a new block.
  Status addBlock(const concordUtils::SetOfKeyValuePairs &kv, BlockId blockId);

  // Gets the value of a key by its version. The actual version is written to the actualVersion output variable.
  // If the requested version is not found, the most recent earlier one will be returned. If no such one, an empty
  // outValue and an actualVersion of 0 will be returned.
  // Returns Status::OK() .
  Status getKeyByReadVersion(BlockId version,
                             const concordUtils::Sliver &key,
                             concordUtils::Sliver &outValue,
                             BlockId &actualVersion) const;

  // Returns the latest block ID, i.e. the key with the biggest version (block ID). Returns 0 if there are no blocks in
  // the system.
  BlockId getLatestBlock() const;

  // Gets a block by its ID. The returned block buffer can be inspected with functions from the block::v2MerkleTree
  // namespace. Returns the status of the operation. If the block is not found, Status::OK() is returned and the found
  // output variable is set to false.
  Status getBlockById(BlockId blockId, concordUtils::Sliver &block, bool &found) const;

  // Returns the current state hash from the internal merkle tree implementation.
  const sparse_merkle::Hash &getStateHash() const { return smTree_.get_root_hash(); }

 private:
  concordUtils::Sliver createBlockNode(const concordUtils::SetOfKeyValuePairs &updates, BlockId blockId) const;

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
