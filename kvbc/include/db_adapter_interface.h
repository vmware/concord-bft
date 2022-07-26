// Copyright 2020 VMware, all rights reserved
//
// This convenience header combines different DB adapter implementations.

#pragma once

#include "kv_types.hpp"
#include <utility>
#include "digest.hpp"

namespace concord::storage {
class IDBClient;
}

namespace concord::kvbc {

using concord::util::digest::BlockDigest;

class NotFoundException : public std::runtime_error {
 public:
  NotFoundException(const std::string& error) : std::runtime_error(("NotFoundException: " + error).c_str()) {}
  const char* what() const noexcept override { return std::runtime_error::what(); }
};

class IDbAdapter {
 public:
  // Returns the added block ID.
  virtual BlockId addBlock(const SetOfKeyValuePairs& updates) = 0;

  // Try to link the blockchain until the given block ID.
  virtual void linkUntilBlockId(BlockId until_block_id) = 0;

  // Adds a block from its raw representation and a block ID.
  // Typically called by state transfer when a block is received and needs to be added.
  virtual void addRawBlock(const RawBlock& rawBlock, const BlockId& blockId, bool lastBlock) = 0;

  // Get block in its raw form
  virtual RawBlock getRawBlock(const BlockId& blockId) const = 0;

  // Return the actual version and the value for a key.
  // Throws if an error occurs.
  virtual std::pair<Value, BlockId> getValue(const Key& key, const BlockId& blockVersion) const = 0;

  // Deletes the block with the passed ID.
  // If the passed block ID doesn't exist, the call has no effect.
  // Throws if an error occurs.
  virtual void deleteBlock(const BlockId& blockId) = 0;

  // Deletes the last reachable block.
  // If the blockchain is empty, the call has no effect.
  // Throws if an error occurs.
  virtual void deleteLastReachableBlock() = 0;

  // Checks whether block exists
  virtual bool hasBlock(const BlockId& blockId) const = 0;

  // Returns the genesis (first) block ID in the system. If the blockchain is empty or if there are no reachable blocks
  // (getLastReachableBlock() == 0), 0 is returned.
  // Throws on errors.
  virtual BlockId getGenesisBlockId() const = 0;

  // Used to retrieve the latest block.
  virtual BlockId getLatestBlockId() const = 0;

  // Used to retrieve the last reachable block.
  // From ST perspective, this is maximal block number N such that all blocks
  // START <= i <= N exist, where START is usually 1, if pruning is not enabled.
  // In the normal state, it should be equal to last block ID.
  virtual BlockId getLastReachableBlockId() const = 0;

  // Returns the block data in the form of a set of key/value pairs.
  virtual SetOfKeyValuePairs getBlockData(const RawBlock& rawBlock) const = 0;

  // Returns the parent digest of the passed block.
  virtual BlockDigest getParentDigest(const RawBlock& rawBlock) const = 0;

  // TODO [TK] not sure it's needed for long term
  virtual std::shared_ptr<storage::IDBClient> getDb() const = 0;

  // These two interfaces are implemented only RO db adapter
  virtual void getLastKnownReconfigurationCmdBlock(std::string& outBlockData) const {}
  virtual void setLastKnownReconfigurationCmdBlock(std::string& blockData) {}

  virtual ~IDbAdapter() = default;
};

}  // namespace concord::kvbc
