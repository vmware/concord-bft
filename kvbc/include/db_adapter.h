// Copyright 2020 VMware, all rights reserved
//
// This convenience header combines different DB adapter implementations.

#pragma once

#include "kv_types.hpp"
#include <optional>
#include <utility>

namespace concord::kvbc {

class NotFoundException : public std::runtime_error {
 public:
  NotFoundException(const std::string& error) : std::runtime_error(("NotFoundException: " + error).c_str()) {}
  const char* what() const noexcept override { return std::runtime_error::what(); }
};

class IDbAdapter {
 public:
  // Returns the added block ID.
  virtual BlockId addBlockToBlockchain(const SetOfKeyValuePairs& updates) = 0;

  // Adds a block from its raw representation and a block ID.
  // Typically called by state transfer when a block is received and needs to be added.
  virtual void addRawBlock(const RawBlock& rawBlock, const BlockId& blockId) = 0;

  // Get block in its raw form
  virtual RawBlock getRawBlock(const BlockId& blockId) const = 0;

  // If found, return the actual version and the value.
  virtual std::optional<std::pair<Value, BlockId>> getValue(const Key&, const BlockId& blockVersion) const = 0;

  // Delete a block from the database
  virtual void deleteBlock(const BlockId& blockId) = 0;

  // Used to retrieve the latest block.
  virtual BlockId getLastBlockchainBlockId() const = 0;

  /**
   * Used to retrieve the last reachable block
   * Searches for the last reachable block.
   * From ST perspective, this is maximal block number N
   * such that all blocks 1 <= i <= N exist.
   * In the normal state, it should be equal to last block ID.
   */
  virtual BlockId getLastRechableBlockId() const = 0;

  virtual ~IDbAdapter() = default;
};

}  // namespace concord::kvbc
#include "direct_kv_db_adapter.h"
#include "merkle_tree_db_adapter.h"
