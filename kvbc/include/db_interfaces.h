// Copyright 2018-2019 VMware, all rights reserved

#pragma once

#include "OpenTracing.hpp"
#include "kv_types.hpp"
#include "status.hpp"

#include "categorization/base_types.h"
#include "categorization/updates.h"

#include <optional>
#include <string>
#include <vector>

namespace concord::kvbc {
/**
 *
 */
class ILocalKeyValueStorageReadOnly {
 public:
  // convenience where readVersion==latest, and block is not needed?
  virtual concordUtils::Status get(const Key &key, Value &outValue) const = 0;
  virtual concordUtils::Status get(BlockId readVersion, const Key &key, Value &outValue, BlockId &outBlock) const = 0;

  // Returns the genesis block ID. If the blockchain is empty, 0 is returned.
  // Throws on errors.
  virtual BlockId getGenesisBlock() const = 0;
  virtual BlockId getLastBlock() const = 0;
  virtual concordUtils::Status getBlockData(BlockId blockId, SetOfKeyValuePairs &outBlockData) const = 0;
  // TODO(GG): explain motivation
  virtual concordUtils::Status mayHaveConflictBetween(const Key &key,
                                                      BlockId fromBlock,
                                                      BlockId toBlock,
                                                      bool &outRes) const = 0;

  virtual ~ILocalKeyValueStorageReadOnly() = default;
};

/**
 *
 */
class IBlocksAppender {
 public:
  virtual concordUtils::Status addBlock(const SetOfKeyValuePairs &updates,
                                        BlockId &outBlockId,
                                        const concordUtils::SpanWrapper &parent_span = concordUtils::SpanWrapper{}) = 0;

  virtual ~IBlocksAppender() = default;
};

// Categorized interfaces follow. Non-categorized interfaces will be deprecated after full integration of categorized
// ones.

// Add blocks to the key-value blockchain.
class IBlockAdder {
 public:
  // Add a block from the given categorized updates and return its ID.
  virtual BlockId add(categorization::Updates &&, const concordUtils::SpanWrapper &parent_span) = 0;

  virtual ~IBlockAdder() = default;
};

// Key-value blockchain categorized read interface.
//
// Output vector parameters to multi* calls can contain more values than requested. This is an optimization, in order to
// reduce memory allocations. Users are encouraged to reuse a single vector instance.
class IReader {
 public:
  // Get the value of a key in `category_id` at `block_id`.
  // Return std::nullopt if `key` doesn't exist at `block_id`.
  virtual std::optional<categorization::Value> get(const std::string &category_id,
                                                   const std::string &key,
                                                   BlockId block_id) const = 0;

  // Get the latest value of `key` in `category_id`.
  // Return std::nullopt if the key doesn't exist or is deleted.
  virtual std::optional<categorization::Value> getLatest(const std::string &category_id,
                                                         const std::string &key) const = 0;

  // Get values for keys at specific versions in `category_id`.
  // `keys` and `versions` must be the same size.
  // If a key is missing at the specified version or deleted, then std::nullopt is returned for it.
  virtual void multiGet(const std::string &category_id,
                        const std::vector<std::string> &keys,
                        const std::vector<BlockId> &versions,
                        std::vector<std::optional<Value>> &values) const = 0;

  // Get the latest values of a list of keys in `category_id`.
  // If a key is missing or is deleted, then std::nullopt is returned for it.
  virtual void multiGetLatest(const std::string &category_id,
                              const std::vector<std::string> &keys,
                              std::vector<std::optional<Value>> &values) const = 0;

  // Get the latest version of `key` in `category_id`.
  // Return std::nullopt if the key doesn't exist or is deleted.
  virtual std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                        const std::string &key) const = 0;

  // Get the latest versions of the given keys in `category_id`.
  // If a key is missing, then std::nullopt is returned for its version.
  virtual void multiGetLatestVersion(const std::string &category_id,
                                     const std::vector<std::string> &keys,
                                     std::vector<std::optional<categorization::TaggedVersion>> &versions) const;

  // Get the updates that were used to create `block_id`.
  virtual categorization::Updates getBlockUpdates(BlockId block_id) const = 0;

  // Get the current genesis block ID in the system.
  virtual BlockId getGenesisBlockId() const = 0;

  // Get the last block ID in the system.
  virtual BlockId getLastBlockId() const = 0;

  virtual ~IReader() = default;
};

class IBlocksDeleter {
 public:
  // Deletes the genesis block.
  // Throws on errors or if the blockchain is empty (no genesis block).
  virtual void deleteGenesisBlock() = 0;

  // Deletes blocks in the [genesis, until) range. If the until value is bigger than the last block, blocks in the
  // range [genesis, lastBlock] will be deleted.
  // Returns the last deleted block ID.
  // Throws on errors or if until <= genesis .
  virtual BlockId deleteBlocksUntil(BlockId until) = 0;

  virtual ~IBlocksDeleter() = default;
};

}  // namespace concord::kvbc
