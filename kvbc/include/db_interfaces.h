// Copyright 2018-2019 VMware, all rights reserved

#pragma once

#include "util/OpenTracing.hpp"
#include "kv_types.hpp"
#include "util/status.hpp"

#include "categorization/base_types.h"
#include "categorization/updates.h"

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace concord::kvbc {

// Add blocks to the key-value blockchain.
class IBlockAdder {
 public:
  // Add a block from the given categorized updates and return its ID.
  virtual BlockId add(categorization::Updates &&) = 0;

  virtual ~IBlockAdder() = default;
};

// Key-value blockchain categorized read interface.
//
// Output vector parameters to multi* calls can contain more values than requested. This is an optimization, in order to
// reduce memory allocations. Users are encouraged to reuse a single vector instance.
//
// If the given category doesn't exist, all get* calls will return std::nullopt. In case of multiGet* calls, the
// returned vectors will be filled with std::nullopts.
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
                        std::vector<std::optional<categorization::Value>> &values) const = 0;

  // Get the latest values of a list of keys in `category_id`.
  // If a key is missing or is deleted, then std::nullopt is returned for it.
  virtual void multiGetLatest(const std::string &category_id,
                              const std::vector<std::string> &keys,
                              std::vector<std::optional<categorization::Value>> &values) const = 0;

  // Get the latest version of `key` in `category_id`.
  // Return std::nullopt if the key doesn't exist or is deleted.
  virtual std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                        const std::string &key) const = 0;

  // Get the latest versions of the given keys in `category_id`.
  // If a key is missing, then std::nullopt is returned for its version.
  virtual void multiGetLatestVersion(const std::string &category_id,
                                     const std::vector<std::string> &keys,
                                     std::vector<std::optional<categorization::TaggedVersion>> &versions) const = 0;

  // Get the updates that were used to create `block_id`.
  // Return std::nullopt if this block doesn't exist.
  virtual std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const = 0;

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
  // If delete_files_in_range is true then a fast deletion is used.
  // Returns the last deleted block ID.
  // Throws on errors or if until <= genesis .
  virtual BlockId deleteBlocksUntil(BlockId until, bool delete_files_in_range) = 0;

  // This method should get the last block ID in the system and deletes it.
  // The last block id is the latest block id.
  virtual void deleteLastReachableBlock() = 0;

  virtual ~IBlocksDeleter() = default;
};

// Given an IReader, return the time of the last application-level transaction stored in the blockchain.
// The result must be a string that can be parsed via google::protobuf::util::TimeUtil::FromString().
using LastApplicationTransactionTimeCallback = std::function<std::string(const IReader &)>;

}  // namespace concord::kvbc
