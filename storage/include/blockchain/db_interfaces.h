// Copyright 2018-2019 VMware, all rights reserved

#pragma once

#include <iterator>
#include <set>
#include <string>
#include <unordered_map>
#include "sliver.hpp"
#include "status.hpp"
#include "storage/db_interface.h"

using std::pair;
using std::string;
using std::unordered_map;

namespace concord {
namespace storage {
namespace blockchain {

using concordUtils::Status;
using concordUtils::Key;
using concordUtils::Value;
using concordUtils::BlockId;

// forward declarations
class ILocalKeyValueStorageReadOnlyIterator;

class ILocalKeyValueStorageReadOnly {
 public:
  // convenience where readVersion==latest, and block is not needed?
  virtual Status get(const Key& key, Value& outValue) const = 0;
  virtual Status get(BlockId readVersion, const Sliver& key, Sliver& outValue, BlockId& outBlock) const = 0;

  virtual BlockId getLastBlock() const = 0;
  virtual Status getBlockData(BlockId blockId, SetOfKeyValuePairs& outBlockData) const = 0;
  // TODO(GG): explain motivation
  virtual Status mayHaveConflictBetween(const Sliver& key, BlockId fromBlock, BlockId toBlock, bool& outRes) const = 0;

  virtual ILocalKeyValueStorageReadOnlyIterator* getSnapIterator() const = 0;
  virtual Status freeSnapIterator(ILocalKeyValueStorageReadOnlyIterator* iter) const = 0;

  virtual void monitor() const = 0;
  virtual ~ILocalKeyValueStorageReadOnly() = default;
};

/*
 * TODO: Iterator cleanup. The general interface should expose only
 * getCurrent() and isEnd(), and there should be two interfaces inheriting
 * from it:
 * 1. One which is "clean", without awareness to blocks
 * 2. Second which is aware of blocks and versions.
 */
class ILocalKeyValueStorageReadOnlyIterator {
 public:
  virtual KeyValuePair first(BlockId readVersion, BlockId& actualVersion, bool& isEnd) = 0;
  virtual KeyValuePair first() = 0;

  // Assumes lexicographical ordering of the keys, seek the first element
  // k >= key
  virtual KeyValuePair seekAtLeast(BlockId readVersion, const Key& key, BlockId& actualVersion, bool& isEnd) = 0;
  virtual KeyValuePair seekAtLeast(const Key& key) = 0;

  // Proceed to next element and return it
  virtual KeyValuePair next(BlockId readVersion, const Key& key, BlockId& actualVersion, bool& isEnd) = 0;
  virtual KeyValuePair next() = 0;

  // Return current element without moving
  virtual KeyValuePair getCurrent() = 0;

  // Check if iterator is in the end.
  virtual bool isEnd() = 0;
};

class IBlocksAppender {
 public:
  virtual Status addBlock(const SetOfKeyValuePairs& updates, BlockId& outBlockId) = 0;
};

}  // namespace blockchain
}  // namespace storage
}  // namespace concord
