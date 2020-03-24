// Copyright 2018-2019 VMware, all rights reserved

#pragma once

#include <iterator>
#include <set>
#include <string>
#include <unordered_map>
#include "sliver.hpp"
#include "status.hpp"
#include "storage/db_interface.h"
#include "kv_types.hpp"

using std::pair;
using std::string;
using std::unordered_map;

namespace concord {
namespace storage {
namespace blockchain {

using concordUtils::Status;
using concord::kvbc::Key;
using concord::kvbc::Value;
using concord::kvbc::BlockId;

class ILocalKeyValueStorageReadOnly {
 public:
  // convenience where readVersion==latest, and block is not needed?
  virtual Status get(const Key& key, Value& outValue) const = 0;
  virtual Status get(BlockId readVersion, const Sliver& key, Sliver& outValue, BlockId& outBlock) const = 0;

  virtual BlockId getLastBlock() const = 0;
  virtual Status getBlockData(BlockId blockId, SetOfKeyValuePairs& outBlockData) const = 0;
  // TODO(GG): explain motivation
  virtual Status mayHaveConflictBetween(const Sliver& key, BlockId fromBlock, BlockId toBlock, bool& outRes) const = 0;

  virtual void monitor() const = 0;
  virtual ~ILocalKeyValueStorageReadOnly() = default;
};

class IBlocksAppender {
 public:
  virtual Status addBlock(const SetOfKeyValuePairs& updates, BlockId& outBlockId) = 0;
};

}  // namespace blockchain
}  // namespace storage
}  // namespace concord
