// Copyright 2020 VMware, all rights reserved

#include "blockchain/base_db_adapter.h"
#include "blockchain/db_types.h"
#include "kv_types.hpp"
#include "Logger.hpp"

#include <memory>

using concordUtils::Key;
using concordUtils::Sliver;

namespace concord {
namespace storage {
namespace blockchain {

DBAdapterBase::DBAdapterBase(const std::shared_ptr<IDBClient> &db, bool readOnly)
    : logger_{concordlogger::Log::getLogger("concord.storage.BlockchainDBAdapter")}, db_{db} {
  db_->init(readOnly);
}

Key DBAdapterBase::getLatestBlock(const Sliver &maxKey) const {
  // Note: RocksDB stores keys in a sorted fashion as per the logic provided in
  // a custom comparator (for our case, refer to the `composedKeyComparison`
  // method above). In short, keys of type 'block' are stored first followed by
  // keys of type 'key'. All keys of type 'block' are sorted in ascending order
  // of block ids.

  auto iter = db_->getIterator();

  // Since we use the maximal key, SeekAtLeast will take the iterator
  // to one position beyond the key corresponding to the largest block id.
  iter->seekAtLeast(maxKey);

  // Read the previous key
  const auto kv = iter->previous();

  db_->freeIterator(iter);

  return kv.first;
}

}  // namespace blockchain
}  // namespace storage
}  // namespace concord
