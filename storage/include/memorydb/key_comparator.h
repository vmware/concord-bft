// Copyright 2018 VMware, all rights reserved
//
// Storage key comparators definition.

#pragma once

#include "Logger.hpp"
#include "sliver.hpp"
#include "storage/db_interface.h"

namespace concord {
namespace storage {
namespace memorydb {

using concordUtils::Sliver;

// Basic comparator. Decomposes storage key into parts (type, version,
// application key).

class KeyComparator {
 public:
  KeyComparator(IDBClient::IKeyComparator *key_comparator)
      : key_comparator_(key_comparator),
        logger_(concordlogger::Log::getLogger("concord.storage.rocksdb.KeyComparator")) {}

  bool operator()(const Sliver &a, const Sliver &b) const {
    int ret = key_comparator_->composedKeyComparison(a.data(), a.length(), b.data(), b.length());
    return ret < 0;
  }

 private:
  std::shared_ptr<IDBClient::IKeyComparator> key_comparator_;
  concordlogger::Logger logger_;
};

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
