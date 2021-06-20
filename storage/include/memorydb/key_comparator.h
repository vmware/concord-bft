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

// A KeyComparator without a custom comparator passed by the user provides lexicographical ordering. If 'a' is shorter
// than 'b' and they match up to the length of 'a', then 'a' is considered to preceed 'b'.
class KeyComparator {
 public:
  KeyComparator(IDBClient::IKeyComparator *key_comparator = nullptr)
      : key_comparator_(key_comparator), logger_(logging::getLogger("concord.storage.rocksdb.KeyComparator")) {}

  bool operator()(const Sliver &a, const Sliver &b) const {
    if (key_comparator_) {
      const auto res = key_comparator_->composedKeyComparison(a.data(), a.length(), b.data(), b.length());
      return res < 0;
    }

    return a.compare(b) < 0;
  }

 private:
  std::shared_ptr<IDBClient::IKeyComparator> key_comparator_;
  logging::Logger logger_;
};

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
