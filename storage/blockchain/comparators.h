// Copyright 2018 VMware, all rights reserved
//
// Storage key comparators definition.

#pragma once
#ifdef USE_ROCKSDB

#include "Logger.hpp"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "sliver.hpp"

namespace concord {
namespace storage {
namespace blockchain {

using concordUtils::Sliver;

// Basic comparator. Decomposes storage key into parts (type, version,
// application key).

class RocksKeyComparator : public rocksdb::Comparator {
 public:
  RocksKeyComparator()
      : logger(concordlogger::Log::getLogger(
            "concord.storage.RocksKeyComparator")) {}
  int Compare(const rocksdb::Slice& _a, const rocksdb::Slice& _b) const;

  // GG: Ignore the following methods for now:
  const char* Name() const { return "RocksKeyComparator"; }
  void FindShortestSeparator(std::string*, const rocksdb::Slice&) const {}
  void FindShortSuccessor(std::string*) const {}
  static bool InMemKeyComp(const Sliver& _a, const Sliver& _b);

 private:
  static int ComposedKeyComparison(const concordlogger::Logger& logger,
                                   const Sliver& _a,
                                   const Sliver& _b);

 private:
  concordlogger::Logger logger;
};

}
}
}
#endif
