// Copyright 2018 VMware, all rights reserved
//
// Storage key comparators definition.

#pragma once
#ifdef USE_ROCKSDB

#include "Logger.hpp"
#include <rocksdb/comparator.h>
#include <rocksdb/slice.h>
#include "sliver.hpp"
#include "storage/db_interface.h"

namespace concord {
namespace storage {
namespace rocksdb {

using concordUtils::Sliver;

// Basic comparator. Decomposes storage key into parts (type, version,
// application key).

class KeyComparator : public ::rocksdb::Comparator {
 public:
  KeyComparator(IDBClient::IKeyComparator* key_comparator)
      : key_comparator_(key_comparator), logger_(logging::getLogger("concord.storage.rocksdb.KeyComparator")) {}
  virtual int Compare(const ::rocksdb::Slice& _a, const ::rocksdb::Slice& _b) const override;
  virtual const char* Name() const override { return "RocksKeyComparator"; }
  virtual void FindShortestSeparator(std::string*, const ::rocksdb::Slice&) const override {}
  virtual void FindShortSuccessor(std::string*) const override {}

 private:
  std::shared_ptr<IDBClient::IKeyComparator> key_comparator_;
  logging::Logger logger_;
};

}  // namespace rocksdb
}  // namespace storage
}  // namespace concord
#endif
