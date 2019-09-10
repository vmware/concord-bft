// Copyright 2018 VMware, all rights reserved
//
// Storage key comparators implementation.

#ifdef USE_ROCKSDB

#include "rocksdb/key_comparator.h"
#include "Logger.hpp"

#include "hex_tools.h"
#include "sliver.hpp"
#include "rocksdb/client.h"

#include <chrono>

using concordUtils::Sliver;
using concordlogger::Logger;
using concord::storage::rocksdb::copyRocksdbSlice;

namespace concord {
namespace storage {
namespace rocksdb {


int KeyComparator::Compare(const ::rocksdb::Slice& _a,
                           const ::rocksdb::Slice& _b) const {
  Sliver a = copyRocksdbSlice(_a);
  Sliver b = copyRocksdbSlice(_b);
  int ret = key_manipulator_->composedKeyComparison(a, b);

  LOG_TRACE(logger_, "Compared " << a << " with " << b << ", returning " << ret);

  return ret;
}

}
}
}
#endif
