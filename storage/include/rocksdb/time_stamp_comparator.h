// Copyright 2022 VMware, all rights reserved

#pragma once
#ifdef USE_ROCKSDB

#include <rocksdb/comparator.h>

namespace concord {
namespace storage {
namespace rocksdb {

const ::rocksdb::Comparator* getComparatorWithU64Ts();
const ::rocksdb::Comparator* getLexicographic64TsComparator();

}  // namespace rocksdb
}  // namespace storage
}  // namespace concord
#endif
