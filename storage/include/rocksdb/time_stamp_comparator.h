// Copyright 2022 VMware, all rights reserved

#pragma once
#ifdef USE_ROCKSDB

#include <rocksdb/comparator.h>

namespace concord {
namespace storage {
namespace rocksdb {

/*
Returns a Comparator to compare user defined timestamp for using with
RocksDB timestamp API.
This comparator is used with uint64_t encoded as big endian string using
concordUtils::toBigEndianStringBuffer

it compares the byte order of two such strings.
*/
const ::rocksdb::Comparator* getLexicographic64TsComparator();

}  // namespace rocksdb
}  // namespace storage
}  // namespace concord
#endif
