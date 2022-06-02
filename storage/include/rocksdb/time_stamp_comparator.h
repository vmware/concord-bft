// Copyright 2022 VMware, all rights reserved

#pragma once
#ifdef USE_ROCKSDB

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
// static constexpr size_t TIME_STAMP_SIZE = sizeof(std::uint64_t);
// ::rocksdb::Slice ExtractTimestampFromUserKey(const ::rocksdb::Slice& user_key, size_t ts_sz);
// ::rocksdb::Slice StripTimestampFromUserKey(const ::rocksdb::Slice& user_key, size_t ts_sz);
}  // namespace rocksdb
}  // namespace storage
}  // namespace concord
#endif
