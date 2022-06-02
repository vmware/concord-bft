#include "rocksdb/time_stamp_comparator.h"
#include "endianness.hpp"
#include "assertUtils.hpp"

namespace concord {
namespace storage {
namespace rocksdb {

#ifdef USE_ROCKSDB

// using namespace ::rocksdb;

// Slice ExtractTimestampFromUserKey(const Slice& user_key, size_t ts_sz) {
//   ConcordAssert(user_key.size() >= ts_sz);
//   return Slice(user_key.data() + user_key.size() - ts_sz, ts_sz);
// }

// Slice StripTimestampFromUserKey(const Slice& user_key, size_t ts_sz) {
//   ConcordAssertGE(user_key.size(), ts_sz);
//   return Slice(user_key.data(), user_key.size() - ts_sz);
// }

#endif

}  // namespace rocksdb
}  // namespace storage
}  // namespace concord