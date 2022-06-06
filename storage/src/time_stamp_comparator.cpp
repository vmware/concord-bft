#include "rocksdb/time_stamp_comparator.h"
#include "endianness.hpp"
#include "assertUtils.hpp"

namespace concord {
namespace storage {
namespace rocksdb {

#ifdef USE_ROCKSDB

using namespace ::rocksdb;

Slice ExtractTimestampFromUserKey(const Slice& user_key, size_t ts_sz) {
  ConcordAssert(user_key.size() >= ts_sz);
  return Slice(user_key.data() + user_key.size() - ts_sz, ts_sz);
}

Slice StripTimestampFromUserKey(const Slice& user_key, size_t ts_sz) {
  ConcordAssertGE(user_key.size(), ts_sz);
  return Slice(user_key.data(), user_key.size() - ts_sz);
}

class Lexicographic64TsComparator : public Comparator {
 public:
  Lexicographic64TsComparator() : Comparator(/*ts_sz=*/TIME_STAMP_SIZE), cmp_without_ts_(BytewiseComparator()) {
    ConcordAssertNE(cmp_without_ts_, nullptr);
    ConcordAssertEQ(cmp_without_ts_->timestamp_size(), 0);
  }
  const char* Name() const override { return "Lexicographic64TsComparator"; }
  void FindShortSuccessor(std::string*) const override {}
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  int Compare(const Slice& a, const Slice& b) const override {
    int ret = CompareWithoutTimestamp(a, b);
    size_t ts_sz = timestamp_size();
    if (ret != 0) {
      return ret;
    }
    // Compare timestamp.
    // For the same user key with different timestamps, larger (newer) timestamp
    // comes first.
    return -CompareTimestamp(ExtractTimestampFromUserKey(a, ts_sz), ExtractTimestampFromUserKey(b, ts_sz));
  }
  using Comparator::CompareWithoutTimestamp;
  int CompareWithoutTimestamp(const Slice& a, bool a_has_ts, const Slice& b, bool b_has_ts) const override {
    const size_t ts_sz = timestamp_size();
    if (a_has_ts) ConcordAssertGE(a.size(), ts_sz);
    if (b_has_ts) ConcordAssertGE(b.size(), ts_sz);
    Slice lhs = a_has_ts ? StripTimestampFromUserKey(a, ts_sz) : a;
    Slice rhs = b_has_ts ? StripTimestampFromUserKey(b, ts_sz) : b;
    return cmp_without_ts_->Compare(lhs, rhs);
  }
  int CompareTimestamp(const Slice& ts1, const Slice& ts2) const override {
    ConcordAssertEQ(ts1.size(), TIME_STAMP_SIZE);
    ConcordAssertEQ(ts2.size(), TIME_STAMP_SIZE);
    return ts1.compare(ts2);
  }

  virtual ~Lexicographic64TsComparator() = default;

 private:
  const Comparator* cmp_without_ts_{nullptr};
};

const Comparator* getLexicographic64TsComparator() {
  static Lexicographic64TsComparator instance_;
  return &instance_;
}

#endif

}  // namespace rocksdb
}  // namespace storage
}  // namespace concord