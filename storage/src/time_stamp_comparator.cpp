#include "rocksdb/time_stamp_comparator.h"
#include "endianness.hpp"
#include "assertUtils.hpp"

namespace concord {
namespace storage {
namespace rocksdb {

#ifdef USE_ROCKSDB

using namespace ::rocksdb;

class ComparatorWithU64Ts : public Comparator {
 public:
  ComparatorWithU64Ts() : Comparator(/*ts_sz=*/sizeof(uint64_t)), cmp_without_ts_(BytewiseComparator()) {
    littleEndian_ = concordUtils::isLittleEndian();
    ConcordAssert(cmp_without_ts_ != nullptr);
    ConcordAssert(cmp_without_ts_->timestamp_size() == 0);
  }
  const char* Name() const override { return "ComparatorWithU64Ts"; }
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
    ConcordAssert(!a_has_ts || a.size() >= ts_sz);
    ConcordAssert(!b_has_ts || b.size() >= ts_sz);
    Slice lhs = a_has_ts ? StripTimestampFromUserKey(a, ts_sz) : a;
    Slice rhs = b_has_ts ? StripTimestampFromUserKey(b, ts_sz) : b;
    return cmp_without_ts_->Compare(lhs, rhs);
  }
  int CompareTimestamp(const Slice& ts1, const Slice& ts2) const override {
    ConcordAssert(ts1.size() == sizeof(uint64_t));
    ConcordAssert(ts2.size() == sizeof(uint64_t));
    uint64_t lhs = DecodeFixed64(ts1.data());
    uint64_t rhs = DecodeFixed64(ts2.data());
    if (lhs < rhs) {
      return -1;
    } else if (lhs > rhs) {
      return 1;
    } else {
      return 0;
    }
  }
  Slice StripTimestampFromUserKey(const Slice& user_key, size_t ts_sz) const {
    ConcordAssert(user_key.size() >= ts_sz);
    return Slice(user_key.data(), user_key.size() - ts_sz);
  }

  Slice ExtractTimestampFromUserKey(const Slice& user_key, size_t ts_sz) const {
    ConcordAssert(user_key.size() >= ts_sz);
    return Slice(user_key.data() + user_key.size() - ts_sz, ts_sz);
  }

  uint32_t DecodeFixed32(const char* ptr) const {
    if (littleEndian_) {
      // Load the raw bytes
      uint32_t result;
      memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
      return result;
    } else {
      return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0]))) |
              (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8) |
              (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16) |
              (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
    }
  }

  uint64_t DecodeFixed64(const char* ptr) const {
    if (littleEndian_) {
      // Load the raw bytes
      uint64_t result;
      memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
      return result;
    } else {
      uint64_t lo = DecodeFixed32(ptr);
      uint64_t hi = DecodeFixed32(ptr + 4);
      return (hi << 32) | lo;
    }
  }

 private:
  const Comparator* cmp_without_ts_{nullptr};
  bool littleEndian_{};
};

const Comparator* getComparatorWithU64Ts() {
  static ComparatorWithU64Ts instance_;
  return &instance_;
}

class Lexicographic64TsComparator : public Comparator {
 public:
  Lexicographic64TsComparator() : Comparator(/*ts_sz=*/sizeof(uint64_t)), cmp_without_ts_(BytewiseComparator()) {
    ConcordAssert(cmp_without_ts_ != nullptr);
    ConcordAssert(cmp_without_ts_->timestamp_size() == 0);
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
    ConcordAssert(!a_has_ts || a.size() >= ts_sz);
    ConcordAssert(!b_has_ts || b.size() >= ts_sz);
    Slice lhs = a_has_ts ? StripTimestampFromUserKey(a, ts_sz) : a;
    Slice rhs = b_has_ts ? StripTimestampFromUserKey(b, ts_sz) : b;
    return cmp_without_ts_->Compare(lhs, rhs);
  }
  int CompareTimestamp(const Slice& ts1, const Slice& ts2) const override {
    ConcordAssert(ts1.size() == sizeof(uint64_t));
    ConcordAssert(ts2.size() == sizeof(uint64_t));
    return ts1.compare(ts2);
  }
  Slice StripTimestampFromUserKey(const Slice& user_key, size_t ts_sz) const {
    ConcordAssert(user_key.size() >= ts_sz);
    return Slice(user_key.data(), user_key.size() - ts_sz);
  }

  Slice ExtractTimestampFromUserKey(const Slice& user_key, size_t ts_sz) const {
    ConcordAssert(user_key.size() >= ts_sz);
    return Slice(user_key.data() + user_key.size() - ts_sz, ts_sz);
  }

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