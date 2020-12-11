// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#ifdef USE_ROCKSDB

#include "details.h"

#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>

#include <memory>
#include <string>
#include <string_view>

namespace concord::storage::rocksdb {

using namespace std::string_view_literals;

// For use with NativeClient. See native_client.h .
class NativeIterator {
 public:
  // Returns true if the iterator points to a key-value pair and false otherwise. Initially, an iterator doesn't point
  // to anything. A false return from this method is not an error - it might mean there are no more keys or a key wasn't
  // found.
  operator bool() const;

  // Methods that move the iterator.
  // Precondition: the iterator points to a key-value pair.
  void first();
  void last();
  void next();
  void prev();

  // Seek for a key that is at or past target.
  template <typename KeySpan>
  void seekAtLeast(const KeySpan &target);

  // Seek for a key that is at or before target.
  template <typename KeySpan>
  void seekAtMost(const KeySpan &target);

  // Copy the key and value from the iterator.
  // Precondition: the iterator points to a key-value pair.
  std::string key() const;
  std::string value() const;

  // A view into the key and value inside the iterator. The return value is valid until the iterator is modified.
  // Precondition: the iterator points to a key-value pair.
  std::string_view keyView() const;
  std::string_view valueView() const;

 private:
  NativeIterator(std::unique_ptr<::rocksdb::Iterator> iter) noexcept;
  static std::string sliceToString(const ::rocksdb::Slice &);
  static std::string_view sliceToStringView(const ::rocksdb::Slice &);

 private:
  std::unique_ptr<::rocksdb::Iterator> iter_;
  friend class NativeClient;
};

inline NativeIterator::NativeIterator(std::unique_ptr<::rocksdb::Iterator> iter) noexcept : iter_{std::move(iter)} {}

inline std::string NativeIterator::sliceToString(const ::rocksdb::Slice &s) { return std::string{s.data(), s.size()}; }

inline std::string_view NativeIterator::sliceToStringView(const ::rocksdb::Slice &s) {
  return std::string_view{s.data(), s.size()};
}

inline NativeIterator::operator bool() const { return iter_->Valid(); }

inline void NativeIterator::first() {
  iter_->SeekToFirst();
  detail::throwOnError("iterator first failed"sv, iter_->status());
}

inline void NativeIterator::last() {
  iter_->SeekToLast();
  detail::throwOnError("iterator last failed"sv, iter_->status());
}

inline void NativeIterator::next() {
  if (!iter_->Valid()) {
    detail::throwOnError("next on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  iter_->Next();
  auto s = iter_->status();
  detail::throwOnError("iterator next failed"sv, iter_->status());
}

inline void NativeIterator::prev() {
  if (!iter_->Valid()) {
    detail::throwOnError("prev on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  iter_->Prev();
  auto s = iter_->status();
  detail::throwOnError("iterator prev failed"sv, iter_->status());
}

template <typename KeySpan>
void NativeIterator::seekAtLeast(const KeySpan &target) {
  iter_->Seek(detail::toSlice(target));
  detail::throwOnError("iterator seekAtLeast failed"sv, iter_->status());
}

template <typename KeySpan>
void NativeIterator::seekAtMost(const KeySpan &target) {
  iter_->SeekForPrev(detail::toSlice(target));
  detail::throwOnError("iterator seekAtMost failed"sv, iter_->status());
}

inline std::string NativeIterator::key() const {
  if (!iter_->Valid()) {
    detail::throwOnError("key on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  return sliceToString(iter_->key());
}

inline std::string NativeIterator::value() const {
  if (!iter_->Valid()) {
    detail::throwOnError("value on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  return sliceToString(iter_->value());
}

inline std::string_view NativeIterator::keyView() const {
  if (!iter_->Valid()) {
    detail::throwOnError("keyView on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  return sliceToStringView(iter_->key());
}

inline std::string_view NativeIterator::valueView() const {
  if (!iter_->Valid()) {
    detail::throwOnError("valueView on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  return sliceToStringView(iter_->value());
}

}  // namespace concord::storage::rocksdb

#endif  // USE_ROCKSDB
