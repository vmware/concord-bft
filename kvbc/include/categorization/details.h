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

#include "base_types.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "rocksdb/native_client.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>
#include <queue>

namespace concord::kvbc::categorization::detail {

using Buffer = std::vector<std::uint8_t>;

template <typename Span>
Hash hash(const Span &span) {
  return Hasher{}.digest(span.data(), span.size());
}

inline VersionedKey versionedKey(const std::string &key, BlockId block_id) {
  return VersionedKey{KeyHash{detail::hash(key)}, block_id};
}

inline VersionedKey versionedKey(const Hash &key_hash, BlockId block_id) {
  return VersionedKey{KeyHash{key_hash}, block_id};
}

template <typename T>
const Buffer serialize(const T &value) {
  auto buf = Buffer{};
  serialize(buf, value);
  return buf;
}

// should only be used when the returned serialized value is immediately used. Otherwise the
// reference will be overwritten. It is unsafe to use the result of this function in an inline call
// to batch.put() for example. It's only safe when a copy is made in which case there is no
// advantage to using it.
template <typename T>
const Buffer &serializeThreadLocal(const T &value) {
  static thread_local auto buf = Buffer{};
  buf.clear();
  serialize(buf, value);
  return buf;
}

template <typename Span,
          typename T,
          std::enable_if_t<std::is_convertible_v<decltype(std::declval<Span>().size()), std::size_t> &&
                               std::is_pointer_v<decltype(std::declval<Span>().data())> &&
                               std::is_convertible_v<std::remove_pointer_t<decltype(std::declval<Span>().data())> (*)[],
                                                     const char (*)[]>,
                           int> = 0>
void deserialize(const Span &in, T &out) {
  auto begin = reinterpret_cast<const std::uint8_t *>(in.data());
  deserialize(begin, begin + in.size(), out);
}

template <typename Span,
          typename T,
          std::enable_if_t<std::is_convertible_v<decltype(std::declval<Span>().size()), std::size_t> &&
                               std::is_pointer_v<decltype(std::declval<Span>().data())> &&
                               std::is_convertible_v<std::remove_pointer_t<decltype(std::declval<Span>().data())> (*)[],
                                                     const std::uint8_t (*)[]>,
                           int> = 0>
void deserialize(const Span &in, T &out) {
  auto begin = in.data();
  deserialize(begin, begin + in.size(), out);
}

inline bool createColumnFamilyIfNotExisting(const std::string &cf, storage::rocksdb::NativeClient &db) {
  if (!db.hasColumnFamily(cf)) {
    db.createColumnFamily(cf);
    return true;
  }
  return false;
}

inline void sortAndRemoveDuplicates(std::vector<std::string> &vec) {
  std::sort(vec.begin(), vec.end());
  vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
}
// A LocalWriteBatch is used in a multithreaded context, where several jobs need to collect updates.
// Each job gets its LocalWriteBatch.
// then all updates are added to the Global NativeWriteBatch, which is not thread safe.
struct LocalWriteBatch {
  struct Put {
    std::string cFamily;
    std::string key;
    std::string value;
  };

  struct Delete {
    std::string cFamily;
    std::string key;
  };

  template <typename KeySpan, typename ValueSpan>
  void put(const std::string &cFamily, const KeySpan &key, const ValueSpan &value) {
    puts_.push({cFamily,
                std::string(reinterpret_cast<const char *>(key.data()), key.size()),
                std::string(reinterpret_cast<const char *>(value.data()), value.size())});
    operations_.push(put_op_);
  }

  void moveToBatch(storage::rocksdb::NativeWriteBatch &batch) noexcept {
    while (!operations_.empty()) {
      if (operations_.front() == put_op_) {
        const auto &put = puts_.front();
        batch.put(put.cFamily, put.key, put.value);
        puts_.pop();
      } else {
        const auto &del = dels_.front();
        batch.del(del.cFamily, del.key);
        dels_.pop();
      }
      operations_.pop();
    }
    ConcordAssert(puts_.empty());
    ConcordAssert(dels_.empty());
  }

  template <typename KeySpan>
  void del(const std::string &cFamily, const KeySpan &key) {
    dels_.push(Delete{cFamily, std::string(reinterpret_cast<const char *>(key.data()), key.size())});
    operations_.push(delete_op_);
  }

  std::queue<Put> puts_;
  std::queue<Delete> dels_;
  std::queue<uint8_t> operations_;
  const uint8_t delete_op_{0};
  const uint8_t put_op_{1};
};  // namespace concord::kvbc::categorization::detail

}  // namespace concord::kvbc::categorization::detail
