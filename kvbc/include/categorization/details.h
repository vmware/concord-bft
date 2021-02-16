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

}  // namespace concord::kvbc::categorization::detail
