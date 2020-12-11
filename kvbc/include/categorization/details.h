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

template <typename T>
const Buffer &serialize(const T &value) {
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

inline void createColumnFamilyIfNotExisting(const std::string &cf, storage::rocksdb::NativeClient &db) {
  if (!db.hasColumnFamily(cf)) {
    db.createColumnFamily(cf);
  }
}

}  // namespace concord::kvbc::categorization::detail
