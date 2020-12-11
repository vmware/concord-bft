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

#include "client.h"
#include "rocksdb_exception.h"

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace concord::storage::rocksdb::detail {

using namespace std::string_view_literals;

// Make sure we only allow types that have data() and size() members. That excludes raw pointers without corresponding
// size.
template <typename Span,
          std::enable_if_t<std::is_convertible_v<decltype(std::declval<Span>().size()), std::size_t> &&
                               std::is_pointer_v<decltype(std::declval<Span>().data())> &&
                               std::is_convertible_v<std::remove_pointer_t<decltype(std::declval<Span>().data())> (*)[],
                                                     const char (*)[]>,
                           int> = 0>
::rocksdb::Slice toSlice(const Span &span) noexcept {
  return ::rocksdb::Slice{span.data(), span.size()};
}

template <typename Span,
          std::enable_if_t<std::is_convertible_v<decltype(std::declval<Span>().size()), std::size_t> &&
                               std::is_pointer_v<decltype(std::declval<Span>().data())> &&
                               std::is_convertible_v<std::remove_pointer_t<decltype(std::declval<Span>().data())> (*)[],
                                                     const std::uint8_t (*)[]>,
                           int> = 0>
::rocksdb::Slice toSlice(const Span &span) noexcept {
  return ::rocksdb::Slice{reinterpret_cast<const char *>(span.data()), span.size()};
}

inline void throwOnError(std::string_view msg1, std::string_view msg2, ::rocksdb::Status &&s) {
  if (!s.ok()) {
    auto rocksdbMsg = std::string{"RocksDB: "};
    rocksdbMsg.append(msg1);
    if (!msg2.empty()) {
      rocksdbMsg.append(": ");
      rocksdbMsg.append(msg2);
    }
    LOG_ERROR(Client::logger(), rocksdbMsg << ", status = " << s.ToString());
    throw RocksDBException{rocksdbMsg, std::move(s)};
  }
}

inline void throwOnError(std::string_view msg, ::rocksdb::Status &&s) { return throwOnError(msg, ""sv, std::move(s)); }

}  // namespace concord::storage::rocksdb::detail

#endif  // USE_ROCKSDB
