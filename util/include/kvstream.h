#pragma once

#include <sstream>

#include "macros.h"
#include "type_traits.h"

#define KVLOG(...) KvLog<true> KVARGS(__VA_ARGS__)

// Don't check the to see if the KV arguments implement ostream::operator<<. We want to be able to use asserts on types
// that are not printable.
#define KVLOG_FOR_ASSERT(...) KvLog<false> KVARGS(__VA_ARGS__)

// These are recurisive variadic template functions for generating formatted key-value pairs that
// can be attached to the end of log messages. The entrypoint is the KVLog function at the bottom
// that only takes a variadic arg.
//
// The mandatory `Strict` template parameter determines whether types that do not implement
// `std::ostream::operator<<` are allowed. If Strict=true, a static_assert will fire.
//
// Right now the entrypoint returns a string, because of the way our logging macros are implemented.
// We could change them to take an extra parameter that's a stringstream and prevent the additional
// allocation and copy from returning a string. We'll deal with that if its needed.

// Forward declaration
template <bool Strict,
          typename K,
          typename V,
          typename std::enable_if<!concord::is_streamable<std::ostream, V>::value>::type * = nullptr>
void KvLog(std::stringstream &ss, K &&key, V &&val);

// Forward declaration
template <bool Strict,
          typename K,
          typename V,
          typename... KVPAIRS,
          typename std::enable_if<!concord::is_streamable<std::ostream, V>::value>::type * = nullptr>
void KvLog(std::stringstream &ss, K &&key, V &&val, KVPAIRS &&... kvpairs);

template <bool Strict,
          typename K,
          typename V,
          typename std::enable_if<concord::is_streamable<std::ostream, V>::value>::type * = nullptr>
void KvLog(std::stringstream &ss, K &&key, V &&val) {
  ss << std::forward<K>(key) << ": ";
  if constexpr (std::is_same_v<V, bool &> || std::is_same_v<V, const bool &>) {
    ss << (val ? "True" : "False");
  } else {
    ss << std::forward<V>(val);
  }
}

template <bool Strict,
          typename K,
          typename V,
          typename... KVPAIRS,
          typename std::enable_if<concord::is_streamable<std::ostream, V>::value>::type * = nullptr>
void KvLog(std::stringstream &ss, K &&key, V &&val, KVPAIRS &&... kvpairs) {
  ss << std::forward<K>(key) << ": ";
  if constexpr (std::is_same_v<V, bool &> || std::is_same_v<V, const bool &>) {
    ss << (val ? "True" : "False");
  } else {
    ss << std::forward<V>(val);
  }
  ss << ", ";
  KvLog<Strict>(ss, std::forward<KVPAIRS>(kvpairs)...);
}

template <bool Strict,
          typename K,
          typename V,
          typename std::enable_if<!concord::is_streamable<std::ostream, V>::value>::type *>
void KvLog(std::stringstream &ss, K &&key, V &&) {
  static_assert(!Strict, "Cannot log types that do not implement ostream::operator<<");
  ss << std::forward<K>(key) << ": _";
}

template <bool Strict,
          typename K,
          typename V,
          typename... KVPAIRS,
          typename std::enable_if<!concord::is_streamable<std::ostream, V>::value>::type *>
void KvLog(std::stringstream &ss, K &&key, V &&, KVPAIRS &&... kvpairs) {
  static_assert(!Strict, "Cannot log types that do not implement ostream::operator<<");
  ss << std::forward<K>(key) << ": _, ";
  KvLog<Strict>(ss, std::forward<KVPAIRS>(kvpairs)...);
}

template <bool Strict, typename... KVPAIRS>
std::string KvLog(KVPAIRS &&... kvpairs) {
  std::stringstream ss;
  ss << " ";
  KvLog<Strict>(ss, std::forward<KVPAIRS>(kvpairs)...);
  return ss.str();
}
