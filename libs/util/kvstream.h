#pragma once

#include <sstream>

#include "util/type_traits.hpp"

// Take up to 16 values and create up to 32 function arguments, surrounded by parenthesis, where every other argument is
// a key, and the following argument is a value.
//
// This stringizes the arguments passed in and uses them as the keys in each key value pair, with the argument as the
// value. If you want a specific key, just create an alias variable.
//
// This is especially useful in generating key-value parameter lists such that they can be unpacked in variadic
// templates.
//
// Example usage:
// #define WRAPPER_MACRO(...) SomeCPPFunction KVARGS(__VA_ARGS__)
#define GET_MACRO(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, NAME, ...) NAME
#define KVARGS(...)      \
  GET_MACRO(__VA_ARGS__, \
            KVARGS16,    \
            KVARGS15,    \
            KVARGS14,    \
            KVARGS13,    \
            KVARGS12,    \
            KVARGS11,    \
            KVARGS10,    \
            KVARGS9,     \
            KVARGS8,     \
            KVARGS7,     \
            KVARGS6,     \
            KVARGS5,     \
            KVARGS4,     \
            KVARGS3,     \
            KVARGS2,     \
            KVARGS1,     \
            UNUSED)      \
  (__VA_ARGS__)
#define KVARGS16(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16) \
  (#_1,                                                                                 \
   _1,                                                                                  \
   #_2,                                                                                 \
   _2,                                                                                  \
   #_3,                                                                                 \
   _3,                                                                                  \
   #_4,                                                                                 \
   _4,                                                                                  \
   #_5,                                                                                 \
   _5,                                                                                  \
   #_6,                                                                                 \
   _6,                                                                                  \
   #_7,                                                                                 \
   _7,                                                                                  \
   #_8,                                                                                 \
   _8,                                                                                  \
   #_9,                                                                                 \
   _9,                                                                                  \
   #_10,                                                                                \
   _10,                                                                                 \
   #_11,                                                                                \
   _11,                                                                                 \
   #_12,                                                                                \
   _12,                                                                                 \
   #_13,                                                                                \
   _13,                                                                                 \
   #_14,                                                                                \
   _14,                                                                                 \
   #_15,                                                                                \
   _15,                                                                                 \
   #_16,                                                                                \
   _16)
#define KVARGS15(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15) \
  (#_1,                                                                            \
   _1,                                                                             \
   #_2,                                                                            \
   _2,                                                                             \
   #_3,                                                                            \
   _3,                                                                             \
   #_4,                                                                            \
   _4,                                                                             \
   #_5,                                                                            \
   _5,                                                                             \
   #_6,                                                                            \
   _6,                                                                             \
   #_7,                                                                            \
   _7,                                                                             \
   #_8,                                                                            \
   _8,                                                                             \
   #_9,                                                                            \
   _9,                                                                             \
   #_10,                                                                           \
   _10,                                                                            \
   #_11,                                                                           \
   _11,                                                                            \
   #_12,                                                                           \
   _12,                                                                            \
   #_13,                                                                           \
   _13,                                                                            \
   #_14,                                                                           \
   _14,                                                                            \
   #_15,                                                                           \
   _15)
#define KVARGS14(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14) \
  (#_1,                                                                       \
   _1,                                                                        \
   #_2,                                                                       \
   _2,                                                                        \
   #_3,                                                                       \
   _3,                                                                        \
   #_4,                                                                       \
   _4,                                                                        \
   #_5,                                                                       \
   _5,                                                                        \
   #_6,                                                                       \
   _6,                                                                        \
   #_7,                                                                       \
   _7,                                                                        \
   #_8,                                                                       \
   _8,                                                                        \
   #_9,                                                                       \
   _9,                                                                        \
   #_10,                                                                      \
   _10,                                                                       \
   #_11,                                                                      \
   _11,                                                                       \
   #_12,                                                                      \
   _12,                                                                       \
   #_13,                                                                      \
   _13,                                                                       \
   #_14,                                                                      \
   _14)
#define KVARGS13(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13) \
  (#_1,                                                                  \
   _1,                                                                   \
   #_2,                                                                  \
   _2,                                                                   \
   #_3,                                                                  \
   _3,                                                                   \
   #_4,                                                                  \
   _4,                                                                   \
   #_5,                                                                  \
   _5,                                                                   \
   #_6,                                                                  \
   _6,                                                                   \
   #_7,                                                                  \
   _7,                                                                   \
   #_8,                                                                  \
   _8,                                                                   \
   #_9,                                                                  \
   _9,                                                                   \
   #_10,                                                                 \
   _10,                                                                  \
   #_11,                                                                 \
   _11,                                                                  \
   #_12,                                                                 \
   _12,                                                                  \
   #_13,                                                                 \
   _13)
#define KVARGS12(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12) \
  (#_1, _1, #_2, _2, #_3, _3, #_4, _4, #_5, _5, #_6, _6, #_7, _7, #_8, _8, #_9, _9, #_10, _10, #_11, _11, #_12, _12)
#define KVARGS11(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11) \
  (#_1, _1, #_2, _2, #_3, _3, #_4, _4, #_5, _5, #_6, _6, #_7, _7, #_8, _8, #_9, _9, #_10, _10, #_11, _11)
#define KVARGS10(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10) \
  (#_1, _1, #_2, _2, #_3, _3, #_4, _4, #_5, _5, #_6, _6, #_7, _7, #_8, _8, #_9, _9, #_10, _10)
#define KVARGS9(_1, _2, _3, _4, _5, _6, _7, _8, _9) \
  (#_1, _1, #_2, _2, #_3, _3, #_4, _4, #_5, _5, #_6, _6, #_7, _7, #_8, _8, #_9, _9)
#define KVARGS8(_1, _2, _3, _4, _5, _6, _7, _8) (#_1, _1, #_2, _2, #_3, _3, #_4, _4, #_5, _5, #_6, _6, #_7, _7, #_8, _8)
#define KVARGS7(_1, _2, _3, _4, _5, _6, _7) (#_1, _1, #_2, _2, #_3, _3, #_4, _4, #_5, _5, #_6, _6, #_7, _7)
#define KVARGS6(_1, _2, _3, _4, _5, _6) (#_1, _1, #_2, _2, #_3, _3, #_4, _4, #_5, _5, #_6, _6)
#define KVARGS5(_1, _2, _3, _4, _5) (#_1, _1, #_2, _2, #_3, _3, #_4, _4, #_5, _5)
#define KVARGS4(_1, _2, _3, _4) (#_1, _1, #_2, _2, #_3, _3, #_4, _4)
#define KVARGS3(_1, _2, _3) (#_1, _1, #_2, _2, #_3, _3)
#define KVARGS2(_1, _2) (#_1, _1, #_2, _2)
#define KVARGS1(_1) (#_1, _1)

#define UNUSED(expr) (void)(expr)

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
