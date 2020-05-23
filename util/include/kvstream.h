// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <sstream>

// Take up to 16 values and output them to a stream as key value pairs.
//
// This stringizes the value passed in and uses it as the key. If you want a specific key, just create an alias
// variable.
//
// Usage looks like the following:
//   KVLOG(seq_num, view, firstStoredCheckpoint)
//
// Output looks like:
//   "seq_num: <VAL>, view: <VAL>, firstStoredCheckpoint: <VAL>"
//
#define GET_MACRO(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, NAME, ...) NAME
#define KVLOG(...)       \
  GET_MACRO(__VA_ARGS__, \
            KVLOG16,     \
            KVLOG15,     \
            KVLOG14,     \
            KVLOG13,     \
            KVLOG12,     \
            KVLOG11,     \
            KVLOG10,     \
            KVLOG9,      \
            KVLOG8,      \
            KVLOG7,      \
            KVLOG6,      \
            KVLOG5,      \
            KVLOG4,      \
            KVLOG3,      \
            KVLOG2,      \
            KVLOG1)      \
  (__VA_ARGS__)
#define KVLOG16(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16) \
  KvLog(#_16,                                                                          \
        _16,                                                                           \
        #_15,                                                                          \
        _15,                                                                           \
        #_14,                                                                          \
        _14,                                                                           \
        #_13,                                                                          \
        _13,                                                                           \
        #_12,                                                                          \
        _12,                                                                           \
        #_11,                                                                          \
        _11,                                                                           \
        #_10,                                                                          \
        _10,                                                                           \
        #_9,                                                                           \
        _9,                                                                            \
        #_8,                                                                           \
        _8,                                                                            \
        #_7,                                                                           \
        _7,                                                                            \
        #_6,                                                                           \
        _6,                                                                            \
        #_5,                                                                           \
        _5,                                                                            \
        #_4,                                                                           \
        _4,                                                                            \
        #_3,                                                                           \
        _3,                                                                            \
        #_2,                                                                           \
        _2,                                                                            \
        #_1,                                                                           \
        _1)
#define KVLOG15(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15) \
  KvLog(#_15,                                                                     \
        _15,                                                                      \
        #_14,                                                                     \
        _14,                                                                      \
        #_13,                                                                     \
        _13,                                                                      \
        #_12,                                                                     \
        _12,                                                                      \
        #_11,                                                                     \
        _11,                                                                      \
        #_10,                                                                     \
        _10,                                                                      \
        #_9,                                                                      \
        _9,                                                                       \
        #_8,                                                                      \
        _8,                                                                       \
        #_7,                                                                      \
        _7,                                                                       \
        #_6,                                                                      \
        _6,                                                                       \
        #_5,                                                                      \
        _5,                                                                       \
        #_4,                                                                      \
        _4,                                                                       \
        #_3,                                                                      \
        _3,                                                                       \
        #_2,                                                                      \
        _2,                                                                       \
        #_1,                                                                      \
        _1)
#define KVLOG14(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14) \
  KvLog(#_14,                                                                \
        _14,                                                                 \
        #_13,                                                                \
        _13,                                                                 \
        #_12,                                                                \
        _12,                                                                 \
        #_11,                                                                \
        _11,                                                                 \
        #_10,                                                                \
        _10,                                                                 \
        #_9,                                                                 \
        _9,                                                                  \
        #_8,                                                                 \
        _8,                                                                  \
        #_7,                                                                 \
        _7,                                                                  \
        #_6,                                                                 \
        _6,                                                                  \
        #_5,                                                                 \
        _5,                                                                  \
        #_4,                                                                 \
        _4,                                                                  \
        #_3,                                                                 \
        _3,                                                                  \
        #_2,                                                                 \
        _2,                                                                  \
        #_1,                                                                 \
        _1)
#define KVLOG13(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13) \
  KvLog(#_13,                                                           \
        _13,                                                            \
        #_12,                                                           \
        _12,                                                            \
        #_11,                                                           \
        _11,                                                            \
        #_10,                                                           \
        _10,                                                            \
        #_9,                                                            \
        _9,                                                             \
        #_8,                                                            \
        _8,                                                             \
        #_7,                                                            \
        _7,                                                             \
        #_6,                                                            \
        _6,                                                             \
        #_5,                                                            \
        _5,                                                             \
        #_4,                                                            \
        _4,                                                             \
        #_3,                                                            \
        _3,                                                             \
        #_2,                                                            \
        _2,                                                             \
        #_1,                                                            \
        _1)
#define KVLOG12(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12) \
  KvLog(#_12,                                                      \
        _12,                                                       \
        #_11,                                                      \
        _11,                                                       \
        #_10,                                                      \
        _10,                                                       \
        #_9,                                                       \
        _9,                                                        \
        #_8,                                                       \
        _8,                                                        \
        #_7,                                                       \
        _7,                                                        \
        #_6,                                                       \
        _6,                                                        \
        #_5,                                                       \
        _5,                                                        \
        #_4,                                                       \
        _4,                                                        \
        #_3,                                                       \
        _3,                                                        \
        #_2,                                                       \
        _2,                                                        \
        #_1,                                                       \
        _1)
#define KVLOG11(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11) \
  KvLog(#_11, _11, #_10, _10, #_9, _9, #_8, _8, #_7, _7, #_6, _6, #_5, _5, #_4, _4, #_3, _3, #_2, _2, #_1, _1)
#define KVLOG10(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10) \
  KvLog(#_10, _10, #_9, _9, #_8, _8, #_7, _7, #_6, _6, #_5, _5, #_4, _4, #_3, _3, #_2, _2, #_1, _1)
#define KVLOG9(_1, _2, _3, _4, _5, _6, _7, _8, _9) \
  KvLog(#_9, _9, #_8, _8, #_7, _7, #_6, _6, #_5, _5, #_4, _4, #_3, _3, #_2, _2, #_1, _1)
#define KVLOG8(_1, _2, _3, _4, _5, _6, _7, _8) \
  KvLog(#_8, _8, #_7, _7, #_6, _6, #_5, _5, #_4, _4, #_3, _3, #_2, _2, #_1, _1)
#define KVLOG7(_1, _2, _3, _4, _5, _6, _7) KvLog(#_7, _7, #_6, _6, #_5, _5, #_4, _4, #_3, _3, #_2, _2, #_1, _1)
#define KVLOG6(_1, _2, _3, _4, _5, _6) KvLog(#_6, _6, #_5, _5, #_4, _4, #_3, _3, #_2, _2, #_1, _1)
#define KVLOG5(_1, _2, _3, _4, _5) KvLog(#_5, _5, #_4, _4, #_3, _3, #_2, _2, #_1, _1)
#define KVLOG4(_1, _2, _3, _4) KvLog(#_4, _4, #_3, _3, #_2, _2, #_1, _1)
#define KVLOG3(_1, _2, _3) KvLog(#_3, _3, #_2, _2, #_1, _1)
#define KVLOG2(_1, _2) KvLog(#_2, _2, #_1, _1)
#define KVLOG1(_1) KvLog(#_1, _1)

// These are recurisive variadic template functions for generating formatted key-value pairs that
// can be attached to the end of log messages. The entrypoint is the KVLog function at the bottom
// that only takes a variadic arg.
//
// Right now the entrypoint returns a string, because of the way our logging macros are implemented.
// We could change them to take an extra parameter that's a stringstream and prevent the additional
// allocation and copy from returning a string. We'll deal with that if its needed.
template <typename K, typename V>
void KvLog(std::stringstream &ss, K &&key, V &&val) {
  ss << std::forward<K>(key) << ": " << std::forward<V>(val);
}

template <typename K, typename V, typename... KVPAIRS>
void KvLog(std::stringstream &ss, K &&key, V &&val, KVPAIRS &&... kvpairs) {
  ss << std::forward<K>(key) << ": " << std::forward<V>(val) << ", ";
  KvLog(ss, std::forward<KVPAIRS>(kvpairs)...);
}

template <typename... KVPAIRS>
std::string KvLog(KVPAIRS &&... kvpairs) {
  std::stringstream ss;
  ss << "| ";
  KvLog(ss, std::forward<KVPAIRS>(kvpairs)...);
  return ss.str();
}