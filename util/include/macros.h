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
