// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>

// TODO(GG): consider to use standard utilities

#ifdef ASESERT_WITH_STACK_PRINT
#include "Logger.hpp"
#define PRINT_STACK Logger::printLastStackFrames()
#else
#define PRINT_STACK
#endif

#define Assert(expr)                                           \
  {                                                            \
    if ((expr) != true) {                                      \
      PRINT_STACK;                                             \
      printf("'%s' is NOT true (in function '%s' in %s:%d)\n", \
             #expr,                                            \
             __FUNCTION__,                                     \
             __FILE__,                                         \
             __LINE__);                                        \
      assert(false);                                           \
    }                                                          \
  }
