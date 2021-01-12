// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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

#include <cxxabi.h>
#include <cassert>
#include <cstring>
#include <execinfo.h>
#include <sstream>

#include "kvstream.h"
#include "Logger.hpp"

inline void printCallStack() {
  const uint32_t MAX_FRAMES = 100;
  void *addrlist[MAX_FRAMES];
  int addrLen = backtrace(addrlist, MAX_FRAMES);
  if (addrLen) {
    char **symbolsList = backtrace_symbols(addrlist, addrLen);
    if (symbolsList) {
      std::ostringstream os;
      const size_t MAX_FUNC_NAME_SIZE = 256;
      // Iterate over the returned symbol lines. Skip the first, it is the address of this function.
      for (int i = 1; i < addrLen; i++) {
        char *beginName = nullptr, *beginOffset = nullptr, *endOffset = nullptr;
        for (char *ptr = symbolsList[i]; *ptr; ++ptr) {
          if (*ptr == '(')
            beginName = ptr;
          else if (*ptr == '+')
            beginOffset = ptr;
          else if (*ptr == ')' && beginOffset) {
            endOffset = ptr;
            break;
          }
        }
        if (beginName && beginOffset && endOffset && beginName < beginOffset) {
          *beginName++ = '\0';
          *beginOffset++ = '\0';
          *endOffset = '\0';
          int status;
          size_t demangledSize;
          char *ret = abi::__cxa_demangle(beginName, nullptr, &demangledSize, &status);
          if (status == 0) {
            if (demangledSize > MAX_FUNC_NAME_SIZE) {
              ret[MAX_FUNC_NAME_SIZE] = '\0';
            }
            os << " [bt] " << ret << "+" << beginOffset << std::endl;
          }
          free(ret);
        }
      }
      LOG_FATAL(GL, "\n" << os.str());
      std::free(symbolsList);
    }
  }
}

#define PRINT_DATA_AND_ASSERT_BOOL_EXPR(expr1, expr2, assertMacro)                                                 \
  {                                                                                                                \
    std::string result1 = (expr1) ? "true" : "false";                                                              \
    std::string result2 = (expr2) ? "true" : "false";                                                              \
    LOG_FATAL(GL,                                                                                                  \
              " " << (assertMacro) << ": expression '" << #expr1 << "' is " << result1.c_str() << ", expression '" \
                  << #expr2 << "' is " << result2.c_str() << " in function " << __FUNCTION__ << " (" << __FILE__   \
                  << " " << __LINE__ << ")");                                                                      \
    printCallStack();                                                                                              \
    std::terminate();                                                                                              \
  }

#define PRINT_DATA_AND_ASSERT(expr1, expr2, assertMacro)                                                        \
  {                                                                                                             \
    LOG_FATAL(GL,                                                                                               \
              " " << (assertMacro) << KVLOG_FOR_ASSERT(expr1, expr2) << " in function " << __FUNCTION__ << " (" \
                  << __FILE__ << " " << __LINE__ << ")");                                                       \
    printCallStack();                                                                                           \
    std::terminate();                                                                                           \
  }

#define ConcordAssert(expr)                                                                                       \
  {                                                                                                               \
    if ((expr) != true) {                                                                                         \
      LOG_FATAL(GL,                                                                                               \
                " Assert: expression '" << #expr << "' is false in function " << __FUNCTION__ << " (" << __FILE__ \
                                        << " " << __LINE__ << ")");                                               \
      printCallStack();                                                                                           \
      std::terminate();                                                                                           \
    }                                                                                                             \
  }
// Assert (expr1 == expr2)
#define ConcordAssertEQ(expr1, expr2)                                        \
  {                                                                          \
    if ((expr1) != (expr2)) PRINT_DATA_AND_ASSERT(expr1, expr2, "AssertEQ"); \
  }
// Assert (expr1 != expr2)
#define ConcordAssertNE(expr1, expr2)                                        \
  {                                                                          \
    if ((expr1) == (expr2)) PRINT_DATA_AND_ASSERT(expr1, expr2, "AssertNE"); \
  }
// Assert (expr1 >= expr2)
#define ConcordAssertGE(expr1, expr2)                                       \
  {                                                                         \
    if ((expr1) < (expr2)) PRINT_DATA_AND_ASSERT(expr1, expr2, "AssertGE"); \
  }

// Assert (expr1 > expr2)
#define ConcordAssertGT(expr1, expr2)                                        \
  {                                                                          \
    if ((expr1) <= (expr2)) PRINT_DATA_AND_ASSERT(expr1, expr2, "AssertGT"); \
  }

// Assert (expr1 < expr2)
#define ConcordAssertLT(expr1, expr2)                                        \
  {                                                                          \
    if ((expr1) >= (expr2)) PRINT_DATA_AND_ASSERT(expr1, expr2, "AssertLT"); \
  }

// Assert (expr1 <= expr2)
#define ConcordAssertLE(expr1, expr2)                                       \
  {                                                                         \
    if ((expr1) > (expr2)) PRINT_DATA_AND_ASSERT(expr1, expr2, "AssertLE"); \
  }

// Assert (expr1 || expr2)
#define ConcordAssertOR(expr1, expr2)                                                                  \
  {                                                                                                    \
    if ((expr1) != true && (expr2) != true) PRINT_DATA_AND_ASSERT_BOOL_EXPR(expr1, expr2, "AssertOR"); \
  }

// Assert (expr1 && expr2)
// TODO: AJS - Remove this. This is doesn't take advantage of value printing in
// PRINT_DATA_AND_ASSERT. All uses should be changed to be separate asserts.
// Ideally we'd do similar for AssertOR, but this requires conditionals outside of asserts.
#define ConcordAssertAND(expr1, expr2)                                                                  \
  {                                                                                                     \
    if ((expr1) != true || (expr2) != true) PRINT_DATA_AND_ASSERT_BOOL_EXPR(expr1, expr2, "AssertAND"); \
  }
