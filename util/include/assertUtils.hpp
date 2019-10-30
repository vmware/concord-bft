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

#include "Logger.hpp"
#include <cxxabi.h>
#include <cassert>
#include <cstring>
#include <execinfo.h>

inline void printCallStack() {
  const uint32_t MAX_FRAMES = 100;
  void *addrlist[MAX_FRAMES];
  int addrLen = backtrace(addrlist, MAX_FRAMES);
  if (addrLen) {
    char **symbolsList = backtrace_symbols(addrlist, addrLen);
    if (symbolsList) {
      std::ostringstream os;
      const size_t MAX_FUNC_NAME_SIZE = 256;
      char funcName[MAX_FUNC_NAME_SIZE];
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
          char *ret = abi::__cxa_demangle(beginName, funcName, (size_t *)&MAX_FUNC_NAME_SIZE, &status);
          if (status == 0) {
            std::memcpy(funcName, ret, MAX_FUNC_NAME_SIZE);
            os << " [bt] " << funcName << "+" << beginOffset << std::endl;
          }
        }
      }
      LOG_ERROR(GL, "\n" << os.str());
      std::free(symbolsList);
    }
  }
}

#define PRINT_DATA_AND_ASSERT(expr1, expr2, assertMacro)                                                         \
  {                                                                                                              \
    std::string result1 = (expr1) ? "true" : "false";                                                            \
    std::string result2 = (expr2) ? "true" : "false";                                                            \
    LOG_ERROR(GL,                                                                                                \
              " " << assertMacro << ": expression '" << #expr1 << "' is " << result1.c_str() << ", expression '" \
                  << #expr2 << "' is " << result2.c_str() << " in function " << __FUNCTION__ << " (" << __FILE__ \
                  << " " << __LINE__ << ")");                                                                    \
    printCallStack();                                                                                            \
    assert(false);                                                                                               \
  }

#define Assert(expr)                                                                                              \
  {                                                                                                               \
    if ((expr) != true) {                                                                                         \
      LOG_ERROR(GL,                                                                                               \
                " Assert: expression '" << #expr << "' is false in function " << __FUNCTION__ << " (" << __FILE__ \
                                        << " " << __LINE__ << ")");                                               \
      printCallStack();                                                                                           \
      assert(false);                                                                                              \
    }                                                                                                             \
  }

// Assert(expr1 || expr2)
#define AssertOR(expr1, expr2)                                                               \
  {                                                                                          \
    if ((expr1) != true && (expr2) != true) PRINT_DATA_AND_ASSERT(expr1, expr2, "AssertOR"); \
  }

// Assert(expr1 && expr2)
#define AssertAND(expr1, expr2)                                                               \
  {                                                                                           \
    if ((expr1) != true || (expr2) != true) PRINT_DATA_AND_ASSERT(expr1, expr2, "AssertAND"); \
  }
