// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <cstdio>
#include <cstdlib>

#ifndef _WIN32
#include <execinfo.h>
#include <unistd.h>
#endif

#include "XAssert.h"

namespace XAssert {

bool coredump() {
  XASSERT_OSTREAM << "Segfaulting so that you can get a coredump and check the backtrace..." << std::endl;
#ifndef _WIN32
  __builtin_trap();
#else
  abort();
#endif
  return true;
}

/**
 * Makes sure the compiler doesn't evaluate expressions passed in to assertX() calls when NDEBUG is defined.
 * If it does, the program will "segfault" instantly to indicate something's wrong.
 */
class XAssertInitializer {
 private:
  static XAssertInitializer xassertInit;

 private:
  XAssertInitializer() {
    // If we're in "no debug" mode, make sure assertTrue(expr) doesn't actually evaluate 'expr'!
#ifdef NDEBUG
    assertTrue(shouldNotBeCalled());
    assertFalse(shouldNotBeCalled());
    assertInclusiveRange(shouldNotBeCalled(), shouldNotBeCalled(), shouldNotBeCalled());
    assertStrictlyGreaterThan(shouldNotBeCalled(), shouldNotBeCalled());
    assertStrictlyLessThan(shouldNotBeCalled(), shouldNotBeCalled());
    assertGreaterThanOrEqual(shouldNotBeCalled(), shouldNotBeCalled());
    assertLessThanOrEqual(shouldNotBeCalled(), shouldNotBeCalled());
    assertEqual(shouldNotBeCalled(), shouldNotBeCalled());
    assertIsZero(shouldNotBeCalled());
    assertNotZero(shouldNotBeCalled());
    assertFail(shouldNotBeCalled());
    assertProperty(4, shouldNotBeCalled());
    assertNotNull(shouldNotBeCalled());
    assertNotEqual(shouldNotBeCalled(), shouldNotBeCalled());
    assertStrictlyPositive(shouldNotBeCalled());
    assertNull(shouldNotBeCalled());

    XASSERT_OSTREAM << "Compiler successfully avoids evaluating expressions in 'assertCall(expr())' calls" << std::endl;
#endif
  }

  bool shouldNotBeCalled() {
    XASSERT_OSTREAM << "Oops, your compiler is not optimizing out expressions passed into XAssert calls." << std::endl;
    return coredump();
  }
};

/**
 * The triggered constructor call makes sure that assert expressions
 * aren't evaluated when compiled in non-debug mode.
 */
XAssertInitializer XAssertInitializer::xassertInit;
}  // namespace XAssert
