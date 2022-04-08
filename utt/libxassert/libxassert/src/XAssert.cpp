/*
 * Assert.h
 *
 *  Created on: Nov 8, 2014
 *      Author: Alin Tomescu <alinush@mit.edu>
 */

#include <cstdio>
#include <cstdlib>

//#include <signal.h>

#ifndef _WIN32
#include <execinfo.h>
#include <unistd.h>
#endif

#include <xassert/XAssert.h>

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
