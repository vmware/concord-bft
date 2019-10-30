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

#pragma once

#include <cassert>
#include <cstdlib>
#include <typeinfo>

namespace XAssert {

/**
 * Dereferences a null pointer and causes the program to coredump so that
 * the state can be inspected.
 */
bool coredump();

/**
 * Makes sure asserts are enabled or segfaults.
 */
// void assertAssertsEnabled();
}  // namespace XAssert

#ifndef XASSERT_OSTREAM
// This is the std::ostream object where assert error messages go to
#include <iostream>
#define XASSERT_OSTREAM std::cerr
#endif

// Assert error messages start with this
#define XASSERT_START \
  XASSERT_OSTREAM << "Assertion failed in function '" << __FUNCTION__ << "', " << __FILE__ << ":" << __LINE__ << ": "

// Assert errors cause the program to exit with the following call
#define XASSERT_EXIT     \
  {                      \
    XAssert::coredump(); \
    exit(1);             \
  }

#define XASSERT_NO_EVAL(expr) \
  { true ? static_cast<void>(0) : static_cast<void>((expr)); }

/**
 * Here we define all our assert macros, after which we define macros that call them!
 */
#define XASSERT_InclusiveRange(start, middle, end) \
  {                                                \
    assertGreaterThanOrEqual(middle, start);       \
    assertLessThanOrEqual(middle, end);            \
  }
// Checks if 1st > 2nd
#define XASSERT_StrictlyGreaterThan(bigger, smaller)                                                                 \
  {                                                                                                                  \
    if ((bigger) <= (smaller)) {                                                                                     \
      XASSERT_START << "Expected '" << #bigger << "' (" << (bigger) << ") to be strictly greater than '" << #smaller \
                    << "' (" << (smaller) << ")" << std::endl;                                                       \
      XASSERT_EXIT;                                                                                                  \
    }                                                                                                                \
  }

// Checks if 1st < 2nd
#define XASSERT_StrictlyLessThan(smaller, bigger)                                                                  \
  {                                                                                                                \
    if ((smaller) >= (bigger)) {                                                                                   \
      XASSERT_START << "Expected '" << #smaller << "' (" << (smaller) << ") to be strictly less than '" << #bigger \
                    << "' (" << (bigger) << ")" << std::endl;                                                      \
      XASSERT_EXIT;                                                                                                \
    }                                                                                                              \
  }

// Checks if 1st >= 2nd
#define XASSERT_GreaterThanOrEqual(bigger, smaller)                                                         \
  {                                                                                                         \
    if ((bigger) < (smaller)) {                                                                             \
      XASSERT_START << "Expected '" << #bigger << "' (" << (bigger) << ") to be greater than or equal to '" \
                    << #smaller << "' (" << (smaller) << ")" << std::endl;                                  \
      XASSERT_EXIT;                                                                                         \
    }                                                                                                       \
  }

// Checks if 1st <= 2nd
#define XASSERT_LessThanOrEqual(smaller, bigger)                                                                      \
  {                                                                                                                   \
    if ((smaller) > (bigger)) {                                                                                       \
      XASSERT_START << "Expected '" << #smaller << "' (" << (smaller) << ") to be less than or equal to '" << #bigger \
                    << "' (" << (bigger) << ")" << std::endl;                                                         \
      XASSERT_EXIT;                                                                                                   \
    }                                                                                                                 \
  }

// Checks if 1st == 2nd
#define XASSERT_Equal(first, second)                                                                          \
  {                                                                                                           \
    if ((first) != (second)) {                                                                                \
      XASSERT_START << "Expected '" << #first << "' (" << (first) << ") to be equal to '" << #second << "' (" \
                    << (second) << ")" << std::endl;                                                          \
      XASSERT_EXIT;                                                                                           \
    }                                                                                                         \
  }

// Checks if 1st != 2nd
#define XASSERT_NotEqual(first, second)                                                                           \
  {                                                                                                               \
    if ((first) == (second)) {                                                                                    \
      XASSERT_START << "Expected '" << #first << "' (" << (first) << ") to NOT be equal to '" << #second << "' (" \
                    << (second) << ")" << std::endl;                                                              \
      XASSERT_EXIT;                                                                                               \
    }                                                                                                             \
  }

// Checks if first == 0
#define XASSERT_IsZero(first)                                                                     \
  {                                                                                               \
    if ((first)) {                                                                                \
      XASSERT_START << "Expected '" << #first << "' (" << (first) << ") to be zero" << std::endl; \
      XASSERT_EXIT;                                                                               \
    }                                                                                             \
  }

// Checks if first != 0
#define XASSERT_NotZero(first)                                                                        \
  {                                                                                                   \
    if ((first) == 0) {                                                                               \
      XASSERT_START << "Expected '" << #first << "' (" << (first) << ") to NOT be zero" << std::endl; \
      XASSERT_EXIT;                                                                                   \
    }                                                                                                 \
  }

// Exits with the specified error message
#define XASSERT_Fail(msg)                \
  {                                      \
    XASSERT_START << (msg) << std::endl; \
    XASSERT_EXIT;                        \
  }

// Checks if the specified property is true for the specified value
// Useful when you're checking a complex assertion and you want to see the value that failed that assertion.
// e.g., assertProperty(finalSign, finalSign == -1 || finalSign == 1)
#define XASSERT_Property(value, property)                                                                           \
  {                                                                                                                 \
    if ((property) == false) {                                                                                      \
      XASSERT_START << "'" << #property << "' is NOT true for '" << #value << "' (" << (value) << ")" << std::endl; \
      XASSERT_EXIT;                                                                                                 \
    }                                                                                                               \
  }

// Checks if p != NULL
#define XASSERT_NotNull(p)                                    \
  {                                                           \
    if ((p) == nullptr) {                                     \
      XASSERT_START << "'" << #p << "' is NULL" << std::endl; \
      XASSERT_EXIT;                                           \
    }                                                         \
  }

// Checks if x > 0
#define XASSERT_StrictlyPositive(x)                                            \
  {                                                                            \
    if ((x) <= 0) {                                                            \
      XASSERT_START << "'" << #x << "' is NOT greater than zero" << std::endl; \
      XASSERT_EXIT;                                                            \
    }                                                                          \
  }

// Checks if p == NULL
#define XASSERT_Null(p)                                           \
  {                                                               \
    if ((p) != nullptr) {                                         \
      XASSERT_START << "'" << #p << "' is NOT NULL" << std::endl; \
      XASSERT_EXIT;                                               \
    }                                                             \
  }

#define XASSERT_True(e)                                           \
  {                                                               \
    if ((e) != true) {                                            \
      XASSERT_START << "'" << #e << "' is NOT true" << std::endl; \
      XASSERT_EXIT;                                               \
    }                                                             \
  }

#define XASSERT_False(e)                                                             \
  {                                                                                  \
    if ((e) != false) {                                                              \
      XASSERT_START << "'" << #e << "' is NOT false (i.e., it's true)" << std::endl; \
      XASSERT_EXIT;                                                                  \
    }                                                                                \
  }

/**
 * Defining assert* calls during DEBUG builds!
 */
#ifndef NDEBUG

template <class T, class S>
T& checked_ref_cast(S& source) {
  return dynamic_cast<T&>(source);
}

#define assertFalse(expr) XASSERT_False(expr)
#define assertTrue(expr) XASSERT_True(expr)
#define assertInclusiveRange(start, middle, end) XASSERT_InclusiveRange(start, middle, end)
#define assertStrictlyPositive(x) XASSERT_StrictlyPositive(x)
#define assertStrictlyGreaterThan(bigger, smaller) XASSERT_StrictlyGreaterThan(bigger, smaller)
#define assertStrictlyLessThan(smaller, bigger) XASSERT_StrictlyLessThan(smaller, bigger)
#define assertGreaterThanOrEqual(bigger, smaller) XASSERT_GreaterThanOrEqual(bigger, smaller)
#define assertLessThanOrEqual(smaller, bigger) XASSERT_LessThanOrEqual(smaller, bigger)
#define assertEqual(first, second) XASSERT_Equal(first, second)
#define assertIsZero(first) XASSERT_IsZero(first)
#define assertNotZero(first) XASSERT_NotZero(first)
#define assertFail(msg) XASSERT_Fail(msg)
#define assertProperty(value, property) XASSERT_Property(value, property)
#define assertNotNull(p) XASSERT_NotNull(p)
#define assertNotEqual(first, second) XASSERT_NotEqual(first, second)
#define assertNull(p) XASSERT_Null(p)

#else

template <class T, class S>
T& checked_ref_cast(S& source) {
  return reinterpret_cast<T&>(source);
}

/**
 * WARNING: You need the (void)arg around every argument 'arg.' On some compilers, not having it
 * will trigger an "error: right operand of comma operator has no effect [-Werror=unused-value]"
 */
#define assertFalse(expr) XASSERT_NO_EVAL(expr)
#define assertTrue(expr) XASSERT_NO_EVAL(expr)
#define assertInclusiveRange(start, middle, end) XASSERT_NO_EVAL(start) XASSERT_NO_EVAL(middle) XASSERT_NO_EVAL(end)
#define assertStrictlyPositive(x) XASSERT_NO_EVAL(x)
#define assertStrictlyGreaterThan(bigger, smaller) XASSERT_NO_EVAL(bigger) XASSERT_NO_EVAL(smaller)
#define assertStrictlyLessThan(smaller, bigger) XASSERT_NO_EVAL(smaller) XASSERT_NO_EVAL(bigger)
#define assertGreaterThanOrEqual(bigger, smaller) XASSERT_NO_EVAL(bigger) XASSERT_NO_EVAL(smaller)
#define assertLessThanOrEqual(smaller, bigger) XASSERT_NO_EVAL(smaller) XASSERT_NO_EVAL(bigger)
#define assertEqual(first, second) XASSERT_NO_EVAL(first) XASSERT_NO_EVAL(second)
#define assertIsZero(first) XASSERT_NO_EVAL(first)
#define assertNotZero(first) XASSERT_NO_EVAL(first)
#define assertFail(msg) XASSERT_NO_EVAL(msg)
#define assertProperty(value, property) XASSERT_NO_EVAL(value) XASSERT_NO_EVAL(property)
#define assertNotNull(p) XASSERT_NO_EVAL(p)
#define assertNotEqual(first, second) XASSERT_NO_EVAL(first) XASSERT_NO_EVAL(second)
#define assertNull(p) XASSERT_NO_EVAL(p)

#endif

#define testAssertFalse(expr) XASSERT_False(expr)
#define testAssertTrue(expr) XASSERT_True(expr)
#define testAssertInclusiveRange(start, middle, end) XASSERT_InclusiveRange(start, middle, end)
#define testAssertStrictlyPositive(x) XASSERT_StrictlyPositive(x)
#define testAssertStrictlyGreaterThan(bigger, smaller) XASSERT_StrictlyGreaterThan(bigger, smaller)
#define testAssertStrictlyLessThan(smaller, bigger) XASSERT_StrictlyLessThan(smaller, bigger)
#define testAssertGreaterThanOrEqual(bigger, smaller) XASSERT_GreaterThanOrEqual(bigger, smaller)
#define testAssertLessThanOrEqual(smaller, bigger) XASSERT_LessThanOrEqual(smaller, bigger)
#define testAssertEqual(first, second) XASSERT_Equal(first, second)
#define testAssertIsZero(first) XASSERT_IsZero(first)
#define testAssertNotZero(first) XASSERT_NotZero(first)
#define testAssertFail(msg) XASSERT_Fail(msg)
#define testAssertProperty(value, property) XASSERT_Property(value, property)
#define testAssertNotNull(p) XASSERT_NotNull(p)
#define testAssertNotEqual(first, second) XASSERT_NotEqual(first, second)
#define testAssertNull(p) XASSERT_Null(p)
