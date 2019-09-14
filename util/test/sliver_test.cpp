// Copyright 2018 VMware, all rights reserved
/**
 * Test the Sliver class.
 *
 * While these tests check that Sliver "works", the most interesting test is to
 * run this under valgrind, and make sure that it doesn't find any
 * leaks/double-frees/etc. `valgrind --leak-check=full test/SliverTests`
 */

#include "sliver.hpp"
#include "gtest/gtest.h"
#include <iostream>

using namespace std;
using concordUtils::Sliver;

namespace {

/**
 * Allocate a `length`-byte buffer, and fill it with the sequence
 * 0,1,2,3...255,0,1,2...
 *
 * Remember: the caller must deallocate this buffer.
 */
char* new_test_memory(size_t length) {
  char* buffer = new char[length];

  for (size_t i = 0; i < length; i++) {
    buffer[i] = i % 256;
  }

  return buffer;
}

bool is_match(const char* expected, const size_t expected_length,
              const Sliver& actual) {
  if (expected_length != actual.length()) {
    return false;
  }
  for (size_t i = 0; i < expected_length; i++) {
    if (expected[i] != actual[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Test that a sliver wraps a char pointer exactly.
 */
TEST(sliver_test, simple_wrap) {
  const size_t test_size = 100;
  char* expected = new_test_memory(test_size);

  Sliver actual(expected, test_size);
  ASSERT_TRUE(is_match(expected, test_size, actual));
}

/**
 * Test that a sliver wraps a copy of a char pointer exactly.
 */
TEST(sliver_test, simple_copy) {
  const size_t test_size = 100;
  char* expected = new_test_memory(test_size);

  auto actual = Sliver::copy(expected, test_size);
  ASSERT_TRUE(is_match(expected, test_size, actual));
  ASSERT_NE(expected, actual.data());
}

/**
 * Test that re-wrapping works.
 */
TEST(sliver_test, simple_rewrap) {
  const size_t test_size = 101;
  char* expected = new_test_memory(test_size);

  Sliver first = Sliver(expected, test_size);
  Sliver actual1(first, 0, first.length());
  Sliver actual2 = first.subsliver(0, first.length());

  ASSERT_TRUE(is_match(expected, test_size, actual1));
  ASSERT_TRUE(is_match(expected, test_size, actual2));
}

/**
 * Test that base offsetting works.
 */
TEST(sliver_test, offsets) {
  const size_t test_size = 102;
  char* expected = new_test_memory(test_size);

  Sliver base(expected, test_size);
  for (size_t offset = 1; offset < test_size; offset += 5) {
    Sliver subsliver(base, offset, test_size - offset);
    ASSERT_TRUE(is_match(expected + offset, test_size - offset, subsliver));
  }
}

/**
 * Test that base length limiting works.
 */
TEST(sliver_test, lengths) {
  const size_t test_size = 103;
  char* expected = new_test_memory(test_size);

  Sliver base(expected, test_size);
  const size_t step = 7;
  for (size_t length = test_size - 1; length > step; length -= step) {
    Sliver subsliver(base, 0, length);
    ASSERT_TRUE(is_match(expected, length, subsliver));
  }
}

/**
 * Test that nested offsetting and lengths works.
 */
TEST(sliver_test, nested) {
  const size_t test_size = 104;
  char* expected = new_test_memory(test_size);

  Sliver base(expected, test_size);
  const size_t offset_step = 3;
  const size_t length_step = 4;
  for (size_t offset = offset_step, length = test_size - (offset_step + length_step);
       length > (offset_step + length_step);
       offset += offset_step, length -= (length_step + offset_step)) {
    Sliver subsliver(base, offset_step, base.length() - (offset_step + length_step));
    ASSERT_TRUE(is_match(expected + offset, length, subsliver)) << " Expected length: " << length << std::endl
                                                                << "   Actual length: " << subsliver.length();
    base = subsliver;
  }
}

/**
 * Create a base sliver, and then return a subsliver. Used to test that the
 * shared pointer is handled properly.
 */
Sliver copied_subsliver(size_t base_size, size_t sub_offset,
                        size_t sub_length) {
  char* data = new_test_memory(base_size);
  Sliver base(data, base_size);
  Sliver sub(base, sub_offset, sub_length);
  return sub;
}

/**
 * Test that copying out of creation scope works.
 *
 * This test may be non-interesting, but this is a thing some of our functions
 * do, so I want to test it explicitly.
 */
TEST(sliver_test, copying) {
  const size_t test_size = 105;
  char* expected = new_test_memory(test_size);

  const size_t test_offset1 = 20;
  const size_t test_length1 = 30;
  Sliver actual1 = copied_subsliver(test_size, test_offset1, test_length1);

  const size_t test_offset2 = 50;
  const size_t test_length2 = 20;
  Sliver actual2 = copied_subsliver(test_size, test_offset2, test_length2);

  ASSERT_TRUE(is_match(expected + test_offset1, test_length1, actual1));
  ASSERT_TRUE(is_match(expected + test_offset2, test_length2, actual2));

  // we didn't wrap the expected buffer this time, so we need to free it
  // ourselves, instead of letting the Sliver do it
  delete[] expected;
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
