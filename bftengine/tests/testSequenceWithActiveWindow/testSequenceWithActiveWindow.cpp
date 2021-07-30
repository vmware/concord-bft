// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include "SequenceWithActiveWindow.hpp"

using namespace std;
using namespace bftEngine::impl;

class MockItemFuncs {
 public:
  static const uint64_t initialized = std::numeric_limits<uint64_t>::max();
  static const uint64_t restarted = std::numeric_limits<uint64_t>::max() - 1;
  static const uint64_t freed = std::numeric_limits<uint64_t>::max() - 2;

  // methods for SequenceWithActiveWindow
  static void init(uint64_t& i, void* d) { i = initialized; }

  static void free(uint64_t& i) { i = freed; }

  static void reset(uint64_t& i) { i = restarted; }

  template <typename T>
  static void printActiveWindow(T window) {
    std::cout << "current active window = " << window.currentActiveWindow().first << ":"
              << window.currentActiveWindow().second << "\n";
  }
};

void basic_tests_no_inactive_window(unsigned int initialAdvance) {
  const int windowSize = 12;
  const int resolution = 2;
  uint64_t beginning = initialAdvance * (windowSize / 2);
  SequenceWithActiveWindow<windowSize, resolution, uint64_t, uint64_t, MockItemFuncs> windowOfInts(0, nullptr);

  auto initial_start = beginning;
  windowOfInts.advanceActiveWindow(beginning);

  // Set consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(MockItemFuncs::restarted == windowOfInts.get(i));
    ConcordAssert(!windowOfInts.isPressentInHistory(i));
    windowOfInts.get(i) = i;
  }
  // Verify consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(i == windowOfInts.get(i));
  }

  // MockItemFuncs::printActiveWindow(windowOfInts);

  // Advance Active Window by half
  beginning += windowSize / 2;
  windowOfInts.advanceActiveWindow(beginning);

  // MockItemFuncs::printActiveWindow(windowOfInts);

  // Verify half of the values remain in the Active Window
  for (uint64_t i = beginning; i < beginning + windowSize / 2; i += resolution) {
    ConcordAssert(i == windowOfInts.get(i));
  }

  // Verify other half of the Active Window is freshly reset
  for (uint64_t i = beginning + windowSize / 2; i < beginning + windowSize; i += resolution) {
    ConcordAssert(MockItemFuncs::restarted == windowOfInts.get(i));
  }

  // Verify history is empty
  for (uint64_t i = initial_start; i < initial_start + windowSize; i += resolution) {
    ConcordAssert(!windowOfInts.isPressentInHistory(i));
  }
}

TEST(testSequenceWithActiveWindow_test, basic_tests_no_inactive_window_no_initial_offset) {
  basic_tests_no_inactive_window(0);
}

TEST(testSequenceWithActiveWindow_test, basic_tests_no_inactive_window_with_half_window_initial_offset) {
  basic_tests_no_inactive_window(1);
}

TEST(testSequenceWithActiveWindow_test, basic_tests_no_inactive_window_with_window_and_half_initial_offset) {
  basic_tests_no_inactive_window(3);
}

void basic_tests_no_inactive_window_scroll_full_window_size(unsigned int numToScroll) {
  const int windowSize = 30;
  const int resolution = 5;
  uint64_t beginning = windowSize / 2;
  SequenceWithActiveWindow<windowSize, resolution, uint64_t, uint64_t, MockItemFuncs> windowOfInts(0, nullptr);

  auto advance = windowSize * numToScroll;
  auto initial_start = beginning;
  windowOfInts.advanceActiveWindow(beginning);

  // Set consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(MockItemFuncs::restarted == windowOfInts.get(i));
    ConcordAssert(!windowOfInts.isPressentInHistory(i));
    windowOfInts.get(i) = i;
  }
  // Verify consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(i == windowOfInts.get(i));
  }

  // MockItemFuncs::printActiveWindow(windowOfInts);

  // Advance Active Window
  beginning += advance;
  windowOfInts.advanceActiveWindow(beginning);

  // MockItemFuncs::printActiveWindow(windowOfInts);

  // Verify the Working Window is freshly reset
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(MockItemFuncs::restarted == windowOfInts.get(i));
  }

  // Verify history is empty
  for (uint64_t i = initial_start; i < initial_start + advance; i += resolution) {
    ConcordAssert(!windowOfInts.isPressentInHistory(i));
  }

  // Set consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(MockItemFuncs::restarted == windowOfInts.get(i));
    ConcordAssert(!windowOfInts.isPressentInHistory(i));
    windowOfInts.get(i) = i;
  }
  // Verify consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(i == windowOfInts.get(i));
  }

  // Advance Active Window by half
  beginning += windowSize / 2;
  windowOfInts.advanceActiveWindow(beginning);

  // Verify half of the values remain in the Active Window
  for (uint64_t i = beginning; i < beginning + windowSize / 2; i += resolution) {
    ConcordAssert(i == windowOfInts.get(i));
  }

  // Verify other half of the Active Window is freshly reset and
  // then set consecutive values in it
  for (uint64_t i = beginning + windowSize / 2; i < beginning + windowSize; i += resolution) {
    ConcordAssert(MockItemFuncs::restarted == windowOfInts.get(i));
    ConcordAssert(!windowOfInts.isPressentInHistory(i));
    windowOfInts.get(i) = i;
  }

  // Verify consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(i == windowOfInts.get(i));
  }
}

TEST(testSequenceWithActiveWindow_test, basic_tests_no_inactive_window_scrow_full_window_size) {
  basic_tests_no_inactive_window_scroll_full_window_size(1);
}

TEST(testSequenceWithActiveWindow_test, basic_tests_no_inactive_window_scrow_twice_window_size) {
  basic_tests_no_inactive_window_scroll_full_window_size(2);
}

void basic_tests_with_inactive_window(bool initialAdvance) {
  const int windowSize = 10;
  const int resolution = 1;
  uint64_t beginning = initialAdvance ? 5 * windowSize : 0;
  SequenceWithActiveWindow<windowSize, resolution, uint64_t, uint64_t, MockItemFuncs, 1> windowOfInts(0, nullptr);

  auto initial_start = beginning;
  windowOfInts.advanceActiveWindow(beginning);

  // Set consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(MockItemFuncs::restarted == windowOfInts.get(i));
    ConcordAssert(!windowOfInts.isPressentInHistory(i));
    windowOfInts.get(i) = i;
  }
  // Verify consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    ConcordAssert(i == windowOfInts.get(i));
  }

  // MockItemFuncs::printActiveWindow(windowOfInts);

  // Advance Active Window by half
  beginning += windowSize / 2;
  windowOfInts.advanceActiveWindow(beginning);

  // MockItemFuncs::printActiveWindow(windowOfInts);

  // Verify half of the values remain in the Active Window
  for (uint64_t i = beginning; i < beginning + windowSize / 2; i += resolution) {
    ConcordAssert(i == windowOfInts.get(i));
  }

  // Verify other half of the Active Window is freshly reset
  for (uint64_t i = beginning + windowSize / 2; i < beginning + windowSize; i += resolution) {
    ConcordAssert(MockItemFuncs::restarted == windowOfInts.get(i));
  }

  // Verify previously second half of Active Window, and now first is not present in history
  for (uint64_t i = initial_start + windowSize / 2; i < initial_start + windowSize; i += resolution) {
    ConcordAssert(!windowOfInts.isPressentInHistory(i));
  }

  // std::cout << "beginningOfInactiveWindow = " << windowOfInts.inactiveStorage.beginningOfInactiveWindow << "\n";
  // std::cout << "elementsInInactiveWindow = " << windowOfInts.inactiveStorage.elementsInInactiveWindow << "\n";
  // std::cout << "currentPosOfInactiveWindow = " << windowOfInts.inactiveStorage.currentPosOfInactiveWindow << "\n";

  // Verify previously first half of Active Window, is present in history
  for (uint64_t i = initial_start; i < initial_start + windowSize / 2; i += resolution) {
    ConcordAssert(windowOfInts.isPressentInHistory(i));
    ConcordAssert(i == windowOfInts.getFromHistory(i));
  }
}

TEST(testSequenceWithActiveWindow_test, basic_tests_with_inactive_window_no_initial_offset) {
  basic_tests_with_inactive_window(false);
}

TEST(testSequenceWithActiveWindow_test, basic_tests_with_inactive_window_with_initial_offset) {
  basic_tests_with_inactive_window(true);
}

TEST(testSequenceWithActiveWindow_test, move_working_window_7_times) {
  const int windowSize = 20;
  const int resolution = 2;
  uint64_t beginning = 0;
  SequenceWithActiveWindow<windowSize, resolution, uint64_t, uint64_t, MockItemFuncs, 1> windowOfInts(beginning,
                                                                                                      nullptr);

  windowOfInts.advanceActiveWindow(beginning);

  // Set consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    windowOfInts.get(i) = i;
  }
  // Advance Working Window and set consecutive values accordingly to transfer in history
  for (int i = 0; i < 7; i++) {
    beginning += windowSize / 2;
    windowOfInts.advanceActiveWindow(beginning);
    for (uint64_t i = beginning + windowSize / 2; i < beginning + windowSize; i += resolution) {
      windowOfInts.get(i) = i;
    }
  }
  // Verify values for current and previous Working Windows are pressent and correct
  for (uint64_t i = beginning - windowSize; i < beginning + windowSize; i += resolution) {
    ConcordAssert(i == windowOfInts.getFromActiveWindowOrHistory(i));
  }
}

TEST(testSequenceWithActiveWindow_test, move_working_window_by_its_full_size_after_previous_fill) {
  const int windowSize = 30;
  const int resolution = 1;
  uint64_t beginning = 0;
  SequenceWithActiveWindow<windowSize, resolution, uint64_t, uint64_t, MockItemFuncs, 1> windowOfInts(beginning,
                                                                                                      nullptr);

  windowOfInts.advanceActiveWindow(beginning);

  // Set consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    windowOfInts.get(i) = i;
  }

  // Advance Working Window and set consecutive values accordingly to transfer in history
  for (int i = 0; i < 3; i++) {
    beginning += windowSize / 2;
    windowOfInts.advanceActiveWindow(beginning);
    for (uint64_t i = beginning + windowSize / 2; i < beginning + windowSize; i += resolution) {
      windowOfInts.get(i) = i;
    }
  }

  // Verify values for current and previous Working Windows are pressent and correct
  for (uint64_t i = beginning - windowSize; i < beginning + windowSize; i += resolution) {
    ConcordAssert(i == windowOfInts.getFromActiveWindowOrHistory(i));
  }

  // Advance Working Window by its full size
  beginning += windowSize;
  windowOfInts.advanceActiveWindow(beginning);

  // Verify all values from previous Active Window are transferred to history (Inactive Window)
  for (uint64_t i = beginning - windowSize; i < beginning; i += resolution) {
    ConcordAssert(i == windowOfInts.getFromHistory(i));
  }

  // Set consecutive values in Active Window
  for (uint64_t i = beginning; i < beginning + windowSize; i += resolution) {
    windowOfInts.get(i) = i;
  }

  // Advance Working Window and set consecutive values accordingly to transfer in history
  for (int i = 0; i < 5; i++) {
    beginning += windowSize / 2;
    windowOfInts.advanceActiveWindow(beginning);
    for (uint64_t i = beginning + windowSize / 2; i < beginning + windowSize; i += resolution) {
      windowOfInts.get(i) = i;
    }
  }

  // Verify values for current and previous Working Windows are pressent and correct
  for (uint64_t i = beginning - windowSize; i < beginning + windowSize; i += resolution) {
    ConcordAssert(i == windowOfInts.getFromActiveWindowOrHistory(i));
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();
  return res;
}
