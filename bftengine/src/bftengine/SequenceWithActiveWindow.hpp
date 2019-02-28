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

#include <stdint.h>
#include <utility>
#include "assertUtils.hpp"

// TODO(GG): ItemFuncs should have operations on ItemType: init, free, reset,
// save, load

namespace bftEngine {
namespace impl {

template <uint16_t WindowSize,
          uint16_t Resolution,
          typename NumbersType,
          typename ItemType,
          typename ItemFuncs>
class SequenceWithActiveWindow {
  static_assert(WindowSize >= 8, "");
  static_assert(WindowSize < UINT16_MAX, "");
  static_assert(Resolution >= 1, "");
  static_assert(Resolution < WindowSize, "");
  static_assert(WindowSize % Resolution == 0, "");

 protected:
  static const uint16_t numItems = WindowSize / Resolution;

  NumbersType beginningOfActiveWindow;
  ItemType activeWindow[numItems];

 public:
  SequenceWithActiveWindow(NumbersType windowFirst, void* initData) {
    Assert(windowFirst % Resolution == 0);

    beginningOfActiveWindow = windowFirst;

    for (uint16_t i = 0; i < numItems; i++) {
      ItemFuncs::init(activeWindow[i], initData);
      ItemFuncs::reset(activeWindow[i]);
    }
  }

  ~SequenceWithActiveWindow() {
    for (uint16_t i = 0; i < numItems; i++) ItemFuncs::free(activeWindow[i]);
  }

  bool insideActiveWindow(NumbersType n) const {
    return ((n >= beginningOfActiveWindow) &&
            (n < (beginningOfActiveWindow + WindowSize)));
  }

  ItemType& get(NumbersType n) {
    Assert(n % Resolution == 0);
    Assert(insideActiveWindow(n));

    uint16_t i = ((n / Resolution) % numItems);

    return activeWindow[i];
  }

  std::pair<NumbersType, NumbersType> currentActiveWindow() const {
    std::pair<NumbersType, NumbersType> win;
    win.first = beginningOfActiveWindow;
    win.second = beginningOfActiveWindow + WindowSize - 1;
    return win;
  }

  void resetAll(NumbersType windowFirst) {
    Assert(windowFirst % Resolution == 0);

    for (uint16_t i = 0; i < numItems; i++) ItemFuncs::reset(activeWindow[i]);

    beginningOfActiveWindow = windowFirst;
  }

  void advanceActiveWindow(NumbersType newFirstIndexOfActiveWindow) {
    Assert(newFirstIndexOfActiveWindow % Resolution == 0);
    Assert(newFirstIndexOfActiveWindow >= beginningOfActiveWindow);

    if (newFirstIndexOfActiveWindow == beginningOfActiveWindow) return;

    if (newFirstIndexOfActiveWindow - beginningOfActiveWindow >= WindowSize) {
      resetAll(newFirstIndexOfActiveWindow);
      return;
    }

    const uint16_t inactiveBegin =
        ((beginningOfActiveWindow / Resolution) % numItems);

    const uint16_t activeBegin =
        ((newFirstIndexOfActiveWindow / Resolution) % numItems);

    const uint16_t inactiveEnd =
        ((activeBegin > 0) ? (activeBegin - 1) : (numItems - 1));

    const uint16_t resetSize =
        (inactiveBegin <= inactiveEnd)
            ? (inactiveEnd - inactiveBegin + 1)
            : (inactiveEnd + 1 + numItems - inactiveBegin);
    Assert(resetSize > 0 && resetSize < numItems);

    uint16_t debugNumOfReset = 0;

    for (uint16_t i = inactiveBegin; i != activeBegin;
         (i = ((i + 1) % numItems))) {
      ItemFuncs::reset(activeWindow[i]);
      debugNumOfReset++;
    }

    Assert(debugNumOfReset == resetSize);

    beginningOfActiveWindow = newFirstIndexOfActiveWindow;
  }

  // TODO(GG): add save & load
};

}  // namespace impl
}  // namespace bftEngine
