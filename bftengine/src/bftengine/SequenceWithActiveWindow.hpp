// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these sub-components is subject to
// the terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <stdint.h>
#include <utility>
#include <type_traits>
#include <atomic>
#include "assertUtils.hpp"

// TODO(GG): ItemFuncs should have operations on ItemType: init, free, reset, save, load

namespace bftEngine {
namespace impl {

template <uint16_t WindowSize,
          uint16_t Resolution,
          typename NumbersType,
          typename ItemType,
          typename ItemFuncs,
          uint16_t WindowHistory = 0,
          bool TypeSelection = true>
class SequenceWithActiveWindow {
  static_assert(WindowSize >= 8, "");
  static_assert(WindowSize < UINT16_MAX, "");
  static_assert(Resolution >= 1, "");
  static_assert(Resolution < WindowSize, "");
  static_assert(WindowSize % Resolution == 0, "");

 protected:
  static constexpr uint16_t numItemsInsideActiveWindow = WindowSize / Resolution;
  static constexpr uint32_t maxNumItemsInsideInactiveWindow = (WindowHistory * numItemsInsideActiveWindow);
  static constexpr uint32_t totalItems = maxNumItemsInsideInactiveWindow + numItemsInsideActiveWindow;

  typename std::conditional<TypeSelection, NumbersType, std::atomic<NumbersType>>::type beginningOfActiveWindow_;
  size_t firstItemOfActiveWindow_;
  size_t numItemsInsideInactiveWindow_;
  ItemType workingWindow_[totalItems];

 public:
  SequenceWithActiveWindow(NumbersType windowFirst, void *initData) {
    ConcordAssert(windowFirst % Resolution == 0);

    beginningOfActiveWindow_ = windowFirst;

    firstItemOfActiveWindow_ = 0;
    numItemsInsideInactiveWindow_ = 0;

    for (uint16_t i = 0; i < totalItems; i++) {
      ItemFuncs::init(workingWindow_[i], initData);
      ItemFuncs::reset(workingWindow_[i]);
    }
  }

  ~SequenceWithActiveWindow() {
    for (uint16_t i = 0; i < totalItems; i++) ItemFuncs::free(workingWindow_[i]);
  }

  bool insideActiveWindow(NumbersType n) const {
    return ((n >= beginningOfActiveWindow_) && (n < (beginningOfActiveWindow_ + WindowSize)));
  }

  bool isPressentInHistory(NumbersType n) const {
    if (numItemsInsideInactiveWindow_ > 0) {
      const auto beginningOfInactiveWindow = getBeginningOfInactiveWindow();
      return ((n >= beginningOfInactiveWindow) && (n < beginningOfActiveWindow_));
    }
    return false;
  }

  NumbersType getBeginningOfInactiveWindow() const {
    ConcordAssertGT(numItemsInsideInactiveWindow_, 0);
    return (beginningOfActiveWindow_ - (numItemsInsideInactiveWindow_ * Resolution));
  }

  ItemType &get(NumbersType n) {
    ConcordAssert(n % Resolution == 0);
    ConcordAssert(insideActiveWindow(n));

    const auto offsetFromActiveWinBegin = (n - beginningOfActiveWindow_) / Resolution;
    const uint16_t i = ((firstItemOfActiveWindow_ + offsetFromActiveWinBegin) % totalItems);
    return workingWindow_[i];
  }

  ItemType &getFromHistory(NumbersType n) {
    ConcordAssert(isPressentInHistory(n));
    ConcordAssert(n % Resolution == 0);
    ConcordAssertGT(numItemsInsideInactiveWindow_, 0);
    ConcordAssertLE(numItemsInsideInactiveWindow_, maxNumItemsInsideInactiveWindow);

    const auto beginningOfInactiveWindow = getBeginningOfInactiveWindow();
    LOG_DEBUG(GL,
              "Getting info from Inactive Window for SeqNo="
                  << n << KVLOG(beginningOfActiveWindow_, beginningOfInactiveWindow));

    const auto offsetFromInactiveWinBegin = (n - beginningOfInactiveWindow) / Resolution;
    const auto i =
        ((firstItemOfActiveWindow_ - numItemsInsideInactiveWindow_ + totalItems + offsetFromInactiveWinBegin) %
         totalItems);
    return workingWindow_[i];
  }

  ItemType &getFromActiveWindowOrHistory(NumbersType n) {
    ConcordAssert(insideActiveWindow(n) || isPressentInHistory(n));
    if (insideActiveWindow(n)) {
      return get(n);
    } else {
      return getFromHistory(n);
    }
  }

  std::pair<NumbersType, NumbersType> currentActiveWindow() const {
    std::pair<NumbersType, NumbersType> win;
    win.first = beginningOfActiveWindow_;
    win.second = beginningOfActiveWindow_ + WindowSize - 1;
    return win;
  }

  void resetAll(NumbersType windowFirst) {
    ConcordAssert(windowFirst % Resolution == 0);

    for (uint16_t i = 0; i < totalItems; i++) ItemFuncs::reset(workingWindow_[i]);

    beginningOfActiveWindow_ = windowFirst;
    numItemsInsideInactiveWindow_ = 0;
  }

  void advanceActiveWindow(NumbersType newFirstNumOfActiveWindow) {
    ConcordAssert(newFirstNumOfActiveWindow % Resolution == 0);
    ConcordAssert(newFirstNumOfActiveWindow >= beginningOfActiveWindow_);

    if (newFirstNumOfActiveWindow == beginningOfActiveWindow_) return;

    if (newFirstNumOfActiveWindow - beginningOfActiveWindow_ > WindowSize) {
      resetAll(newFirstNumOfActiveWindow);
      return;
    }

    const uint16_t numItemsToAdvanceActiveWindowWith =
        (newFirstNumOfActiveWindow - beginningOfActiveWindow_) / Resolution;

    const uint16_t oldActiveEnd = (firstItemOfActiveWindow_ + numItemsInsideActiveWindow) % totalItems;

    const uint16_t newActiveBegin = (firstItemOfActiveWindow_ + numItemsToAdvanceActiveWindowWith) % totalItems;

    const uint16_t resetSize = numItemsToAdvanceActiveWindowWith;

    ConcordAssert(resetSize > 0 && resetSize <= numItemsInsideActiveWindow);

    // We need to reset all Items starting from (oldActiveEnd) to (oldActiveEnd + numItemsToAdvanceActiveWindowWith),
    // because this is the amount of Items that will be included in the Active Window after scrolling it.
    uint16_t resetCount = 0;
    for (uint16_t i = oldActiveEnd; resetCount < resetSize; resetCount++, (i = ((i + 1) % totalItems))) {
      ItemFuncs::reset(workingWindow_[i]);
    }

    firstItemOfActiveWindow_ = newActiveBegin;
    beginningOfActiveWindow_ = newFirstNumOfActiveWindow;
    numItemsInsideInactiveWindow_ += numItemsToAdvanceActiveWindowWith;
    if (numItemsInsideInactiveWindow_ > maxNumItemsInsideInactiveWindow) {
      numItemsInsideInactiveWindow_ = maxNumItemsInsideInactiveWindow;
    }
  }

  // TODO(GG): add save & load
};

}  // namespace impl
}  // namespace bftEngine
