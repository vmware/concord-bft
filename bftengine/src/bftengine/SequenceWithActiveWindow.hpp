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
#include "assertUtils.hpp"

// TODO(GG): ItemFuncs should have operations on ItemType: init, free, reset, save, load

namespace bftEngine {
namespace impl {

template <uint16_t Resolution, typename NumbersType, typename ItemType, typename ItemFuncs>
class InactiveStorage {
 public:
  InactiveStorage(uint16_t maxSize, void *initData) {
    if (maxSize > 0) {
      filled = false;
      inactiveSavedSeqs.resize(maxSize);
      elementsInInactiveWindow = currentPosOfInactiveWindow = 0;
      for (uint16_t i = 0; i < maxSize; i++) {
        ItemFuncs::init(inactiveSavedSeqs[i], initData);
        ItemFuncs::reset(inactiveSavedSeqs[i]);
      }
    }
  }

  ~InactiveStorage() {
    for (uint16_t i = 0; i < inactiveSavedSeqs.size(); i++) ItemFuncs::free(inactiveSavedSeqs[i]);
  }

  void add(ItemType &item) {
    ConcordAssertGT(inactiveSavedSeqs.size(), 0);
    ItemFuncs::reset(inactiveSavedSeqs[currentPosOfInactiveWindow]);
    ItemFuncs::acquire(inactiveSavedSeqs[currentPosOfInactiveWindow], item);
    currentPosOfInactiveWindow = (currentPosOfInactiveWindow + 1) % inactiveSavedSeqs.size();
    if (filled || ++elementsInInactiveWindow > inactiveSavedSeqs.size()) {
      filled = true;
      beginningOfInactiveWindow += Resolution;
    }
  }

  ItemType &get(NumbersType n) {
    ConcordAssert(n % Resolution == 0);
    ConcordAssertGT(inactiveSavedSeqs.size(), 0);
    auto index = (currentPosOfInactiveWindow + (n - beginningOfInactiveWindow) / Resolution) % inactiveSavedSeqs.size();
    LOG_DEBUG(GL,
              "Actual get from Inactive Window"
                  << KVLOG(index, currentPosOfInactiveWindow, n, beginningOfInactiveWindow, inactiveSavedSeqs.size()));
    return inactiveSavedSeqs[index];
  }

  const NumbersType &getBeginningOfInactiveWindow() const {
    ConcordAssertGT(inactiveSavedSeqs.size(), 0);
    return beginningOfInactiveWindow;
  }

 private:
  std::vector<ItemType> inactiveSavedSeqs;
  size_t currentPosOfInactiveWindow;
  size_t elementsInInactiveWindow;
  bool filled;
  NumbersType beginningOfInactiveWindow;
};

template <uint16_t WindowSize,
          uint16_t Resolution,
          typename NumbersType,
          typename ItemType,
          typename ItemFuncs,
          uint16_t WindowHistory = 0>
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
  InactiveStorage<Resolution, NumbersType, ItemType, ItemFuncs> inactiveStorage;

 public:
  SequenceWithActiveWindow(NumbersType windowFirst, void *initData)
      : inactiveStorage(WindowHistory * numItems, initData) {
    ConcordAssert(windowFirst % Resolution == 0);

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
    return ((n >= beginningOfActiveWindow) && (n < (beginningOfActiveWindow + WindowSize)));
  }

  bool IsPressentInHistory(NumbersType n) const {
    return ((n >= inactiveStorage.getBeginningOfInactiveWindow()) && (n < (beginningOfActiveWindow + WindowSize)));
  }

  ItemType &get(NumbersType n) {
    ConcordAssert(n % Resolution == 0);
    ConcordAssert(insideActiveWindow(n));

    uint16_t i = ((n / Resolution) % numItems);
    return activeWindow[i];
  }

  ItemType &getFromHistory(NumbersType n) {
    if (insideActiveWindow(n)) {
      return get(n);
    } else {
      LOG_DEBUG(GL,
                "Getting info from Inactive Window for SeqNo="
                    << n << KVLOG(beginningOfActiveWindow, inactiveStorage.getBeginningOfInactiveWindow()));
      return inactiveStorage.get(n);
    }
  }

  std::pair<NumbersType, NumbersType> currentActiveWindow() const {
    std::pair<NumbersType, NumbersType> win;
    win.first = beginningOfActiveWindow;
    win.second = beginningOfActiveWindow + WindowSize - 1;
    return win;
  }

  void resetAll(NumbersType windowFirst) {
    ConcordAssert(windowFirst % Resolution == 0);

    for (uint16_t i = 0; i < numItems; i++) ItemFuncs::reset(activeWindow[i]);

    beginningOfActiveWindow = windowFirst;
  }

  void advanceActiveWindow(NumbersType newFirstIndexOfActiveWindow) {
    ConcordAssert(newFirstIndexOfActiveWindow % Resolution == 0);
    ConcordAssert(newFirstIndexOfActiveWindow >= beginningOfActiveWindow);

    if (newFirstIndexOfActiveWindow == beginningOfActiveWindow) return;

    if (newFirstIndexOfActiveWindow - beginningOfActiveWindow >= WindowSize) {
      resetAll(newFirstIndexOfActiveWindow);
      if (WindowHistory > 0) {
        for (NumbersType n = 0; n > newFirstIndexOfActiveWindow - beginningOfActiveWindow; n++) {
          inactiveStorage.add(activeWindow[0]);  // clear necessary elements from inactiveStorage to stay in sync
        }
      }
      return;
    }

    const uint16_t inactiveBegin = ((beginningOfActiveWindow / Resolution) % numItems);

    const uint16_t activeBegin = ((newFirstIndexOfActiveWindow / Resolution) % numItems);

    const uint16_t inactiveEnd = ((activeBegin > 0) ? (activeBegin - 1) : (numItems - 1));

    const uint16_t resetSize = (inactiveBegin <= inactiveEnd) ? (inactiveEnd - inactiveBegin + 1)
                                                              : (inactiveEnd + 1 + numItems - inactiveBegin);
    ConcordAssert(resetSize > 0 && resetSize < numItems);

    uint16_t debugNumOfReset = 0;
    for (uint16_t i = inactiveBegin; i != activeBegin; (i = ((i + 1) % numItems))) {
      if (WindowHistory > 0) {
        inactiveStorage.add(activeWindow[i]);
      }

      ItemFuncs::reset(activeWindow[i]);
      debugNumOfReset++;
    }

    ConcordAssert(debugNumOfReset == resetSize);
    beginningOfActiveWindow = newFirstIndexOfActiveWindow;
  }

  // TODO(GG): add save & load
};

}  // namespace impl
}  // namespace bftEngine
