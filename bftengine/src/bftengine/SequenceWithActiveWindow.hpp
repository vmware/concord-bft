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
#include "assertUtils.hpp"

// TODO(GG): ItemFuncs should have operations on ItemType: init, free, reset, save, load

namespace bftEngine {
namespace impl {

template <uint16_t Resolution, typename NumbersType, typename ItemType, typename ItemFuncs>
class InactiveStorage {
 public:
  InactiveStorage(uint16_t maxSize, void *initData, NumbersType windowFirst) {
    if (maxSize > 0) {
      beginningOfInactiveWindow = windowFirst;
      inactiveSavedSeqs.resize(maxSize);
      readPosOfInactiveWindow = elementsInInactiveWindow = writePosOfInactiveWindow = 0;
      for (uint16_t i = 0; i < maxSize; i++) {
        ItemFuncs::init(inactiveSavedSeqs[i], initData);
        ItemFuncs::reset(inactiveSavedSeqs[i]);
      }
    }
  }

  ~InactiveStorage() {
    for (size_t i = 0; i < inactiveSavedSeqs.size(); i++) ItemFuncs::free(inactiveSavedSeqs[i]);
  }

  void clear(NumbersType newBeginningOfInactiveWindow) {
    beginningOfInactiveWindow = newBeginningOfInactiveWindow;
    readPosOfInactiveWindow = elementsInInactiveWindow = writePosOfInactiveWindow = 0;
    for (size_t i = 0; i < inactiveSavedSeqs.size(); i++) {
      ItemFuncs::reset(inactiveSavedSeqs[i]);
    }
  }

  void add(ItemType &item) {
    ConcordAssertGT(inactiveSavedSeqs.size(), 0);
    ItemFuncs::reset(inactiveSavedSeqs[writePosOfInactiveWindow]);
    ItemFuncs::acquire(inactiveSavedSeqs[writePosOfInactiveWindow], item);
    writePosOfInactiveWindow = (writePosOfInactiveWindow + 1) % inactiveSavedSeqs.size();
    if (++elementsInInactiveWindow > inactiveSavedSeqs.size()) {
      elementsInInactiveWindow = inactiveSavedSeqs.size();
      beginningOfInactiveWindow += Resolution;
      readPosOfInactiveWindow = (readPosOfInactiveWindow + 1) % inactiveSavedSeqs.size();
    }
  }

  ItemType &get(NumbersType n) {
    ConcordAssert(n % Resolution == 0);
    ConcordAssertGT(inactiveSavedSeqs.size(), 0);
    auto index = (readPosOfInactiveWindow + (n - beginningOfInactiveWindow) / Resolution) % inactiveSavedSeqs.size();
    LOG_DEBUG(GL,
              "Actual get from Inactive Window" << KVLOG(index,
                                                         readPosOfInactiveWindow,
                                                         writePosOfInactiveWindow,
                                                         n,
                                                         beginningOfInactiveWindow,
                                                         inactiveSavedSeqs.size()));
    return inactiveSavedSeqs[index];
  }

  const NumbersType &getBeginningOfInactiveWindow() const {
    ConcordAssertGT(inactiveSavedSeqs.size(), 0);
    return beginningOfInactiveWindow;
  }

  const NumbersType size() const { return elementsInInactiveWindow * Resolution; }

 private:
  std::vector<ItemType> inactiveSavedSeqs;
  size_t writePosOfInactiveWindow;
  size_t readPosOfInactiveWindow;
  size_t elementsInInactiveWindow;
  NumbersType beginningOfInactiveWindow;
};

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
  static const uint16_t numItems = WindowSize / Resolution;

  typename std::conditional<TypeSelection, NumbersType, std::atomic<NumbersType>>::type beginningOfActiveWindow;
  ItemType activeWindow[numItems];
  InactiveStorage<Resolution, NumbersType, ItemType, ItemFuncs> inactiveStorage;

 public:
  SequenceWithActiveWindow(NumbersType windowFirst, void *initData)
      : inactiveStorage(WindowHistory * numItems, initData, windowFirst) {
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

  bool isPressentInHistory(NumbersType n) const {
    auto BeginningOfInactiveWindow = inactiveStorage.getBeginningOfInactiveWindow();
    return ((n >= BeginningOfInactiveWindow) && (n < (BeginningOfInactiveWindow + inactiveStorage.size())));
  }

  ItemType &get(NumbersType n) {
    ConcordAssert(n % Resolution == 0);
    ConcordAssert(insideActiveWindow(n));

    uint16_t i = ((n / Resolution) % numItems);
    return activeWindow[i];
  }

  ItemType &getFromHistory(NumbersType n) {
    ConcordAssert(isPressentInHistory(n));
    LOG_INFO(GL,
             "Getting info from Inactive Window for SeqNo="
                 << n << KVLOG(beginningOfActiveWindow, inactiveStorage.getBeginningOfInactiveWindow()));
    return inactiveStorage.get(n);
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
      if (WindowHistory > 0) {
        inactiveStorage.clear(newFirstIndexOfActiveWindow);  // clear elements from inactiveStorage to stay in sync
      }
      resetAll(newFirstIndexOfActiveWindow);
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
