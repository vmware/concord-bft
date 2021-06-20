// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of sub-components with separate copyright
// notices and license terms. Your use of these sub-components is subject to
// the terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "SerializableActiveWindow.hpp"
#include <memory>
#include <cstring>

using namespace std;

namespace bftEngine {
namespace impl {

#define INPUT_PARAMS WindowSize, Resolution, ItemType

template <TEMPLATE_PARAMS>
SerializableActiveWindow<INPUT_PARAMS>::SerializableActiveWindow(const SeqNum &windowFirst) {
  ConcordAssert(windowFirst % Resolution == 0);
  beginningOfActiveWindow_ = windowFirst;
  for (uint32_t i = 0; i < numItems_; i++) activeWindow_[i].reset();
}

template <TEMPLATE_PARAMS>
SerializableActiveWindow<INPUT_PARAMS>::~SerializableActiveWindow() {
  for (uint32_t i = 0; i < numItems_; i++) activeWindow_[i].reset();
}

template <TEMPLATE_PARAMS>
bool SerializableActiveWindow<INPUT_PARAMS>::equals(const SerializableActiveWindow &other) const {
  if (numItems_ != other.numItems_) return false;
  for (uint32_t i = 0; i < numItems_; i++) {
    if (!(activeWindow_[i].equals(other.activeWindow_[i]))) return false;
  }
  return true;
}

template <TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::deserializeElement(const SeqNum &index,
                                                                char *buf,
                                                                const size_t &bufLen,
                                                                uint32_t &actualSize) {
  actualSize = 0;
  ConcordAssert(insideActiveWindow(beginningOfActiveWindow_ + index));
  activeWindow_[index].reset();
  activeWindow_[index] = ItemType::deserialize(buf, bufLen, actualSize);
  ConcordAssert(actualSize != 0);
}

template <TEMPLATE_PARAMS>
bool SerializableActiveWindow<INPUT_PARAMS>::insideActiveWindow(const SeqNum &num) const {
  return insideActiveWindow(num, beginningOfActiveWindow_);
}

template <TEMPLATE_PARAMS>
bool SerializableActiveWindow<INPUT_PARAMS>::insideActiveWindow(const SeqNum &num,
                                                                const SeqNum &beginningOfActiveWindow) {
  return ((num >= beginningOfActiveWindow) && (num < (beginningOfActiveWindow + WindowSize)));
}

template <TEMPLATE_PARAMS>
SeqNum SerializableActiveWindow<INPUT_PARAMS>::convertIndex(const SeqNum &seqNum) {
  return convertIndex(seqNum, beginningOfActiveWindow_);
}

template <TEMPLATE_PARAMS>
SeqNum SerializableActiveWindow<INPUT_PARAMS>::convertIndex(const SeqNum &seqNum,
                                                            const SeqNum &beginningOfActiveWindow) {
  ConcordAssert(seqNum % Resolution == 0);
  ConcordAssert(insideActiveWindow(seqNum, beginningOfActiveWindow));
  SeqNum converted = (seqNum / Resolution) % numItems_;
  return converted;
}

template <TEMPLATE_PARAMS>
ItemType &SerializableActiveWindow<INPUT_PARAMS>::get(const SeqNum &seqNum) {
  return activeWindow_[convertIndex(seqNum)];
}

template <TEMPLATE_PARAMS>
ItemType &SerializableActiveWindow<INPUT_PARAMS>::getByRealIndex(const SeqNum &index) {
  return activeWindow_[index];
}

template <TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::resetAll(const SeqNum &windowFirst) {
  ConcordAssert(windowFirst % Resolution == 0);
  for (uint32_t i = 0; i < numItems_; i++) activeWindow_[i].reset();
  beginningOfActiveWindow_ = windowFirst;
}

template <TEMPLATE_PARAMS>
list<SeqNum> SerializableActiveWindow<INPUT_PARAMS>::advanceActiveWindow(const SeqNum &newFirstIndex) {
  ConcordAssert(newFirstIndex % Resolution == 0);
  ConcordAssert(newFirstIndex >= beginningOfActiveWindow_);

  list<SeqNum> cleanedItems;
  if (newFirstIndex == beginningOfActiveWindow_) return cleanedItems;

  if (newFirstIndex - beginningOfActiveWindow_ >= WindowSize) {
    resetAll(newFirstIndex);
    for (SeqNum i = 0; i < numItems_; i++) cleanedItems.push_back(i);
    return cleanedItems;
  }
  const uint16_t inactiveBegin = ((beginningOfActiveWindow_ / Resolution) % numItems_);
  const uint16_t activeBegin = ((newFirstIndex / Resolution) % numItems_);
  const uint16_t inactiveEnd = ((activeBegin > 0) ? (activeBegin - 1) : (numItems_ - 1));
  const uint16_t resetSize = (inactiveBegin <= inactiveEnd) ? (inactiveEnd - inactiveBegin + 1)
                                                            : (inactiveEnd + 1 + numItems_ - inactiveBegin);
  ConcordAssert(resetSize > 0 && resetSize < numItems_);
  uint16_t debugNumOfReset = 0;
  for (SeqNum i = inactiveBegin; i != activeBegin; (i = ((i + 1) % numItems_))) {
    activeWindow_[i].reset();
    debugNumOfReset++;
    cleanedItems.push_back(i);
  }
  ConcordAssert(debugNumOfReset == resetSize);
  beginningOfActiveWindow_ = newFirstIndex;
  return cleanedItems;
}

}  // namespace impl
}  // namespace bftEngine
