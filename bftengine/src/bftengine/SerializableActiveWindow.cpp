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

using namespace std;

namespace bftEngine {
namespace impl {

#define INPUT_PARAMS WindowSize, Resolution, ItemType

template<TEMPLATE_PARAMS>
SerializableActiveWindow<INPUT_PARAMS>::SerializableActiveWindow(SeqNum windowFirst) {
  Assert(windowFirst % Resolution == 0);
  beginningOfActiveWindow_ = windowFirst;
  for (uint32_t i = 0; i < numItems_; i++)
    activeWindow_[i].reset();
}

template<TEMPLATE_PARAMS>
SerializableActiveWindow<INPUT_PARAMS>::~SerializableActiveWindow() {
  for (uint32_t i = 0; i < numItems_; i++)
    activeWindow_[i].reset();
}

template<TEMPLATE_PARAMS>
bool SerializableActiveWindow<INPUT_PARAMS>::equals(const SerializableActiveWindow &other) const {
  if (numItems_ != other.numItems_)
    return false;
  for (uint32_t i = 0; i < numItems_; i++) {
    if (!(activeWindow_[i].equals(other.activeWindow_[i])))
      return false;
  }
  return true;
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::serializeSimpleParams(char *buf, size_t bufLen) const {
  Assert(bufLen >= simpleParamsSize());
  size_t beginningOfActiveWindowSize = sizeof(beginningOfActiveWindow_);
  memcpy(buf, &beginningOfActiveWindow_, beginningOfActiveWindowSize);
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::serializeElement(
    uint16_t index, char *buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(insideActiveWindow(beginningOfActiveWindow_+ index));
  Assert(bufLen >= maxElementSize());
  activeWindow_[index].serialize(buf, maxElementSize(), actualSize);
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize) {
  Assert(bufLen >= simpleParamsSize());
  size_t beginningOfActiveWindowSize = sizeof(beginningOfActiveWindow_);
  memcpy(&beginningOfActiveWindow_, buf, beginningOfActiveWindowSize);
  actualSize = simpleParamsSize();
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::deserializeElement(
    uint16_t index, char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  Assert(insideActiveWindow(beginningOfActiveWindow_+ index));
  activeWindow_[index] = ItemType::deserialize(buf, bufLen, actualSize);
  Assert(actualSize != 0);
}

template<TEMPLATE_PARAMS>
bool SerializableActiveWindow<INPUT_PARAMS>::insideActiveWindow(uint16_t num) const {
  return ((num >= beginningOfActiveWindow_) && (num < (beginningOfActiveWindow_ + WindowSize)));
}

template<TEMPLATE_PARAMS>
SeqNum SerializableActiveWindow<INPUT_PARAMS>::convertIndex(const SeqNum &seqNum) const {
  Assert(seqNum % Resolution == 0);
  Assert(insideActiveWindow(seqNum));
  return (seqNum / Resolution) % numItems_;
}

template<TEMPLATE_PARAMS>
ItemType &SerializableActiveWindow<INPUT_PARAMS>::get(uint16_t num) {
  return activeWindow_[convertIndex(num)];
}

template<TEMPLATE_PARAMS>
std::pair<uint16_t, uint16_t> SerializableActiveWindow<INPUT_PARAMS>::currentActiveWindow() const {
  std::pair<uint16_t, uint16_t> win;
  win.first = beginningOfActiveWindow_;
  win.second = beginningOfActiveWindow_ + WindowSize - 1;
  return win;
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::resetAll(SeqNum windowFirst) {
  Assert(windowFirst % Resolution == 0);
  for (uint32_t i = 0; i < numItems_; i++)
    activeWindow_[i].reset();
  beginningOfActiveWindow_ = windowFirst;
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::advanceActiveWindow(uint32_t newFirstIndexOfActiveWindow) {
  Assert(newFirstIndexOfActiveWindow % Resolution == 0);
  Assert(newFirstIndexOfActiveWindow >= beginningOfActiveWindow_);
  if (newFirstIndexOfActiveWindow == beginningOfActiveWindow_)
    return;
  if (newFirstIndexOfActiveWindow - beginningOfActiveWindow_ >= WindowSize) {
    resetAll(newFirstIndexOfActiveWindow);
    return;
  }
  const uint16_t inactiveBegin = ((beginningOfActiveWindow_ / Resolution) % numItems_);
  const uint16_t activeBegin = ((newFirstIndexOfActiveWindow / Resolution) % numItems_);
  const uint16_t inactiveEnd = ((activeBegin > 0) ? (activeBegin - 1) : (numItems_ - 1));
  const uint16_t resetSize = (inactiveBegin <= inactiveEnd) ? (inactiveEnd - inactiveBegin + 1) :
                             (inactiveEnd + 1 + numItems_ - inactiveBegin);
  Assert(resetSize > 0 && resetSize < numItems_);
  uint16_t debugNumOfReset = 0;
  for (uint32_t i = inactiveBegin; i != activeBegin; (i = ((i + 1) % numItems_))) {
    activeWindow_[i].reset();
    debugNumOfReset++;
  }
  Assert(debugNumOfReset == resetSize);
  beginningOfActiveWindow_ = newFirstIndexOfActiveWindow;
}
};

}
