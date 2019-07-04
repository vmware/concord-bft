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

template<TEMPLATE_PARAMS>
SerializableActiveWindow<INPUT_PARAMS>::SerializableActiveWindow(const SeqNum &windowFirst) {
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
void SerializableActiveWindow<INPUT_PARAMS>::serializeActiveWindowBeginning(char *buf) const {
  memcpy(buf, &beginningOfActiveWindow_, sizeof(beginningOfActiveWindow_));
}

template<TEMPLATE_PARAMS>
SeqNum SerializableActiveWindow<INPUT_PARAMS>::deserializeActiveWindowBeginning(char *buf) {
  SeqNum beginningOfActiveWindow = 0;
  memcpy(&beginningOfActiveWindow, buf, sizeof(beginningOfActiveWindow));
  return beginningOfActiveWindow;
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::deserializeBeginningOfActiveWindow(char *buf) {
  beginningOfActiveWindow_ = deserializeActiveWindowBeginning(buf);
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::deserializeElement(const uint16_t &index, char *buf, const size_t &bufLen,
                                                                uint32_t &actualSize) {
  actualSize = 0;
  Assert(insideActiveWindow(beginningOfActiveWindow_ + index));
  activeWindow_[index].reset();
  activeWindow_[index] = ItemType::deserialize(buf, bufLen, actualSize);
  Assert(actualSize != 0);
}

template<TEMPLATE_PARAMS>
bool SerializableActiveWindow<INPUT_PARAMS>::insideActiveWindow(const uint16_t &num) const {
  return insideActiveWindow(num, beginningOfActiveWindow_);
}

template<TEMPLATE_PARAMS>
bool SerializableActiveWindow<INPUT_PARAMS>::insideActiveWindow(const uint16_t &num,
                                                                const SeqNum &beginningOfActiveWindow) {
  return ((num >= beginningOfActiveWindow) && (num < (beginningOfActiveWindow + WindowSize)));
}

template<TEMPLATE_PARAMS>
SeqNum SerializableActiveWindow<INPUT_PARAMS>::convertIndex(const SeqNum &seqNum) {
  return convertIndex(seqNum, beginningOfActiveWindow_);
}

template<TEMPLATE_PARAMS>
SeqNum SerializableActiveWindow<INPUT_PARAMS>::convertIndex(const SeqNum &seqNum,
                                                            const SeqNum &beginningOfActiveWindow) {
  Assert(seqNum % Resolution == 0);
  Assert(insideActiveWindow(seqNum, beginningOfActiveWindow));
  SeqNum converted = (seqNum / Resolution) % numItems_;
  return converted;
}

template<TEMPLATE_PARAMS>
ItemType &SerializableActiveWindow<INPUT_PARAMS>::get(const uint16_t &seqNum) {
  return activeWindow_[convertIndex(seqNum)];
}

template<TEMPLATE_PARAMS>
ItemType &SerializableActiveWindow<INPUT_PARAMS>::getByRealIndex(const uint16_t &index) {
  return activeWindow_[index];
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::resetAll(const SeqNum &windowFirst) {
  Assert(windowFirst % Resolution == 0);
  for (uint32_t i = 0; i < numItems_; i++)
    activeWindow_[i].reset();
  beginningOfActiveWindow_ = windowFirst;
}

template<TEMPLATE_PARAMS>
void SerializableActiveWindow<INPUT_PARAMS>::advanceActiveWindow(const uint32_t &newFirstIndexOfActiveWindow) {
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

}
}
