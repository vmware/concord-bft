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

#pragma once

#include <stdint.h>
#include <utility>
#include "assertUtils.hpp"
#include "PrimitiveTypes.hpp"

namespace bftEngine {
namespace impl {

#define TEMPLATE_PARAMS uint16_t WindowSize, uint16_t Resolution, typename ItemType

template<TEMPLATE_PARAMS>
class SerializableActiveWindow {
 public:
  static_assert(WindowSize >= 8, "");
  static_assert(WindowSize < UINT16_MAX, "");
  static_assert(Resolution >= 1, "");
  static_assert(Resolution < WindowSize, "");
  static_assert(WindowSize % Resolution == 0, "");

  explicit SerializableActiveWindow(SeqNum windowFirst);
  ~SerializableActiveWindow();

  static uint32_t maxElementSize() {
    return ItemType::maxSize();
  }

  static uint32_t simpleParamsSize() {
    return sizeof(beginningOfActiveWindow_);
  }

  SeqNum getBeginningOfActiveWindow() const { return beginningOfActiveWindow_; }

  bool equals(const SerializableActiveWindow &other) const;

  void serializeActiveWindowBeginning(char *buf) const;

  void deserializeBeginningOfActiveWindow(char *buf);

  static SeqNum deserializeActiveWindowBeginning(char *buf);

  void deserializeElement(uint16_t index, char *buf, size_t bufLen, uint32_t &actualSize);

  bool insideActiveWindow(uint16_t num) const;

  static bool insideActiveWindow(uint16_t num, const SeqNum& beginningOfActiveWindow);

  ItemType &get(uint16_t num) ;

  ItemType &getByRealIndex(uint16_t index);

  std::pair<SeqNum, SeqNum> currentActiveWindow() const;

  void resetAll(SeqNum windowFirst);

  void advanceActiveWindow(uint32_t newFirstIndexOfActiveWindow);

  SeqNum convertIndex(const SeqNum &seqNum);

  static SeqNum convertIndex(const SeqNum &seqNum, const SeqNum& beginningOfActiveWindow);

 private:
  static const uint32_t numItems_ = WindowSize / Resolution;

 private:
  SeqNum beginningOfActiveWindow_;
  ItemType activeWindow_[numItems_];
};

}
}
