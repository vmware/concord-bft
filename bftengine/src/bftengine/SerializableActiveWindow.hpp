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

  SerializableActiveWindow(SeqNum windowFirst);
  ~SerializableActiveWindow();

  uint32_t getNumItems() const { return numItems_; }

  static uint32_t maxElementSize() {
    return ItemType::maxSize();
  }

  static uint32_t simpleParamsSize() {
    return sizeof(beginningOfActiveWindow_);
  }

  static uint32_t maxSize() {
    return (simpleParamsSize() + numItems_ * maxElementSize());
  }

  SeqNum convertIndex(const SeqNum &seqNum) const;

  bool equals(const SerializableActiveWindow &other) const;

  void serializeSimpleParams(char *buf, size_t bufLen) const;

  void serializeElement(uint16_t index, char *buf, size_t bufLen,
                        size_t &actualSize) const;

  void deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize);

  void deserializeElement(uint16_t index, char *buf, size_t bufLen,
                          uint32_t &actualSize);

  bool insideActiveWindow(uint16_t num) const;

  ItemType &get(uint16_t num);

  std::pair<uint16_t, uint16_t> currentActiveWindow() const;

  void resetAll(SeqNum windowFirst);

  void advanceActiveWindow(uint32_t newFirstIndexOfActiveWindow);

 protected:
  static const uint32_t numItems_ = WindowSize / Resolution;

  SeqNum beginningOfActiveWindow_;
  ItemType activeWindow_[numItems_];
};

}
}
