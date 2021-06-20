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
#include <list>
#include "assertUtils.hpp"
#include "PrimitiveTypes.hpp"

namespace bftEngine {
namespace impl {

#define TEMPLATE_PARAMS uint16_t WindowSize, uint16_t Resolution, typename ItemType

template <TEMPLATE_PARAMS>
class SerializableActiveWindow {
 public:
  static_assert(WindowSize >= 8, "");
  static_assert(WindowSize < UINT16_MAX, "");
  static_assert(Resolution >= 1, "");
  static_assert(Resolution < WindowSize, "");
  static_assert(WindowSize % Resolution == 0, "");

  explicit SerializableActiveWindow(const SeqNum &windowFirst);
  ~SerializableActiveWindow();

  static uint32_t maxElementSize() { return ItemType::maxSize(); }

  SeqNum getBeginningOfActiveWindow() const { return beginningOfActiveWindow_; }

  bool equals(const SerializableActiveWindow &other) const;

  void deserializeElement(const SeqNum &index, char *buf, const size_t &bufLen, uint32_t &actualSize);

  bool insideActiveWindow(const SeqNum &num) const;

  static bool insideActiveWindow(const SeqNum &num, const SeqNum &newFirstIndex);

  ItemType &get(const SeqNum &num);

  ItemType &getByRealIndex(const SeqNum &index);

  SeqNum convertIndex(const SeqNum &seqNum);

  void resetAll(const SeqNum &windowFirst);

  std::list<SeqNum> advanceActiveWindow(const SeqNum &newFirstIndex);

  static SeqNum convertIndex(const SeqNum &seqNum, const SeqNum &beginningOfActiveWindow);

 private:
  static const uint32_t numItems_ = WindowSize / Resolution;

 private:
  SeqNum beginningOfActiveWindow_;
  ItemType activeWindow_[numItems_];
};

}  // namespace impl
}  // namespace bftEngine
