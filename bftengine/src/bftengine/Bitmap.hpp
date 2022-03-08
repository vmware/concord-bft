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

#include "PrimitiveTypes.hpp"
#include "assertUtils.hpp"

#include <vector>
#include <cstring>

namespace bftEngine {
namespace impl {
class Bitmap {
 public:
  Bitmap() : numBits_(0), p_(nullptr) {}

  Bitmap(uint32_t numBits) : numBits_(numBits), p_(nullptr) {
    if (numBits_ > 0) {
      p_ = (unsigned char*)std::malloc(realSize());
      zeroAll();
    }
  }

  // create Bitmap from buffer
  Bitmap(char* buffer, uint32_t bufferLength, uint32_t* actualSize) : numBits_(0), p_(nullptr) {
    if (actualSize) *actualSize = 0;
    if (bufferLength < sizeof(uint32_t)) return;
    uint32_t* pNumOfBits = (uint32_t*)buffer;
    const uint32_t numOfBitmapBytes = realSize(*pNumOfBits);
    const uint32_t sizeNeeded = sizeof(uint32_t) + numOfBitmapBytes;
    if (bufferLength < sizeNeeded) return;
    if (*pNumOfBits > 0) {
      char* pBitmap = buffer + sizeof(uint32_t);
      numBits_ = *pNumOfBits;
      p_ = (unsigned char*)std::malloc(numOfBitmapBytes);
      std::memcpy(p_, pBitmap, numOfBitmapBytes);
    }
    if (actualSize) *actualSize = sizeNeeded;
  }

  Bitmap(const Bitmap& other) : numBits_(other.numBits_), p_(nullptr) {
    if (numBits_ == 0) {
      p_ = nullptr;
    } else {
      p_ = (unsigned char*)std::malloc(realSize());
      std::memcpy(p_, other.p_, realSize());
    }
  }

  Bitmap(Bitmap&& other) : numBits_(other.numBits_), p_(other.p_) {
    if (numBits_ > 0) {
      ConcordAssert(p_ != nullptr);
    }

    other.p_ = nullptr;
    other.numBits_ = 0;
  }

  ~Bitmap() {
    if (numBits_ > 0) {
      ConcordAssert(p_ != nullptr);
      std::free(p_);
    }
  }

  bool isEmpty() const { return (numBits_ == 0); }

  bool equals(const Bitmap& other) const { return (other.numBits_ == numBits_ && !memcmp(other.p_, p_, realSize())); }

  Bitmap& operator=(const Bitmap& other) {
    if (&other == this) {
      return *this;
    }
    if (numBits_ > 0) {
      ConcordAssert(p_ != nullptr);
      std::free(p_);
    }

    numBits_ = other.numBits_;
    if (numBits_ == 0) {
      p_ = nullptr;
    } else {
      p_ = (unsigned char*)std::malloc(realSize());
      std::memcpy(p_, other.p_, realSize());
    }

    return *this;
  }

  Bitmap& operator=(Bitmap&& other) {
    if (numBits_ > 0) {
      ConcordAssert(p_ != nullptr);
      std::free(p_);
    }

    numBits_ = other.numBits_;
    p_ = other.p_;

    if (numBits_ > 0) {
      ConcordAssert(p_ != nullptr);
    }

    other.p_ = nullptr;
    other.numBits_ = 0;

    return *this;
  }

  void zeroAll() {
    if (p_ == nullptr) return;
    const uint32_t s = realSize();
    ConcordAssert(s > 0);
    memset((void*)p_, 0, s);
  }

  uint32_t numOfBits() const { return numBits_; }

  bool get(uint32_t i) const {
    ConcordAssert(i < numBits_);
    const uint32_t byteIndex = i / 8;
    const unsigned char byteMask = (1 << (i % 8));
    return ((p_[byteIndex] & byteMask) != 0);
  }

  void set(uint32_t i) {
    ConcordAssert(i < numBits_);
    const uint32_t byteIndex = i / 8;
    const unsigned char byteMask = (1 << (i % 8));
    p_[byteIndex] = p_[byteIndex] | byteMask;
  }

  void reset(uint32_t i) {
    ConcordAssert(i < numBits_);
    const uint32_t byteIndex = i / 8;
    const unsigned char byteMask = ((unsigned char)0xFF) & ~(1 << (i % 8));
    p_[byteIndex] = p_[byteIndex] & byteMask;
  }

  Bitmap& operator+=(const Bitmap& other) {
    ConcordAssert(numOfBits() == other.numOfBits());
    for (size_t i = 0; i < other.numOfBits(); i++) {
      if (other.get(i)) set(i);
    }
    return *this;
  }
  void writeToBuffer(char* buffer, uint32_t bufferLength, uint32_t* actualSize) const {
    const uint32_t sizeNeeded = sizeNeededInBuffer();
    ConcordAssert(bufferLength >= sizeNeeded);
    uint32_t* pNumOfBits = (uint32_t*)buffer;
    char* pBitmap = buffer + sizeof(uint32_t);
    *pNumOfBits = numBits_;
    memcpy(pBitmap, p_, realSize());
    if (actualSize) *actualSize = sizeNeeded;
  }

  uint32_t sizeNeededInBuffer() const { return (sizeof(numBits_) + realSize(numBits_)); }

  static uint32_t maxSizeNeededToStoreInBuffer(uint32_t maxNumOfBits) {
    return (sizeof(uint32_t) + realSize(maxNumOfBits));
  }

 protected:
  uint32_t numBits_;
  unsigned char* p_;

  uint32_t realSize() const { return realSize(numBits_); }
  static uint32_t realSize(uint32_t nbits) { return ((nbits + 7) / 8); }
};
}  // namespace impl
}  // namespace bftEngine
