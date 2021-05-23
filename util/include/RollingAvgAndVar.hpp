// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <stdint.h>
#include <mutex>

namespace bftEngine {
namespace impl {

// Based on Knuth TAOCP vol 2, 2nd edition, page 216 (see also https://www.johndcook.com/blog/standard_deviation/)
class RollingAvgAndVar {
 public:
  RollingAvgAndVar(bool safe = false) : safe_(safe) { ; }

  void resetUnsafe() {
    k_ = 0;
    prevM_ = 0;
    prevS_ = 0;
    currM_ = 0;
    currS_ = 0;
  }

  void reset() {
    if (safe_) {
      std::lock_guard<std::mutex> lock(mutex_);
      resetUnsafe();
    } else
      resetUnsafe();
  }

  void addUnsafe(double x) {
    k_++;
    if (k_ == 1) {
      prevM_ = currM_ = x;
      prevS_ = 0.0;
    } else {
      currM_ = prevM_ + (x - prevM_) / k_;
      currS_ = prevS_ + (x - prevM_) * (x - currM_);
      prevM_ = currM_;
      prevS_ = currS_;
    }
  }

  void add(double x) {
    if (safe_) {
      std::lock_guard<std::mutex> lock(mutex_);
      addUnsafe(x);
    } else
      addUnsafe(x);
  }

  double avgUnsafe() const { return (k_ > 0) ? currM_ : 0.0; }

  double avg() {
    if (safe_) {
      std::lock_guard<std::mutex> lock(mutex_);
      return avgUnsafe();
    } else
      return avgUnsafe();
  }

  double varUnsafe() const { return ((k_ > 1) ? currS_ / (k_ - 1) : 0.0); }

  double var() {
    if (safe_) {
      std::lock_guard<std::mutex> lock(mutex_);
      return varUnsafe();
    } else
      return varUnsafe();
  }

  int numOfElementsUnsafe() const { return k_; }

  int numOfElements() {
    if (safe_) {
      std::lock_guard<std::mutex> lock(mutex_);
      return numOfElementsUnsafe();
    } else
      return numOfElementsUnsafe();
  }

 private:
  std::mutex mutex_;
  bool safe_;
  int k_{0};
  double prevM_{0};
  double prevS_{0};
  double currM_{0};
  double currS_{0};
};

}  // namespace impl
}  // namespace bftEngine
