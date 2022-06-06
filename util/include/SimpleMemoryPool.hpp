// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
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

#include <stdint.h>
#include <functional>
#include <deque>
#include <memory>

#include "assertUtils.hpp"

namespace concord::util {

/**
 * This is a template class that implements a simple memory pool for a single type of element.
 * A common use case for this class is the pre-allocation of N elements at the start. This is done in order to:
 * 1) Make sure the only possible allocation failures in the module can happen on boot (startup).
 * 2) Avoid allocation/deallocation of memory. Specifically, for small memory chunks, in the long term this can cause
 * performance issues and memory defragmentation.
 * Currently, the class does not implements pool of elements of pointer or array type.
 */

template <typename T>
class SimpleMemoryPool {
  static_assert(!std::is_array<T>::value, "Array type not supported!");
  static_assert(!std::is_pointer<T>::value, "Pointer type not supported!");

 public:
  using ElementPtr = std::shared_ptr<T>;
  SimpleMemoryPool(size_t maxNumElements,
                   std::function<void(ElementPtr&)> allocCallback = nullptr,
                   std::function<void(ElementPtr&)> freeCallback = nullptr,
                   std::function<void()> ctorCallback = nullptr)
      : maxNumElements_(maxNumElements), allocCallback_{allocCallback}, freeCallback_(freeCallback) {
    if (maxNumElements == 0) throw std::invalid_argument("maxNumElements cannnot be 0!");
    if (ctorCallback) ctorCallback();
    for (size_t i{0}; i < maxNumElements_; ++i) {
      auto element = std::make_shared<T>();
      freeQ_.push_back(element);
    }
  }

  size_t numFreeElements() const { return freeQ_.size(); }
  size_t numAllocatedElements() const { return allocatedQ_.size(); }
  bool empty() { return freeQ_.empty(); };
  bool full() { return allocatedQ_.empty(); };
  size_t maxElements() { return maxNumElements_; };

  ElementPtr alloc() {
    if (freeQ_.empty()) {
      throw std::runtime_error("No more free elements!");
    }
    auto ret = freeQ_.front();
    freeQ_.pop_front();
    allocatedQ_.push_back(ret);
    if (allocCallback_) {
      allocCallback_(ret);
    }
    return ret;
  }

  // Comment: elements can be free in any order
  void free(ElementPtr& element) {
    if (freeQ_.size() == maxNumElements_) {
      throw std::runtime_error("All elements have been already returned!");
    }
    auto it = std::find(allocatedQ_.begin(), allocatedQ_.end(), element);
    if (it == allocatedQ_.end()) {
      throw std::runtime_error("Trying to free unrecognized element (element was not allocated by this pool)!");
    }

    if (freeCallback_) {
      freeCallback_(*it);
    }
    allocatedQ_.erase(it);
    freeQ_.push_back(std::move(element));
  }

 private:
  const size_t maxNumElements_;
  std::deque<ElementPtr> freeQ_;
  std::deque<ElementPtr> allocatedQ_;
  std::function<void(ElementPtr&)> allocCallback_;
  std::function<void(ElementPtr&)> freeCallback_;
};  // SimpleMemoryPool
}  // namespace concord::util
