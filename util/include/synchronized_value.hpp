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
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace concord::util {

// Syncrhonizes multi-threaded access to a value of type T.
template <typename T>
class SynchronizedValue {
 private:
  using MutexType = std::shared_mutex;
  using ExclusiveLock = std::scoped_lock<MutexType>;
  using SharedLock = std::shared_lock<MutexType>;

 public:
  // Construct the value in-place with the given arguments.
  template <typename... Args>
  SynchronizedValue(Args&&... args) {
    replace(std::forward<Args>(args)...);
  }

  // Replace the value by constructing a new one in-place with the given arguments.
  //
  // Precondition: the calling thread doesn't have any accessors at the time of the call. If it does, the behaviour is
  // undefined.
  template <typename... Args>
  void replace(Args&&... args) {
    auto new_val = std::make_unique<T>(std::forward<Args>(args)...);
    auto lock = ExclusiveLock{mtx_};
    val_ = std::move(new_val);
  }

  // Provides read-write access to the value.
  // Locks the mutex in the constructor and unlocks it in the dtor.
  class Accessor {
   public:
    Accessor(T& val, MutexType& mtx) : val_{val}, lock_{mtx} {}

    T& operator*() const noexcept { return val_; }
    T* operator->() const noexcept { return &val_; }

   private:
    T& val_;
    ExclusiveLock lock_;
  };

  // Provides read access to the value.
  // Locks the mutex in the constructor and unlocks it in the dtor.
  class ConstAccessor {
   public:
    ConstAccessor(const T& val, MutexType& mtx) : val_{val}, lock_{mtx} {}

    const T& operator*() const noexcept { return val_; }
    const T* operator->() const noexcept { return &val_; }

   private:
    const T& val_;
    SharedLock lock_;
  };

  // Create an accessor to the value.
  //
  // Precondition: the calling thread doesn't have any other accessors at the time of the call. If it does, the
  // behaviour is undefined.
  //
  // A single thread can create one accessor only at a time. Other threads will wait until it is destroyed before they
  // can do any operations on the synchronized value.
  Accessor access() { return Accessor{*val_, mtx_}; }
  ConstAccessor constAccess() const { return ConstAccessor{*val_, mtx_}; }

 private:
  std::unique_ptr<T> val_;
  mutable MutexType mtx_;
};

}  // namespace concord::util
