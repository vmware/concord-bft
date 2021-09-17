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
  using ExclusiveLock = std::unique_lock<MutexType>;
  using SharedLock = std::shared_lock<MutexType>;

 public:
  // Provides read-write access to the value.
  // Locks the mutex in the constructor and unlocks it in the dtor.
  class Accessor {
   public:
    Accessor(T& val, ExclusiveLock&& lock) : val_{val}, lock_{std::move(lock)} {}

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
    ConstAccessor(const T& val, SharedLock&& lock) : val_{val}, lock_{std::move(lock)} {}

    const T& operator*() const noexcept { return val_; }
    const T* operator->() const noexcept { return &val_; }

   private:
    const T& val_;
    SharedLock lock_;
  };

 public:
  // Construct the value in-place with the given arguments.
  template <typename... Args>
  SynchronizedValue(Args&&... args) : val_{std::make_unique<T>(std::forward<Args>(args)...)} {}

  // Replace the value by constructing a new one in-place with the given arguments.
  //
  // Precondition: the calling thread doesn't have any accessors at the time of the call. If it does, the behaviour is
  // undefined.
  template <typename... Args>
  Accessor replace(Args&&... args) {
    auto new_val = std::make_unique<T>(std::forward<Args>(args)...);
    auto lock = ExclusiveLock{mtx_};
    val_ = std::move(new_val);
    return Accessor{*val_, std::move(lock)};
  }

  // Create an accessor to the value.
  //
  // Precondition: the calling thread doesn't have any other accessors at the time of the call. If it does, the
  // behaviour is undefined.
  //
  // A single thread can create one accessor only at a time. Other threads will wait until it is destroyed before they
  // can do any operations on the synchronized value.
  Accessor access() {
    auto lock = ExclusiveLock{mtx_};
    return Accessor{*val_, std::move(lock)};
  }
  ConstAccessor constAccess() const {
    auto lock = SharedLock{mtx_};
    return ConstAccessor{*val_, std::move(lock)};
  }

 private:
  std::unique_ptr<T> val_;
  mutable MutexType mtx_;
};

}  // namespace concord::util
