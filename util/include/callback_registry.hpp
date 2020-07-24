// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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

#include <cstddef>
#include <list>
#include <functional>
#include <utility>

namespace concord::util {

// A move-only opaque callback handle. Supports callback invocation.
template <typename Iterator>
class GenericCallbackHandle {
 public:
  GenericCallbackHandle(const GenericCallbackHandle&) = delete;
  GenericCallbackHandle& operator=(const GenericCallbackHandle&) = delete;
  GenericCallbackHandle(GenericCallbackHandle&&) = default;
  GenericCallbackHandle& operator=(GenericCallbackHandle&&) = default;

  template <typename... InvokeArgs>
  void invoke(InvokeArgs&&... args) const {
    iter_->operator()(std::forward<InvokeArgs>(args)...);
  }

  bool operator==(const GenericCallbackHandle& other) const { return (iter_ == other.iter_); }
  bool operator!=(const GenericCallbackHandle& other) const { return !(*this == other); }

 private:
  GenericCallbackHandle(Iterator iter) : iter_{iter} {};

 private:
  Iterator iter_;

  template <typename...>
  friend class CallbackRegistry;
};

// A callback registry that supports registration, deregistration and invocation of callbacks. Supports arbitrary
// callback parameters. Callback return type is void.
template <typename... Args>
class CallbackRegistry {
 private:
  // Use an std::list in order to preserve iterators in handles after erase() calls.
  using Callback = std::function<void(Args...)>;
  using CallbackContainer = std::list<Callback>;
  using Iterator = typename CallbackContainer::const_iterator;

 public:
  using CallbackHandle = GenericCallbackHandle<Iterator>;

  CallbackRegistry() = default;
  CallbackRegistry(const CallbackRegistry&) = delete;
  CallbackRegistry& operator=(const CallbackRegistry&) = delete;

  // Registers a callback and returns a handle to it. Handles are only valid for the registry instance that created
  // them. Additionally, handles are invalidated when the registry is destructed.
  template <typename Func>
  CallbackHandle registerCallback(Func&& callback) {
    callbacks_.emplace_back(std::forward<Func>(callback));
    auto it = callbacks_.cend();
    return --it;
  }

  // Deregisters a callback. The corresponding handle is invalidated if this method returns. If the passed handle is
  // invalid, the behavior is undefined.
  void deregisterCallback(CallbackHandle handle) { callbacks_.erase(handle.iter_); }

  // Invokes all callbacks in the registry. Exceptions from callbacks are propagated to callers of this method.
  // Invocation stops at the first exception thrown, without invoking further callbacks.
  template <typename... InvokeArgs>
  void invokeAll(InvokeArgs&&... args) const {
    for (auto& callback : callbacks_) {
      callback(std::forward<InvokeArgs>(args)...);
    }
  }

  bool empty() const { return callbacks_.empty(); }
  std::size_t size() const { return callbacks_.size(); }

 private:
  CallbackContainer callbacks_;
};

}  // namespace concord::util
