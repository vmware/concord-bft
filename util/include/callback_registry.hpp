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
#include <optional>
#include <stdexcept>
#include <utility>

namespace concord::util {

// A move-only opaque callback handle. Supports callback invocation.
template <typename Iterator>
class GenericCallbackHandle {
 public:
  GenericCallbackHandle(const GenericCallbackHandle&) = delete;
  GenericCallbackHandle& operator=(const GenericCallbackHandle&) = delete;

  // Move-constructs a handle. Throws an exception if 'other' is invalid.
  GenericCallbackHandle(GenericCallbackHandle&& other) { *this = std::move(other); }

  // Move-assigns a handle. Throws an exception if 'other' is invalid.
  GenericCallbackHandle& operator=(GenericCallbackHandle&& other) {
    // If moving to self, treat as a no-op.
    if (this != &other) {
      if (other.iter_.has_value()) {
        iter_ = std::move(other.iter_);
        // Make sure we reset() the std::optional as moving from it only moves the contained value and leaves the
        // std::optional itself with a value.
        other.iter_.reset();
      } else {
        throw std::invalid_argument{"Move operation on CallbackHandle called with an invalid handle"};
      }
    }
    return *this;
  }

  // Invokes the callback if the handle is valid. Throws an exception if the handle is invalid.
  template <typename... InvokeArgs>
  void invoke(InvokeArgs&&... args) const {
    if (iter_.has_value()) {
      (*iter_)->operator()(std::forward<InvokeArgs>(args)...);
    } else {
      throw std::logic_error{"invoke() called on an invalid CallbackHandle"};
    }
  }

  bool operator==(const GenericCallbackHandle& other) const {
    return (iter_.has_value() && other.iter_.has_value() && iter_ == other.iter_);
  }

  bool operator!=(const GenericCallbackHandle& other) const { return !(*this == other); }

  bool valid() const { return iter_.has_value(); }

 private:
  GenericCallbackHandle(Iterator iter) : iter_{iter} {};

 private:
  std::optional<Iterator> iter_;

  template <typename...>
  friend class CallbackRegistry;
};

// A callback registry that supports adding, removing and invoking callbacks. Supports arbitrary
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
  // them. Additionally, handles are invalidated when the registry is destructed. Using handles across registries or
  // after their corresponding registry has been destructed causes undefined bahavior.
  template <typename Func>
  CallbackHandle add(Func&& callback) {
    callbacks_.emplace_back(std::forward<Func>(callback));
    auto it = callbacks_.cend();
    return --it;
  }

  // Removes a registered callback. If the passed handle is invalid, an exception is thrown.
  void remove(CallbackHandle handle) {
    if (handle.iter_.has_value()) {
      callbacks_.erase(*handle.iter_);
    } else {
      throw std::invalid_argument{"CallbackRegistry::remove() called with an invalid CallbackHandle"};
    }
  }

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
