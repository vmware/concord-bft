// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include "SlowdownManager.hpp"

namespace concord::performance {

class PerformanceManager {
 public:
  PerformanceManager() = default;
  explicit PerformanceManager(std::shared_ptr<SlowdownConfiguration> &config) {
    if (!slowdownManager_) slowdownManager_ = std::make_shared<SlowdownManager>(config);
  }
  template <typename T>
  struct type {};
  typedef type<SlowdownManager> slowdown;

  bool enabled(slowdown) { return slowdownManager_ && slowdownManager_->isEnabled(); }

  template <typename T>
  bool isEnabled() {
    auto t = type<T>{};
    return enabled(t);
  }

  // slow down methods
  template <SlowdownPhase T>
  SlowDownResult Delay(concord::kvbc::SetOfKeyValuePairs &kvpairs) {
    if (slowdownManager_) return slowdownManager_->Delay<T>(kvpairs);
    return SlowDownResult{};
  }

  template <SlowdownPhase T>
  SlowDownResult Delay() {
    if (slowdownManager_) return slowdownManager_->Delay<T>();
    return SlowDownResult{};
  }

  template <SlowdownPhase T>
  SlowDownResult Delay(char *msg, size_t &&size, std::function<void(char *, size_t &)> &&f) {
    if (slowdownManager_)
      return slowdownManager_->Delay<T>(msg, size, std::forward<std::function<void(char *, size_t &)>>(f));
    return SlowDownResult{};
  }

 private:
  std::shared_ptr<SlowdownManager> slowdownManager_ = nullptr;
};

}  // namespace concord::performance
