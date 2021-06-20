// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license,
// as noted in the LICENSE file.

#pragma once

#include <thread>
#include <chrono>
#include <functional>
#include <atomic>
namespace concord::util {
class PeriodicCall {
  std::thread t_;
  uint32_t intervalMilli_;
  std::atomic_bool active_ = false;
  std::function<void()> fun_;

 public:
  PeriodicCall(const std::function<void()>& fun, uint32_t intervalMilli = 100)
      : intervalMilli_(intervalMilli), active_(true), fun_(fun) {
    t_ = std::thread([this] {
      while (active_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMilli_));
        if (!active_) break;
        fun_();
      }
    });
  }
  ~PeriodicCall() {
    active_ = false;
    t_.join();
  }
};
}  // namespace concord::util