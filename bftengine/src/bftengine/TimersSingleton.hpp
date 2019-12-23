// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "Timers.hpp"
#include <mutex>
#include <memory>

namespace bftEngine {

// Singleton wrapper class for Timers.
class TimersSingleton {
 public:
  static std::unique_ptr<concordUtil::Timers>& getInstance() {
    static std::once_flag created_;
    std::call_once(created_, &TimersSingleton::create);
    return timers_;
  }

  TimersSingleton(const TimersSingleton&) = delete;
  TimersSingleton& operator=(const TimersSingleton&) = delete;

 private:
  TimersSingleton() = default;
  ~TimersSingleton() = default;

  static void create() { timers_ = std::make_unique<concordUtil::Timers>(); }

 private:
  static std::unique_ptr<concordUtil::Timers> timers_;
};

}  // namespace bftEngine
