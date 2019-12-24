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
  static concordUtil::Timers& getInstance() {
    static concordUtil::Timers timers_;
    return timers_;
  }

  TimersSingleton(const TimersSingleton&) = delete;
  TimersSingleton& operator=(const TimersSingleton&) = delete;

 private:
  TimersSingleton() = default;
  ~TimersSingleton() = default;
};

}  // namespace bftEngine
