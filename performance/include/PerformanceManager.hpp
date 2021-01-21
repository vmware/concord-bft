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
    slowdownManager_ = std::make_shared<SlowdownManager>(config);
  }
#ifdef USE_SLOWDOWN
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
#else
  // slow down methods
  template <SlowdownPhase T>
  void Delay(concord::kvbc::SetOfKeyValuePairs &kvpairs) {
    if (slowdownManager_) slowdownManager_->Delay<T>(kvpairs);
  }

  template <SlowdownPhase T>
  void Delay() {
    if (slowdownManager_) slowdownManager_->Delay<T>();
  }
#endif

 private:
  std::shared_ptr<SlowdownManager> slowdownManager_ = nullptr;
};

}  // namespace concord::performance