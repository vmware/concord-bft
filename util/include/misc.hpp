// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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

#include <chrono>

inline uint64_t get_monotonic_time() {
  std::chrono::steady_clock::time_point curTimePoint = std::chrono::steady_clock::now();

  auto timeSinceEpoch = curTimePoint.time_since_epoch();
  uint64_t micro = std::chrono::duration_cast<std::chrono::microseconds>(timeSinceEpoch).count();

  return micro;
}
