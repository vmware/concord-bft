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

#include <fstream>
#include <string>
#include "ReplicaConfig.hpp"

namespace concord {
namespace util {

class FakeClock {
 public:
  FakeClock() = default;
  ~FakeClock() = default;
  FakeClock(const FakeClock&) = delete;

  static std::chrono::system_clock::time_point now() {
    // read the file for this replica id and return time based on the command present in file
    auto& config = ReplicaConfig::instance();
    std::string path = "/tmp/simulated_clock_" + std::to_string(config.replicaId) + ".config";
    std::fstream file_reader(path, std::ios_base::in);
    int clock_drift = 0;
    file_reader >> clock_drift;
    return std::chrono::system_clock::now() + std::chrono::milliseconds(clock_drift);
  }
};

}  // namespace util
}  // namespace concord