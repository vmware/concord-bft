// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <chrono>
#include "utilization.hpp"

namespace concord::performance {
class ISystemResourceEntity {
 public:
  enum class type {
    pruning_avg_time_micro,
    pruning_utilization,
    post_execution_utilization,
    post_execution_avg_time_micro,
    add_blocks_accumulated,
    transactions_accumulated
  };
  struct measurement {
    ISystemResourceEntity::type type;
    std::uint64_t count{0};
    std::uint64_t start{0};
    std::uint64_t end{0};
  };

  struct scopedDurMeasurment {
    scopedDurMeasurment(ISystemResourceEntity& e, type t, bool commit = true) : entity(e), commit(commit) {
      if (commit) {
        m.type = t;
        m.start =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count();
      }
    }
    ~scopedDurMeasurment() {
      if (commit) {
        m.end =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count();

        entity.addMeasurement(m);
      }
    }
    ISystemResourceEntity& entity;
    measurement m;
    bool commit{true};
  };

  virtual ~ISystemResourceEntity() = default;
  // Implementation returns how much more pruned blocks can this entity support.
  // It also expected that current availability might be negative, due to a
  // resource being overburden with operations.
  virtual int64_t getAvailableResources() const = 0;
  // Measurements of handled operations. Can be transaction, prune blocks, etc.
  virtual uint64_t getMeasurement(const type) = 0;
  // Name of the resource for metrics purposes.
  virtual const std::string getResourceName() const = 0;
  // Restarts the measurement calculation.
  virtual void reset() = 0;
  // add a measurement to some measurement type
  virtual void addMeasurement(const measurement&) = 0;
  // stop/start adding and calculating measurements
  virtual void stop() = 0;
  virtual void start() = 0;
};
}  // namespace concord::performance
