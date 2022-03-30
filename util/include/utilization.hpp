// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <vector>
#include "assertUtils.hpp"

namespace concordUtil {
// returns from an interval of time, the percentage that the client was performing operations.
// the intervals should be monotonically increasing
// i.e. for an interval its end time should be bigger or equal than the start, and
// For consecutive intervals the start of the following interval should be greater or equal to the end
// of the preceding
class utilization {
 public:
  struct durtionMicro {
    std::uint64_t start;
    std::uint64_t end;
  };

  void addDuration(durtionMicro&& dur) {
    ConcordAssertGE(dur.end, dur.start);
    if (dur.end == dur.start) return;
    if (utilization_.size() > 0) {
      ConcordAssertGE(dur.start, utilization_.back().end);
    }
    utilization_.push_back(std::move(dur));
  }

  // Calculate the total time and then subtracts the intervals between the operations.
  std::uint64_t getUtilization() const {
    if (utilization_.size() == 0) return 0;
    const auto total_time = utilization_.back().end - utilization_.front().start;
    auto busy_time = total_time;
    for (auto it = utilization_.cbegin(); (it + 1) < utilization_.cend(); it++) {
      auto dead_interval = (it + 1)->start - it->end;
      ConcordAssertGE(busy_time, dead_interval);
      busy_time -= dead_interval;
    }

    return (busy_time * 100) / total_time;
  }

  void restart() { utilization_.clear(); }

 private:
  std::vector<durtionMicro> utilization_;
};

}  // namespace concordUtil
