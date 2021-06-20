// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <stdint.h>
#include <chrono>

#include "assertUtils.hpp"

namespace concord::util {
/**
 * A Throughput object is used to calculate the number of items processed in a time unit.
 * After construction, it must be started by calling start(). In order to get meaningful statistics, user should
 * report periodically to the object on the processing progress by calling report().
 *
 * If the user supplies a window_size > 0:
 * 1) User may call all member functions prefixed with getPrevWin*.
 * 2) Last window throughput is calculated and saved.
 * 3) Overall and last window calculations are based on the window's end time.
 * 4) report() returns true when the window's end reached.
 *
 * To get overall and/or last window statistics, the user has 2 options:
 * 1) If window_size > 0, it should waits until report() returns true and then it may call getOverallResults()
 * and/or getPrevWinResults().
 * 2) If window_size is 0, user can call at any time for getOverallResults(). Calling report() to continue collecting
 * statistics is still possible after.
 */
class Throughput {
 public:
  Throughput(uint32_t window_size = 0ul) : num_reports_per_window_(window_size) {}
  Throughput() = delete;

  // Reset all statistics and record starting time
  void start();

  // Report amount of items processed since last report.
  // If window_size > 0: returns true if reached the end of a summary window, and started a new window
  bool report(uint64_t items_processed = 1);

  struct Results {
    uint64_t elapsed_time_ms_;
    uint64_t throughput_ = 0ull;  // items per sec
    uint64_t num_processed_items_ = 0ull;
  };

  // Get overall Results: total number of items processed, and throughput from time elapsed_time_ms_
  const Results& getOverallResults();

  // Get previous window's results. Can be called only if report() returned true.
  const Results& getPrevWinResults() const;

  // Get previous window's index. Can be called only if report() returned true.
  uint64_t getPrevWinIndex() const;

 protected:
  class Stats {
    std::chrono::time_point<std::chrono::steady_clock> start_time_;

   public:
    Results results_;

    void reset();
    void calcThroughput();
  };

  const uint32_t num_reports_per_window_;
  bool started_ = false;
  bool prev_win_calculated_ = false;
  Stats overall_stats_;
  Stats current_window_stats_;
  Stats previous_window_stats_;
  uint64_t previous_window_index_;
  uint64_t reports_counter_ = 0;
};  // class Throughput

}  // namespace concord::util
