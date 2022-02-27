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
#include "Logger.hpp"

// uncomment the next line to add debug prints
// #define DO_DEBUG_TRP
#ifdef DO_DEBUG_TRP
#define DEBUG_TRP_PRINT(x, y) LOG_DEBUG(x, y)
#else
#define DEBUG_TRP_PRINT(x, y)
#endif

namespace concord::util {

/** A Duration Tracker allows to track the sum of multiple none-continues time intervals (durations).
 * This is done by calling start() / stop() multiple times.
 * If the last call to the tracker is start(): call totalDuration() to get the sum of intervals, including current
 * still running interval.
 * If the last call to a tracker is stop(): caller may get the sum of intervals from stop() returned
 * value or call totalDuration() explicitly.
 * To reset (re-use) the tracker, call start/stop with do_reset=true.
 */
template <typename T>
class DurationTracker {
 public:
  // create a tracker with a given name. start it immediately if do_start is true.
  DurationTracker(std::string&& name = "", bool do_start = false)
      : total_duration_{}, start_time_{}, running_{do_start}, name_(name) {
    static size_t obj_counter{};
    if (do_start) {
      start_time_ = std::chrono::steady_clock::now();
    }
    if (name.empty()) {
      name_ = std::to_string(++obj_counter);
    }
    DEBUG_TRP_PRINT(GL, KVLOG(name_, do_start));
  }

  // Starts tracker by changing state and recording start_time_. If do_reset is true - resets total_duration_.
  // Consecutive calls to start override the starting time.
  void start(bool do_reset = false) {
    ConcordAssertOR(running_, !do_reset);
    start_time_ = std::chrono::steady_clock::now();
    if (do_reset) {
      total_duration_ = 0;
    }
    running_ = true;
    DEBUG_TRP_PRINT(GL, KVLOG(name_, running_, total_duration_, do_reset));
  }

  // Stop tracker. Adds the time passed till now to total_duration_. If do_reset is true,resets total_duration_.
  // Returns total_duration_ value before optional reset.
  // Consecutive calls to stop with same flags returns the same total_duration_
  uint64_t stop(bool do_reset = false) {
    uint64_t ret = totalDuration(do_reset);
    running_ = false;
    DEBUG_TRP_PRINT(GL, KVLOG(name_, running_, total_duration_, ret, do_reset));
    return ret;
  }

  // Returns total_duration_, which is the sum of all start/stop calls + current ongoing interval (if running).
  // When do_reset is true, tracker keeps its state (running/not running), returns total_duration_ up to current point
  // in time and resets total_duration_ to 0.
  // When do_restart is true, tracker starts, returns total_duration_ up to current point before restarting,
  // resets total_duration_ to 0 and starts running.
  // It is not possible to call totalDuration() with both do_reset and do_restart set to true.
  uint64_t totalDuration(bool do_reset = false, bool do_restart = false) {
    ConcordAssert(!(do_reset && do_restart));
    uint64_t ret;
    if (running_) {
      total_duration_ += std::chrono::duration_cast<T>(std::chrono::steady_clock::now() - start_time_).count();
      start_time_ = std::chrono::steady_clock::now();
    }
    ret = total_duration_;
    if (do_reset) {
      total_duration_ = 0;
      start_time_ = TimePoint();
    } else if (do_restart) {
      total_duration_ = 0;
      start_time_ = std::chrono::steady_clock::now();
      running_ = true;
    }
    DEBUG_TRP_PRINT(GL, KVLOG(name_, running_, total_duration_, ret, do_reset));
    return ret;
  };

 protected:
  uint64_t total_duration_ = 0;
  using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
  TimePoint start_time_;
  bool running_ = false;
  std::string name_;
};  // class DurationTracker

/**
 * A Throughput object is used to calculate the number of items processed in a time unit.
 * After construction, it must be started by calling start().
 * It can be paused by calling pause() and resumed by calling resume(). While paused, reports cannot be made.
 * To continue call resume().
 * To end the current measurements, call end().After ending, pause and resume are not allowed, only start() can be
 * called to re-use the object.
 * In order to get meaningful statistics, user should report periodically to the object on the processing
 * progress by calling report().
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
  Throughput(uint32_t window_size = 0ul, std::string&& name = "");
  Throughput() = delete;

  // Reset all statistics and record starting time
  void start(bool do_reset = false);

  // stop timer. reporting is not allowed.
  void stop(bool do_reset = false);

  // Report amount of items processed since last report.
  // If window_size > 0: returns true if reached the end of a summary window, and started a new window
  // trigger_calc_throughput is true: manually trigger end of window
  bool report(uint64_t items_processed = 1, bool trigger_calc_throughput = false);

  struct Results {
    void reset() {
      elapsed_time_ms_ = 0;
      throughput_ = 0;
      num_processed_items_ = 0;
    }
    uint64_t elapsed_time_ms_ = 0;
    uint64_t throughput_ = 0;  // items per sec
    uint64_t num_processed_items_ = 0;
  };

  // Get overall Results: total number of items processed, and throughput from time elapsed_time_ms_
  const Results& getOverallResults();

  // Get previous window's results. Can be called only if report() returned true.
  const Results& getPrevWinResults() const;

  // Get previous window's index. Can be called only if report() returned true.
  uint64_t getPrevWinIndex() const;

 protected:
  struct Stats {
    Stats() = delete;
    Stats(std::string&& name);
    void start(bool do_reset = false);
    void stop(bool do_reset = false);
    void addProcessedItems(uint64_t num_processed_items);
    void calcThroughput();  // in Items/sec
    const Results& results() const { return results_; }
    std::string toString() const;

   protected:
    void reset();

    Results results_{};
    std::string name_;
    DurationTracker<std::chrono::milliseconds> total_duration_;
  };

  const uint32_t num_reports_per_window_;
  bool started_ = false;
  Stats overall_stats_;
  Stats current_window_stats_;
  Stats previous_window_stats_;
  uint64_t previous_window_index_;
  uint64_t reports_counter_ = 0;
  std::string name_;
};  // class Throughput

}  // namespace concord::util
