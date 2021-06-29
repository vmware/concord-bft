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

/** A Duration Tracker allows to track the sum of multiple none-continues time intervals (durations).
 * This is done by calling start() / pause() multiple times.
 * If the last call to the tracker is start(): call totalDuration() to get the sum of intervals, including current
 * still running interval.
 * If the last call to a tracker is pause(): you may get the sum of intervals from pause() returned
 * value or call totalDuration() explicitly.
 * To reset (re-use) the tracker, call reset() and then start().
 */
template <typename T>
class DurationTracker {
 public:
  void start() {
    ConcordAssert(!running_);
    startTime_ = std::chrono::steady_clock::now();
    running_ = true;
  }
  uint64_t pause() {
    if (running_)
      totalDuration_ += std::chrono::duration_cast<T>(std::chrono::steady_clock::now() - startTime_).count();
    running_ = false;
    return totalDuration_;
  }
  void reset() {
    totalDuration_ = 0;
    running_ = false;
  }
  uint64_t totalDuration(bool doReset = false) {
    if (running_) {
      totalDuration_ = pause();
      if (doReset)
        reset();
      else
        start();
    }
    return totalDuration_;
  };

 private:
  uint64_t totalDuration_ = 0;
  std::chrono::time_point<std::chrono::steady_clock> startTime_;
  bool running_ = false;
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
 * If the user supplies a windowSize > 0:
 * 1) User may call all member functions prefixed with getPrevWin*.
 * 2) Last window throughput is calculated and saved.
 * 3) Overall and last window calculations are based on the window's end time.
 * 4) report() returns true when the window's end reached.
 *
 * To get overall and/or last window statistics, the user has 2 options:
 * 1) If windowSize > 0, it should waits until report() returns true and then it may call getOverallResults()
 * and/or getPrevWinResults().
 * 2) If windowSize is 0, user can call at any time for getOverallResults(). Calling report() to continue collecting
 * statistics is still possible after.
 */
class Throughput {
 public:
  Throughput(uint32_t windowSize = 0ul) : numReportsPerWindow_(windowSize) {}
  Throughput() = delete;

  // Reset all statistics and record starting time
  void start();
  bool isStarted() { return started_; }

  // Reset all statistics, and set started_ to false. Call a again start() to re-use object
  void end();

  // pause timer. reporting is not allowed.
  void pause();

  // continue timer after pause was called()
  void resume();

  // Report amount of items processed since last report.
  // If windowSize > 0: returns true if reached the end of a summary window, and started a new window
  // triggerCalcThroughput is true: manually trigger end of window
  bool report(uint64_t itemsProcessed = 1, bool triggerCalcThroughput = false);

  struct Results {
    uint64_t elapsedTimeMillisec_ = 0ull;
    uint64_t throughput_ = 0ull;  // items per sec
    uint64_t numProcessedItems_ = 0ull;
  };

  // Get overall Results: total number of items processed, and throughput from time elapsedTimeMillisec_
  const Results& getOverallResults();

  // Get previous window's results. Can be called only if report() returned true.
  const Results& getPrevWinResults() const;

  // Get previous window's index. Can be called only if report() returned true.
  uint64_t getPrevWinIndex() const;

 protected:
  struct Stats {
    DurationTracker<std::chrono::milliseconds> durationDT_;
    Results results_;

    void restart();
    void reset();
    void calcThroughput();  // in Items/sec
  };

  const uint32_t numReportsPerWindow_;
  bool started_ = false;
  bool prevWinCalculated_ = false;
  Stats overallStats_;
  Stats currentWindowStats_;
  Stats previousWindowStats_;
  uint64_t previousWindowIndex_;
  uint64_t reportsCounter_ = 0;
};  // class Throughput

}  // namespace concord::util
