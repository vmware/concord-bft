// Concord
//
// Copyright (c) 2019-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <unordered_map>
#include <string>
#include <chrono>
#include <mutex>

#include "RollingAvgAndVar.hpp"
#include "Metrics.hpp"
#include "Logger.hpp"

using std::chrono::steady_clock;
using concordMetrics::GaugeHandle;
using concordMetrics::AtomicGaugeHandle;
using concordMetrics::Component;

template <typename T>
class PerfMetric {
 public:
  PerfMetric(Component& component,
             std::string name,
             uint64_t num_map_entries_for_reset,
             int moving_avg_window_size,
             bool isThreadSafe)
      : num_map_entries_for_reset_(num_map_entries_for_reset),
        moving_avg_window_size_(moving_avg_window_size),
        is_thread_safe_(isThreadSafe),
        name_(name),
        avg_{component.RegisterGauge(name + "Avg", 0)},
        variance_{component.RegisterGauge(name + "Variance", 0)} {}

  void addStartTimeStamp(const T& key) {
    if (is_thread_safe_) {
      std::lock_guard<std::recursive_mutex> lock(mutex_);
      addStartTimeStampUnSafe(key);
    } else {
      addStartTimeStampUnSafe(key);
    }
  }

  void finishMeasurement(const T& key) {
    if (is_thread_safe_) {
      std::lock_guard<std::recursive_mutex> lock(mutex_);
      finishMeasurementUnSafe(key);
    } else {
      finishMeasurementUnSafe(key);
    }
  }

  void deleteSingleEntry(const T& key) {
    if (is_thread_safe_) {
      std::lock_guard<std::recursive_mutex> lock(mutex_);
      deleteSingleEntryUnSafe(key);
    } else {
      deleteSingleEntryUnSafe(key);
    }
  }

  // this resets the map that holds all the concurrent measurements (multithreading support), as a safety mechanism
  // to prevent entries in the map from never being erased (measurements that started but never finished)
  void resetEntries() {
    if (is_thread_safe_) {
      std::lock_guard<std::recursive_mutex> lock(mutex_);
      entries_.clear();
    } else {
      entries_.clear();
    }
  }

 private:
  void resetAvgAndVarUnsafe() {
    auto tmp_avg = avg_and_variance_.avg();
    // we reset the rolling avg object in order to have a "moving avg" behavior and don't remember the
    // entire history of the measurements
    avg_and_variance_.resetUnsafe();
    // after the rolling avg reset we don't want to start from 0, so we artificially add
    // the last avg before the reset to the "new" rolling avg. we do it 3 times in order to reduce the impact
    // of the 1st sample after the reset. as long as the sliding window size it at lease 10x long it should still
    // reflect the "new" avg pretty well.
    for (int i = 0; i < 3; i++) {
      avg_and_variance_.add(tmp_avg);
    }
  }

  void addStartTimeStampUnSafe(const T& key) {
    if (entries_.count(key) == 0) {
      entries_[key] = steady_clock::now();
    }
  }
  void finishMeasurementUnSafe(const T& key) {
    if (entries_.count(key) != 0) {
      auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(steady_clock::now() - entries_[key]).count();

      avg_and_variance_.add(static_cast<double>(duration));

      avg_.Get().Set(static_cast<uint64_t>(avg_and_variance_.avg()));
      variance_.Get().Set(static_cast<uint64_t>(avg_and_variance_.var()));

      entries_.erase(key);
    }

    if (entries_.size() > num_map_entries_for_reset_) {
      resetEntries();
    }

    if (avg_and_variance_.numOfElementsUnsafe() >= moving_avg_window_size_) {
      resetAvgAndVarUnsafe();
    }
  }
  void deleteSingleEntryUnSafe(const T& key) {
    if (entries_.count(key) != 0) {
      entries_.erase(key);
    }
  }

  std::recursive_mutex mutex_;
  uint64_t num_map_entries_for_reset_;
  int moving_avg_window_size_;
  bool is_thread_safe_;
  std::string name_;
  bftEngine::impl::RollingAvgAndVar avg_and_variance_;
  std::unordered_map<T, std::chrono::time_point<std::chrono::steady_clock>> entries_;
  GaugeHandle avg_;
  GaugeHandle variance_;
};

// this class is for simple average and duration meters (no histograms) which can be paused and resumed
// as many times as the user likes before a measurement is finished. it is currently used in concord level code,
// but can be used in concord-bft as well.
class SimpleAvgAndVarDurationMeter {
 public:
  SimpleAvgAndVarDurationMeter() : last_saved_timestamp_{}, intermediate_accumulation_{0}, state_{NOT_STARTED} {}

  void startMeasurement() {
    if (state_ == NOT_STARTED) {
      last_saved_timestamp_ = steady_clock::now();
      state_ = RUNNING;
    }
    return;
  }

  void pauseMeasurement() {
    if (state_ == RUNNING) {
      const std::chrono::time_point<std::chrono::steady_clock> now = steady_clock::now();
      state_ = NOT_RUNNING;
      intermediate_accumulation_ += now - last_saved_timestamp_;
      last_saved_timestamp_ = now;
    }
    return;
  }

  void resumeMeasurement() {
    if (state_ == NOT_RUNNING) {
      last_saved_timestamp_ = steady_clock::now();
      state_ = RUNNING;
    }
    return;
  }

  void finishMeasurement() {
    pauseMeasurement();

    if (state_ != NOT_STARTED) {
      avg_n_var_.add(intermediate_accumulation_.count());
      intermediate_accumulation_ = intermediate_accumulation_.zero();
      state_ = NOT_STARTED;
    }

    return;
  }

  double getAvg() { return avg_n_var_.avg(); }

  double getVar() { return avg_n_var_.var(); }

 private:
  enum MeterState { NOT_STARTED, RUNNING, NOT_RUNNING };

  std::chrono::time_point<std::chrono::steady_clock> last_saved_timestamp_;
  std::chrono::duration<double, std::milli> intermediate_accumulation_;
  MeterState state_;
  bftEngine::impl::RollingAvgAndVar avg_n_var_;
};
