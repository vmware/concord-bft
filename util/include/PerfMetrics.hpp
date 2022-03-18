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
#include <variant>
#include <mutex>

#include "RollingAvgAndVar.hpp"
#include "Metrics.hpp"
#include "Logger.hpp"

using namespace std;
using concordMetrics::GaugeHandle;
using concordMetrics::AtomicGaugeHandle;
using concordMetrics::Component;

template <typename T>
class PerfMetric {
 public:
  PerfMetric(Component& component, string name, uint64_t num_map_entries_for_reset, bool isThreadSafe)
      : num_map_entries_for_reset_(num_map_entries_for_reset),
        is_thread_safe_(isThreadSafe),
        name_(name),
        avg_{component.RegisterGauge(name + "Avg", 0)},
        variance_{component.RegisterGauge(name + "Variance", 0)} {}

  void addStartTimeStamp(const T& key) {
    if (is_thread_safe_) {
      lock_guard<mutex> lock(mutex_);
      addStartTimeStampUnSafe(key);
    } else {
      addStartTimeStampUnSafe(key);
    }
  }

  void finishMeasurement(const T& key) {
    if (is_thread_safe_) {
      lock_guard<mutex> lock(mutex_);
      finishMeasurementUnSafe(key);
    } else {
      finishMeasurementUnSafe(key);
    }
  }

  void deleteSingleEntry(const T& key) {
    if (is_thread_safe_) {
      lock_guard<mutex> lock(mutex_);
      deleteSingleEntryUnSafe(key);
    } else {
      deleteSingleEntryUnSafe(key);
    }
  }

  void reset() {
    if (is_thread_safe_) {
      lock_guard<mutex> lock(mutex_);
      entries_.clear();
    } else {
      entries_.clear();
    }
  }

 private:
  void addStartTimeStampUnSafe(const T& key) {
    if (entries_.count(key) == 0) {
      entries_[key] = getMonotonicTime();
    }
  }
  void finishMeasurementUnSafe(const T& key) {
    if (entries_.count(key) != 0) {
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(getMonotonicTime() - entries_[key]).count();

      avg_and_variance_.add(static_cast<double>(duration));

      avg_.Get().Set(static_cast<uint64_t>(avg_and_variance_.avg()));
      variance_.Get().Set(static_cast<uint64_t>(avg_and_variance_.var()));
      entries_.erase(key);
    }

    if (entries_.size() > num_map_entries_for_reset_) {
      reset();
    }
  }
  void deleteSingleEntryUnSafe(const T& key) {
    if (entries_.count(key) != 0) {
      entries_.erase(key);
    }
  }

  mutex mutex_;
  uint64_t num_map_entries_for_reset_;
  bool is_thread_safe_;
  string name_;
  bftEngine::impl::RollingAvgAndVar avg_and_variance_;
  unordered_map<T, chrono::time_point<std::chrono::steady_clock>> entries_;
  GaugeHandle avg_;
  GaugeHandle variance_;
};