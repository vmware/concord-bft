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

#include "throughput.hpp"

// uncomment to add debug prints
// #define DO_DEBUG
#ifdef DO_DEBUG
#define DEBUG_PRINT(x, y) LOG_TRACE(x, y)
#else
#define DEBUG_PRINT(x, y)
#endif

namespace concord::util {

//////////////////////////////////////////////////////////////////////////////
// Throughput member functions
//////////////////////////////////////////////////////////////////////////////

Throughput::Throughput(uint32_t window_size, std::string&& name)
    : num_reports_per_window_(window_size),
      overall_stats_(name),
      current_window_stats_(name),
      previous_window_stats_(name),
      name_(name) {
  static size_t obj_counter{};

  if (name.empty()) {
    name_ = std::to_string(++obj_counter);
  }
  DEBUG_PRINT(GL, KVLOG(name_, window_size));
}

void Throughput::start(bool do_reset) {
  started_ = true;
  if (do_reset) {
    overall_stats_.start(do_reset);
    if (num_reports_per_window_ > 0ul) {
      current_window_stats_.start(do_reset);
    }
  }
  overall_stats_.total_duration_.start();
  if (num_reports_per_window_ > 0ul) {
    current_window_stats_.total_duration_.start();
  }
  DEBUG_PRINT(GL, KVLOG(name_, do_reset));
}

bool Throughput::report(uint64_t items_processed, bool trigger_calc_throughput) {
  ConcordAssert(started_);
  bool ret{false};

  ++reports_counter_;
  overall_stats_.results_.num_processed_items_ += items_processed;
  if (num_reports_per_window_ > 0ul) {
    current_window_stats_.results_.num_processed_items_ += items_processed;
    if (trigger_calc_throughput || ((reports_counter_ % num_reports_per_window_) == 0ul)) {
      // Calculate throughput every  num_reports_per_window_ reports
      current_window_stats_.calcThroughput();
      previous_window_stats_ = current_window_stats_;
      previous_window_index_ = (reports_counter_ - 1) / num_reports_per_window_;
      current_window_stats_.start(true);
    }
  }
  DEBUG_PRINT(GL, KVLOG(name_, ret, items_processed, trigger_calc_throughput));
  return ret;
}

void Throughput::stop(bool do_reset) {
  overall_stats_.total_duration_.stop(do_reset);
  current_window_stats_.total_duration_.stop(do_reset);
}

const Throughput::Results& Throughput::getOverallResults() {
  overall_stats_.calcThroughput();
  DEBUG_PRINT(GL, KVLOG(name_));
  return overall_stats_.results_;
}

const Throughput::Results& Throughput::getPrevWinResults() const {
  DEBUG_PRINT(GL, KVLOG(name_));
  return previous_window_stats_.results_;
}

uint64_t Throughput::getPrevWinIndex() const {
  DEBUG_PRINT(GL, KVLOG(name_));
  return previous_window_index_;
}

//////////////////////////////////////////////////////////////////////////////
// Throughput::Stats member functions
//////////////////////////////////////////////////////////////////////////////

Throughput::Stats::Stats(std::string& name) : total_duration_(std::move(name)) { reset(); }

void Throughput::Stats::start(bool do_reset) {
  if (do_reset) {
    results_.reset();
  }
  total_duration_.start(do_reset);
}

void Throughput::Stats::stop(bool do_reset) {
  if (do_reset) {
    results_.reset();
  }
  total_duration_.stop(do_reset);
}

void Throughput::Stats::calcThroughput() {
  results_.elapsed_time_ms_ = total_duration_.totalDuration();
  if (results_.elapsed_time_ms_ == 0) {
    results_.elapsed_time_ms_ = 1;
  }
  results_.throughput_ = static_cast<uint64_t>((1000 * results_.num_processed_items_) / results_.elapsed_time_ms_);
}

void Throughput::Stats::reset() { results_.reset(); }

}  // namespace concord::util
