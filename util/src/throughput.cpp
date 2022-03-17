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

namespace concord::util {

//////////////////////////////////////////////////////////////////////////////
// Throughput member functions
//////////////////////////////////////////////////////////////////////////////

Throughput::Throughput(uint32_t window_size, std::string&& name)
    : num_reports_per_window_(window_size),
      overall_stats_(name + std::string("_overall")),
      current_window_stats_(name + std::string("_cur")),
      previous_window_stats_(name + std::string("_prev")),
      name_(name) {
  static size_t obj_counter{};

  if (name.empty()) {
    name_ = std::to_string(++obj_counter);
  }
  DEBUG_TRP_PRINT(GL, KVLOG(name_, window_size));
}

void Throughput::start(bool do_reset) {
  started_ = true;
  if (do_reset) {
    overall_stats_.start(do_reset);
    if (num_reports_per_window_ > 0ul) {
      current_window_stats_.start(do_reset);
    }
  }
  overall_stats_.start();
  if (num_reports_per_window_ > 0ul) {
    current_window_stats_.start();
  }
  DEBUG_TRP_PRINT(GL, KVLOG(name_, do_reset));
}

bool Throughput::report(uint64_t items_processed, bool trigger_calc_throughput) {
  ConcordAssert(started_);
  bool ret{false};

  ++reports_counter_;
  overall_stats_.addProcessedItems(items_processed);
  if (num_reports_per_window_ > 0ul) {
    current_window_stats_.addProcessedItems(items_processed);
    if (trigger_calc_throughput || ((reports_counter_ % num_reports_per_window_) == 0ul)) {
      // Calculate throughput every  num_reports_per_window_ reports
      current_window_stats_.calcThroughput();
      previous_window_stats_ = current_window_stats_;
      previous_window_index_ = (reports_counter_ - 1) / num_reports_per_window_;
      current_window_stats_.start(true);
      ret = true;
    }
  }
  DEBUG_TRP_PRINT(GL,
                  KVLOG(name_,
                        current_window_stats_.toString(),
                        previous_window_stats_.toString(),
                        overall_stats_.toString(),
                        reports_counter_,
                        previous_window_index_,
                        ret,
                        items_processed,
                        trigger_calc_throughput));
  return ret;
}

void Throughput::stop(bool do_reset) {
  overall_stats_.stop(do_reset);
  DEBUG_TRP_PRINT(GL, KVLOG(name_, overall_stats_.toString()));
  current_window_stats_.stop(do_reset);
}

const Throughput::Results& Throughput::getOverallResults() {
  overall_stats_.calcThroughput();
  DEBUG_TRP_PRINT(GL, KVLOG(name_, overall_stats_.toString()));
  return overall_stats_.results();
}

const Throughput::Results& Throughput::getPrevWinResults() const {
  DEBUG_TRP_PRINT(GL, KVLOG(name_, previous_window_stats_.toString()));
  return previous_window_stats_.results();
}

uint64_t Throughput::getPrevWinIndex() const {
  DEBUG_TRP_PRINT(GL, KVLOG(name_));
  return previous_window_index_;
}

//////////////////////////////////////////////////////////////////////////////
// Throughput::Stats member functions
//////////////////////////////////////////////////////////////////////////////

Throughput::Stats::Stats(std::string&& name) : name_(name), total_duration_(std::move(name)) { reset(); }

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

void Throughput::Stats::addProcessedItems(uint64_t num_processed_items) {
  results_.num_processed_items_ += num_processed_items;
  DEBUG_TRP_PRINT(GL, KVLOG(name_, results_.num_processed_items_));
}

void Throughput::Stats::calcThroughput() {
  results_.elapsed_time_ms_ = total_duration_.totalDuration();
  if (results_.elapsed_time_ms_ == 0) {
    results_.elapsed_time_ms_ = 1;
  }
  results_.throughput_ = static_cast<uint64_t>((1000 * results_.num_processed_items_) / results_.elapsed_time_ms_);
  DEBUG_TRP_PRINT(GL, toString());
}

std::string Throughput::Stats::toString() const {
  return name_ + ": " + std::to_string(results_.elapsed_time_ms_) + " " + std::to_string(results_.throughput_) + " " +
         std::to_string(results_.num_processed_items_);
}

void Throughput::Stats::reset() { results_.reset(); }

}  // namespace concord::util
