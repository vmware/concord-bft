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
void Throughput::start() {
  started_ = true;
  overall_stats_.reset();
  if (num_reports_per_window_ > 0ul) {
    current_window_stats_.reset();
  }
}

bool Throughput::report(uint64_t items_processed) {
  ConcordAssert(started_);

  ++reports_counter_;
  overall_stats_.results_.num_processed_items_ += items_processed;

  if (num_reports_per_window_ > 0ul) {
    current_window_stats_.results_.num_processed_items_ += items_processed;
    if ((reports_counter_ % num_reports_per_window_) == 0ul) {
      // Calculate throughput every num_reports_per_window_ reports
      previous_window_stats_ = current_window_stats_;
      previous_window_index_ = (reports_counter_ - 1) / num_reports_per_window_;
      current_window_stats_.reset();
      previous_window_stats_.calcThroughput();
      overall_stats_.calcThroughput();
      prev_win_calculated_ = true;
      return true;
    }
  }

  return false;
}

const Throughput::Results& Throughput::getOverallResults() {
  if (!prev_win_calculated_) {
    ConcordAssert(started_);
    overall_stats_.calcThroughput();
  }

  return overall_stats_.results_;
}

const Throughput::Results& Throughput::getPrevWinResults() const {
  ConcordAssert(prev_win_calculated_);

  return previous_window_stats_.results_;
}

uint64_t Throughput::getPrevWinIndex() const {
  ConcordAssert(prev_win_calculated_);

  return previous_window_index_;
}

//////////////////////////////////////////////////////////////////////////////
// Throughput::Stats member functions
//////////////////////////////////////////////////////////////////////////////

void Throughput::Stats::reset() {
  results_.num_processed_items_ = 0ull;
  results_.throughput_ = 0ull;
  start_time_ = std::chrono::steady_clock::now();
}

void Throughput::Stats::calcThroughput() {
  auto duration = std::chrono::steady_clock::now() - start_time_;
  results_.elapsed_time_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  results_.throughput_ = static_cast<uint64_t>((1000 * results_.num_processed_items_) / results_.elapsed_time_ms_);
}

}  // namespace concord::util
