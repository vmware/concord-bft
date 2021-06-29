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
  overallStats_.restart();
  if (numReportsPerWindow_ > 0ul) {
    currentWindowStats_.restart();
  }
}

void Throughput::end() {
  ConcordAssert(started_);
  started_ = false;
  overallStats_.reset();
  if (numReportsPerWindow_ > 0ul) currentWindowStats_.reset();
}

bool Throughput::report(uint64_t itemsProcessed, bool triggerCalcThroughput) {
  ConcordAssert(started_);

  ++reportsCounter_;
  overallStats_.results_.numProcessedItems_ += itemsProcessed;
  if (numReportsPerWindow_ > 0ul) {
    currentWindowStats_.results_.numProcessedItems_ += itemsProcessed;
    if (triggerCalcThroughput || ((reportsCounter_ % numReportsPerWindow_) == 0ul)) {
      // Calculate throughput every  numReportsPerWindow_ reports
      previousWindowStats_ = currentWindowStats_;
      previousWindowIndex_ = (reportsCounter_ - 1) / numReportsPerWindow_;
      currentWindowStats_.restart();
      previousWindowStats_.calcThroughput();
      overallStats_.calcThroughput();
      prevWinCalculated_ = true;
      return true;
    }
  }

  return false;
}

void Throughput::pause() {
  ConcordAssert(started_);
  overallStats_.durationDT_.pause();
  currentWindowStats_.durationDT_.pause();
}

void Throughput::resume() {
  ConcordAssert(started_);
  overallStats_.durationDT_.start();
  currentWindowStats_.durationDT_.start();
}

const Throughput::Results& Throughput::getOverallResults() {
  if (!prevWinCalculated_) {
    ConcordAssert(started_);
    overallStats_.calcThroughput();
  }
  return overallStats_.results_;
}

const Throughput::Results& Throughput::getPrevWinResults() const {
  ConcordAssert(prevWinCalculated_);
  return previousWindowStats_.results_;
}

uint64_t Throughput::getPrevWinIndex() const {
  ConcordAssert(prevWinCalculated_);
  return previousWindowIndex_;
}

//////////////////////////////////////////////////////////////////////////////
// Throughput::Stats member functions
//////////////////////////////////////////////////////////////////////////////

void Throughput::Stats::reset() {
  results_.numProcessedItems_ = 0ull;
  results_.throughput_ = 0ull;
  durationDT_.reset();
}

void Throughput::Stats::restart() {
  reset();
  durationDT_.start();
}

void Throughput::Stats::calcThroughput() {
  results_.elapsedTimeMillisec_ = durationDT_.totalDuration();
  results_.throughput_ = static_cast<uint64_t>((1000 * results_.numProcessedItems_) / results_.elapsedTimeMillisec_);
}

}  // namespace concord::util
