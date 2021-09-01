// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <cmath>
#include <stdio.h>
#include "UtilCalc.hpp"

namespace concordUtils {

UtilCalc::UtilCalc() {
  activeMilliSeconds_ = 0;
  lastSecond_ = 0;
  startMilli_ = 0;
  aggMilliSeconds_ = 0;
  secondCount_ = 0;
}

void UtilCalc::Start() {
  uint64_t nowMilli = getMonotonicTimeMilli();
  uint64_t nowSecond = nowMilli / 1000;
  if (nowSecond > lastSecond_ && activeMilliSeconds_ > 0) {
    Add(activeMilliSeconds_);
    activeMilliSeconds_ = 0;
  }
  lastSecond_ = nowSecond;
  startMilli_ = nowMilli;
}

void UtilCalc::End() {
  uint64_t nowMilli = getMonotonicTimeMilli();
  uint64_t nowSecond = nowMilli / 1000;
  if (nowSecond < lastSecond_) {
    LOG_WARN(logger_, "The clock is not monotonic, can't measure");
  } else if (nowSecond == lastSecond_) {
    if (nowMilli - startMilli_ > 0) activeMilliSeconds_ += (nowMilli - startMilli_);
  } else if (nowSecond > lastSecond_) {
    activeMilliSeconds_ += (1000 - (startMilli_ % 1000));
    Add(activeMilliSeconds_);
    if (nowSecond - lastSecond_ > MAX_GAP) {
      LOG_WARN(logger_, "Measured a gap of=" << MAX_GAP << " seconds");
      lastSecond_ = nowSecond - MAX_GAP;
    }
    for (uint64_t i = lastSecond_ + 1; i <= nowSecond - 1; i++) Add(1000);
    activeMilliSeconds_ += (nowMilli % 1000);
    lastSecond_ = nowSecond;
  }
}

void UtilCalc::Add(uint64_t ms) {
  aggMilliSeconds_ += ms;
  secondCount_++;
}

std::string UtilCalc::ToString() const {
  std::string r = "";
  r += "Measured " + std::to_string(secondCount_) + " seconds\n";
  uint64_t aggMsPercent = aggMilliSeconds_ / (secondCount_ * 1000);
  r += "The main thread was occupied " + std::to_string(aggMsPercent * 100) + " of the time";
  return r;
}

uint64_t UtilCalc::getMonotonicTimeMilli() {
  steady_clock::time_point curTimePoint = steady_clock::now();
  return duration_cast<milliseconds>(curTimePoint.time_since_epoch()).count();
}

}  // namespace concordUtils
