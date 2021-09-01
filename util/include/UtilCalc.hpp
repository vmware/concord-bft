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

#pragma once

#include <string>
#include <chrono>
#include "Logger.hpp"

namespace concordUtils {
using namespace std::chrono;
uint32_t MAX_GAP = 10;
class UtilCalc {
 public:
  UtilCalc();
  ~UtilCalc() {}

  void Start();
  void End();
  void Add(uint64_t ms);

  std::string ToString() const;

  uint64_t getMonotonicTimeMilli();

 private:
  uint64_t activeMilliSeconds_;
  uint64_t lastSecond_;
  uint64_t startMilli_;
  uint64_t aggMilliSeconds_;
  uint64_t secondCount_;
  logging::Logger logger_ = logging::getLogger("util");

  /*double Median() const;
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;*/
};

}  // namespace concordUtils
