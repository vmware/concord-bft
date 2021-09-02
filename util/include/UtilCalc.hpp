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
#include "RollingAvgAndVar.hpp"
#include "Metrics.hpp"
#include "../../diagnostics/include/diagnostics.h"

namespace concordUtils {
using namespace std::chrono;

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
  concordMetrics::Component metricsComponent_;
  concordMetrics::GaugeHandle average_util_gauge_;
  bftEngine::impl::RollingAvgAndVar average_util_;

  // 1 Minutes
  static constexpr int64_t MAX_VALUE_MILLISECONDS = 1000 * 60;

  struct Recorders {
    Recorders() {
      auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      const auto component = "UtilCalc";
      if (!registrar.perf.isRegisteredComponent(component)) {
        registrar.perf.registerComponent(component, {mainThread});
      }
    }

    DEFINE_SHARED_RECORDER(mainThread, 1, 100000, 3, concord::diagnostics::Unit::COUNT);
  };

  Recorders histograms_;
};

}  // namespace concordUtils