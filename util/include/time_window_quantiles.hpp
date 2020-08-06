// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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

#include <chrono>
#include <cstddef>
#include <vector>

#include "ckms_quantiles.hpp"
namespace concordMetrics {
namespace prometheusMetrics {

class TimeWindowQuantiles {
  using Clock = std::chrono::steady_clock;

 public:
  TimeWindowQuantiles(const std::vector<CKMSQuantiles::Quantile>& quantiles,
                      Clock::duration max_age_seconds,
                      int age_buckets);

  double get(double q);
  void insert(double value);

 private:
  CKMSQuantiles& rotate();

  const std::vector<CKMSQuantiles::Quantile>& quantiles_;
  std::vector<CKMSQuantiles> ckms_quantiles_;
  std::size_t current_bucket_;

  Clock::time_point last_rotation_;
  const Clock::duration rotation_interval_;
};
}  // namespace prometheusMetrics
}  // namespace concordMetrics
