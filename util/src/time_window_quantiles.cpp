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

#include <time_window_quantiles.hpp>

#include "time_window_quantiles.hpp"
namespace concordMetrics {
namespace prometheusMetrics {

TimeWindowQuantiles::TimeWindowQuantiles(const std::vector<CKMSQuantiles::Quantile>& quantiles,
                                         const Clock::duration max_age,
                                         const int age_buckets)
    : quantiles_(quantiles),
      ckms_quantiles_(age_buckets, CKMSQuantiles(quantiles_)),
      current_bucket_(0),
      last_rotation_(Clock::now()),
      rotation_interval_(max_age / age_buckets) {}

double TimeWindowQuantiles::get(double q) {
  CKMSQuantiles& current_bucket = rotate();
  return current_bucket.get(q);
}

void TimeWindowQuantiles::insert(double value) {
  rotate();
  for (auto& bucket : ckms_quantiles_) {
    bucket.insert(value);
  }
}

CKMSQuantiles& TimeWindowQuantiles::rotate() {
  auto delta = Clock::now() - last_rotation_;
  while (delta > rotation_interval_) {
    ckms_quantiles_[current_bucket_].reset();

    if (++current_bucket_ >= ckms_quantiles_.size()) {
      current_bucket_ = 0;
    }

    delta -= rotation_interval_;
    last_rotation_ += rotation_interval_;
  }
  return ckms_quantiles_[current_bucket_];
}
}  // namespace prometheusMetrics
}  // namespace concordMetrics