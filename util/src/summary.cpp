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

#include "summary.hpp"
#include <sstream>

namespace concordMetrics {
namespace prometheusMetrics {
Summary::Summary(Quantiles& quantiles, const std::chrono::milliseconds max_age, const int age_buckets)
    : quantiles_{quantiles}, count_{0}, sum_{0}, quantile_values_{quantiles_, max_age, age_buckets} {}

void Summary::Observe(const double value) {
  count_ += 1;
  sum_ += value;
  quantile_values_.insert(value);
}

Summary::SummaryDescription Summary::Collect() {
  auto metric = Summary::SummaryDescription{};

  for (const auto& quantile : quantiles_) {
    auto metricQuantile = Summary::Quantile{};
    metricQuantile.quantile = quantile.quantile;
    metricQuantile.value = quantile_values_.get(quantile.quantile);
    metric.quantile.push_back(metricQuantile);
  }
  metric.sample_count = count_;
  metric.sample_sum = sum_;

  return metric;
}

std::string Summary::ToJson() {
  auto data = Collect();
  std::ostringstream oss;
  oss << "{\"Quantiles\":{";
  for (uint32_t i = 0; i < data.quantile.size(); i++) {
    if (i != 0) oss << ",";
    oss << "\"" << data.quantile[i].quantile << "\":" << data.quantile[i].value << "";
  }
  oss << "}";
  oss << ", \"Sample_sum\":" << data.sample_sum;
  oss << ", \"Sample_count:\"" << data.sample_count;
  oss << "}";
  return oss.str();
}
}  // namespace prometheusMetrics
}  // namespace concordMetrics