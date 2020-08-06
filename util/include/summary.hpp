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
#include <cstdint>
#include <mutex>
#include <vector>

#include "ckms_quantiles.hpp"
#include "time_window_quantiles.hpp"

namespace concordMetrics {
namespace prometheusMetrics {
class Summary {
 public:
  struct Quantile {
    double quantile = 0.0;
    double value = 0.0;
  };

  struct SummaryDescription {
    std::uint64_t sample_count = 0;
    double sample_sum = 0.0;
    std::vector<Quantile> quantile;
  };

  using Quantiles = std::vector<CKMSQuantiles::Quantile>;

  /// \brief Create a summary metric.
  ///
  /// \param quantiles A list of 'targeted' Phi-quantiles. A targeted
  /// Phi-quantile is specified in the form of a Phi-quantile and tolerated
  /// error. For example a Quantile{0.5, 0.1} means that the median (= 50th
  /// percentile) should be returned with 10 percent error or a Quantile{0.2,
  /// 0.05} means the 20th percentile with 5 percent tolerated error. Note that
  /// percentiles and quantiles are the same concept, except percentiles are
  /// expressed as percentages. The Phi-quantile must be in the interval [0, 1].
  /// Note that a lower tolerated error for a Phi-quantile results in higher
  /// usage of resources (memory and cpu) to calculate the summary.
  ///
  /// The Phi-quantiles are calculated over a sliding window of time. The
  /// sliding window of time is configured by max_age and age_buckets.
  ///
  /// \param max_age Set the duration of the time window, i.e., how long
  /// observations are kept before they are discarded. The default value is 60
  /// seconds.
  ///
  /// \param age_buckets Set the number of buckets of the time window. It
  /// determines the number of buckets used to exclude observations that are
  /// older than max_age from the summary, e.g., if max_age is 60 seconds and
  /// age_buckets is 5, buckets will be switched every 12 seconds. The value is
  /// a trade-off between resources (memory and cpu for maintaining the bucket)
  /// and how smooth the time window is moved. With only one age bucket it
  /// effectively results in a complete reset of the summary each time max_age
  /// has passed. The default value is 5.
  Summary(Quantiles& quantiles, std::chrono::milliseconds max_age = std::chrono::seconds{60}, int age_buckets = 5);

  /// \brief Observe the given amount.
  void Observe(double value);

  /// \brief Get the current value of the summary.
  ///
  /// Collect is called by the Registry when collecting metrics.
  SummaryDescription Collect();

  std::string ToJson();

 private:
  const Quantiles quantiles_;
  std::uint64_t count_;
  double sum_;
  TimeWindowQuantiles quantile_values_;
};

}  // namespace prometheusMetrics
}  // namespace concordMetrics