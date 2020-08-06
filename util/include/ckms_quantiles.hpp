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

#include <array>
#include <cstddef>
#include <functional>
#include <vector>

namespace concordMetrics {
namespace prometheusMetrics {

class CKMSQuantiles {
 public:
  class Quantile {
   public:
    const double quantile;
    const double error;
    const double u;
    const double v;

    Quantile(double quantile, double error);
  };

 private:
  class Item {
   public:
    double value;
    int g;
    int delta;
    explicit Item(double value, int lower_delta, int delta);
  };

 public:
  explicit CKMSQuantiles(const std::vector<Quantile>& quantiles);
  void insert(double value);
  double get(double q);
  void reset();

 private:
  double allowableError(int rank);
  bool insertBatch();
  void compress();

 private:
  const std::reference_wrapper<const std::vector<Quantile>> quantiles_;

  std::size_t count_;
  std::vector<Item> sample_;
  std::array<double, 500> buffer_;
  std::size_t buffer_count_;
};

}  // namespace prometheusMetrics
}  // namespace concordMetrics
