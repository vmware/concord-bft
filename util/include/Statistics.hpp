// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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
#include <vector>
namespace concordMetrics {
struct Quantile {
  double quantile = 0.0;
  double value = 0.0;
  Quantile(double quantile_, double value_) : quantile(quantile_), value(value_) {}
};

typedef std::vector<Quantile> Quantiles;

struct SummaryDescription {
  Quantiles quantiles_;
  int64_t samples_sum_;
  int64_t samples_count_;
};
class ISummary {
 public:
  virtual void Observe(double value) = 0;
  virtual SummaryDescription Collect() = 0;
  virtual ~ISummary() {}
};

class IStatisticsFactory {
 public:
  virtual ISummary* createSummary(const Quantiles& quantiles) = 0;
  virtual ~IStatisticsFactory() {}
};

class EmptySummary : public ISummary {
 public:
  void Observe(double value) override {}
  SummaryDescription Collect() override { return {}; }
};

class DefaultStatisticFactory : public IStatisticsFactory {
 public:
  virtual ISummary* createSummary(const Quantiles& quantiles) { return new EmptySummary(); }
};

class StatisticsFactory {
  IStatisticsFactory* pImp = nullptr;

 public:
  StatisticsFactory() : pImp(new DefaultStatisticFactory()) {}

  ISummary* createSummary(const Quantiles& quantiles) { return pImp->createSummary(quantiles); }

  ~StatisticsFactory() {
    if (pImp) delete pImp;
  }

  static StatisticsFactory& get() {
    static StatisticsFactory sf;
    return sf;
  }

  static void setImp(StatisticsFactory& sf, IStatisticsFactory* pImp) { sf.pImp = pImp; }
};
}  // namespace concordMetrics
