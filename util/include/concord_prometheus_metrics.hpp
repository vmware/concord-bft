// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/summary.h>
#include <string>
#include <utility>
#include "Logger.hpp"
#include "Metrics.hpp"
#include "Statistics.hpp"

namespace concord::utils {

class PrometheusRegistry;
template <typename T>
class ConcordCustomCollector : public prometheus::Collectable {
  prometheus::Family<T>& createFamily(const std::string& name,
                                      const std::string& help,
                                      const std::map<std::string, std::string>& labels);

 public:
  explicit ConcordCustomCollector(std::chrono::seconds dumpInterval, bool dump_metrics_enabled)
      : logger_(logging::getLogger("concord.utils.prometheus")),
        dumpInterval_(dumpInterval),
        dump_metrics_enabled_(dump_metrics_enabled),
        last_dump_time_(0) {}
  std::vector<prometheus::MetricFamily> Collect() const override;
  friend class PrometheusRegistry;

 private:
  logging::Logger logger_;
  std::vector<std::shared_ptr<prometheus::Family<T>>> metrics_;
  std::chrono::seconds dumpInterval_;
  bool dump_metrics_enabled_;
  mutable std::chrono::seconds last_dump_time_;
  mutable std::mutex lock_;
};

class PrometheusRegistry {
 public:
  explicit PrometheusRegistry(const std::string& bindAddress,
                              uint64_t metricsDumpInterval /* 10 minutes by default */,
                              const bool& metricsDumpEnabled = true);

  explicit PrometheusRegistry(const std::string& bindAddress);
  PrometheusRegistry();
  void scrapeRegistry(std::shared_ptr<prometheus::Collectable> registry);

  prometheus::Family<prometheus::Counter>& createCounterFamily(const std::string& name,
                                                               const std::string& help,
                                                               const std::map<std::string, std::string>& labels);

  prometheus::Counter& createCounter(prometheus::Family<prometheus::Counter>& source,
                                     const std::map<std::string, std::string>& labels);

  prometheus::Counter& createCounter(const std::string& name,
                                     const std::string& help,
                                     const std::map<std::string, std::string>& labels);

  prometheus::Family<prometheus::Gauge>& createGaugeFamily(const std::string& name,
                                                           const std::string& help,
                                                           const std::map<std::string, std::string>& labels);

  prometheus::Gauge& createGauge(prometheus::Family<prometheus::Gauge>& source,
                                 const std::map<std::string, std::string>& labels);

  prometheus::Gauge& createGauge(const std::string& name,
                                 const std::string& help,
                                 const std::map<std::string, std::string>& labels);

  prometheus::Family<prometheus::Histogram>& createHistogramFamily(const std::string& name,
                                                                   const std::string& help,
                                                                   const std::map<std::string, std::string>& labels);

  // Measures distribution of quantity such as duration. User defines a list of buckets, and calls Observe
  // With the observed quantity. As a result, the counter of the corresponding bucket is being incremented.
  prometheus::Histogram& createHistogram(prometheus::Family<prometheus::Histogram>& source,
                                         const std::map<std::string, std::string>& labels,
                                         const std::vector<double>& buckets);

  prometheus::Histogram& createHistogram(const std::string& name,
                                         const std::string& help,
                                         const std::map<std::string, std::string>& labels,
                                         const std::vector<double>& buckets);

  prometheus::Family<prometheus::Summary>& createSummaryFamily(const std::string& name,
                                                               const std::string& help,
                                                               const std::map<std::string, std::string>& labels);

  prometheus::Summary& createSummary(prometheus::Family<prometheus::Summary>& source,
                                     const std::map<std::string, std::string>& labels,
                                     const prometheus::Summary::Quantiles& quantiles,
                                     std::chrono::milliseconds max_age = std::chrono::seconds{60},
                                     int age_buckets = 5);

  prometheus::Summary& createSummary(const std::string& name,
                                     const std::string& help,
                                     const std::map<std::string, std::string>& labels,
                                     const prometheus::Summary::Quantiles& quantiles,
                                     std::chrono::milliseconds max_age = std::chrono::seconds{60},
                                     int age_buckets = 5);

 private:
  static const uint64_t defaultMetricsDumpInterval = 600;

  std::unique_ptr<prometheus::Exposer> exposer_;
  std::shared_ptr<ConcordCustomCollector<prometheus::Counter>> counters_custom_collector_;
  std::shared_ptr<ConcordCustomCollector<prometheus::Gauge>> gauges_custom_collector_;
  std::shared_ptr<ConcordCustomCollector<prometheus::Histogram>> histogram_custom_collector_;
  std::shared_ptr<ConcordCustomCollector<prometheus::Summary>> summary_custom_collector_;
};

class ConcordBftPrometheusCollector : public prometheus::Collectable {
 public:
  explicit ConcordBftPrometheusCollector(bool metricsEnabled = true)
      : aggregator_(std::make_shared<concordMetrics::Aggregator>(metricsEnabled)) {}
  std::vector<prometheus::MetricFamily> Collect() const override;
  std::shared_ptr<concordMetrics::Aggregator> getAggregator() { return aggregator_; }

 private:
  prometheus::ClientMetric collect(const std::string& component, concordMetrics::Counter& c) const;
  prometheus::ClientMetric collect(const std::string& component, concordMetrics::Gauge& g) const;
  prometheus::ClientMetric collect(const std::string& component, concordMetrics::Status& s) const;

  std::vector<prometheus::MetricFamily> collectCounters() const;
  std::vector<prometheus::MetricFamily> collectGauges() const;
  std::vector<prometheus::MetricFamily> collectStatuses() const;
  std::string getMetricName(const std::string& origName) const;

 private:
  const std::string metricNamePrefix_ = "concord_concordbft_";
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
};

class ConcordBftStatisticsCollector;
class ConcordBftStatisticsFactory : public concordMetrics::IStatisticsFactory {
 public:
  class ConcordBftSummaryImp : public concordMetrics::ISummary {
   public:
    explicit ConcordBftSummaryImp(prometheus::Summary& summary) : summary_(summary) {}
    void Observe(double value) override { summary_.Observe(value); }
    concordMetrics::SummaryDescription Collect() override { return {}; }

   private:
    prometheus::Summary& summary_;
  };

  ConcordBftStatisticsFactory()
      : concordBFtSummaries_("concordBftSummaries", "Collection of all concordBftSummaries", {}) {}
  std::unique_ptr<concordMetrics::ISummary> createSummary(const std::string& name,
                                                          const concordMetrics::Quantiles& quantiles) override {
    prometheus::Summary::Quantiles q;
    for (auto& quantile : quantiles) {
      q.emplace_back(prometheus::detail::CKMSQuantiles::Quantile(quantile.quantile, quantile.value));
    }
    return std::make_unique<ConcordBftSummaryImp>(concordBFtSummaries_.Add({{"internalMetricName", name}}, q));
  }

 private:
  prometheus::Family<prometheus::Summary> concordBFtSummaries_;
  friend ConcordBftStatisticsCollector;
};

class ConcordBftStatisticsCollector : public prometheus::Collectable {
 public:
  ConcordBftStatisticsCollector()
      : bftStatisticsFactory_(
            concordMetrics::StatisticsFactory::setImp(std::make_unique<ConcordBftStatisticsFactory>())) {}
  std::vector<prometheus::MetricFamily> Collect() const override {
    return dynamic_cast<ConcordBftStatisticsFactory&>(bftStatisticsFactory_).concordBFtSummaries_.Collect();
  }

 private:
  concordMetrics::IStatisticsFactory& bftStatisticsFactory_;
};

template <typename T, typename RESOLUTION>
class PrometheusTimeRecorder {
 public:
  explicit PrometheusTimeRecorder(T& recorder,
                                  bool active = true,
                                  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now())
      : recorder_(recorder), active_(active), start_time_(start_time) {}
  void cancel() { active_ = false; }
  uint64_t take(std::chrono::steady_clock::time_point stop_time = std::chrono::steady_clock::now()) {
    if (!active_) return 0;
    const auto total = std::chrono::duration_cast<RESOLUTION>(stop_time - start_time_).count();
    recorder_.Observe(total);
    active_ = false;
    return total;
  }
  ~PrometheusTimeRecorder() { take(); }

 private:
  T& recorder_;
  bool active_;
  std::chrono::steady_clock::time_point start_time_;
};

template <typename T, typename RESOLUTION>
class AccumulativePrometheusTimeRecorder {
 public:
  explicit AccumulativePrometheusTimeRecorder(T& recorder, bool active = true) : recorder_(recorder), active_(active) {}
  void cancel() { active_ = false; }
  void start(std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now(), bool enable = true) {
    if (active_ && enable) start_time_ = start_time;
  }
  void stop(std::chrono::steady_clock::time_point stop_time = std::chrono::steady_clock::now(), bool enable = true) {
    if (active_ && enable) total_ += std::chrono::duration_cast<RESOLUTION>(stop_time - start_time_).count();
  }
  uint64_t take() {
    if (!active_ || total_ == 0) return 0;
    recorder_.Observe(total_);
    active_ = false;
    return total_;
  }

  uint64_t reset() {
    if (!active_ || total_ == 0) return 0;
    recorder_.Observe(total_);
    auto res = total_;
    total_ = 0;
    return res;
  }
  ~AccumulativePrometheusTimeRecorder() { take(); }

 private:
  T& recorder_;
  bool active_;
  uint64_t total_ = 0;
  std::chrono::steady_clock::time_point start_time_;
};

}  // namespace concord::utils
