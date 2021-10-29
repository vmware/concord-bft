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

#include "concord_prometheus_metrics.hpp"

#include <prometheus/serializer.h>
#include <prometheus/text_serializer.h>
#include <map>
#include <tuple>
#include "Logger.hpp"

using namespace prometheus;
using namespace concordMetrics;

namespace concord::utils {

prometheus::ClientMetric ConcordBftPrometheusCollector::collect(const std::string& component,
                                                                concordMetrics::Counter& c) const {
  ClientMetric metric;
  metric.counter.value = c.Get();
  metric.label = {{"source", "concordbft"}, {"component", component}};
  return metric;
}

prometheus::ClientMetric ConcordBftPrometheusCollector::collect(const std::string& component,
                                                                concordMetrics::Gauge& g) const {
  ClientMetric metric;
  metric.gauge.value = g.Get();
  metric.label = {{"source", "concordbft"}, {"component", component}};
  return metric;
}

prometheus::ClientMetric ConcordBftPrometheusCollector::collect(const std::string& component,
                                                                concordMetrics::Status& s) const {
  return ClientMetric();
}

std::vector<MetricFamily> ConcordBftPrometheusCollector::Collect() const {
  auto results = std::vector<MetricFamily>{};
  const auto counters = collectCounters();
  results.insert(results.end(), counters.begin(), counters.end());
  auto gauges = collectGauges();
  results.insert(results.end(), std::move_iterator(gauges.begin()), std::move_iterator(gauges.end()));
  auto statuses = collectStatuses();
  results.insert(results.end(), std::move_iterator(statuses.begin()), std::move_iterator(statuses.end()));
  return results;
}

std::vector<MetricFamily> ConcordBftPrometheusCollector::collectCounters() const {
  std::vector<MetricFamily> cf;
  std::map<std::string, MetricFamily> metricsMap;
  for (auto& c : aggregator_->CollectCounters()) {
    if (metricsMap.find(c.name) == metricsMap.end()) {
      metricsMap[c.name] =
          MetricFamily{getMetricName(c.name), c.name + " - a concordbft metric", MetricType::Counter, {}};
    }
    metricsMap[c.name].metric.emplace_back(collect(c.component, std::get<concordMetrics::Counter>(c.value)));
  }
  cf.reserve(metricsMap.size());
  for (auto& it : metricsMap) {
    cf.emplace_back(std::move(it.second));
  }
  return cf;
}

std::vector<MetricFamily> ConcordBftPrometheusCollector::collectGauges() const {
  std::vector<MetricFamily> gf;
  std::map<std::string, MetricFamily> metricsMap;
  for (auto& g : aggregator_->CollectGauges()) {
    if (metricsMap.find(g.name) == metricsMap.end()) {
      metricsMap[g.name] =
          MetricFamily{getMetricName(g.name), g.name + " - a concordbft metric", MetricType::Gauge, {}};
    }
    metricsMap[g.name].metric.emplace_back(collect(g.component, std::get<concordMetrics::Gauge>(g.value)));
  }
  gf.reserve(metricsMap.size());
  for (auto& it : metricsMap) {
    gf.emplace_back(std::move(it.second));
  }
  return gf;
}

std::vector<MetricFamily> ConcordBftPrometheusCollector::collectStatuses() const { return {}; }
std::string ConcordBftPrometheusCollector::getMetricName(const std::string& origName) const {
  return metricNamePrefix_ + origName;
}

PrometheusRegistry::PrometheusRegistry(const std::string& bindAddress,
                                       uint64_t metricsDumpInterval,
                                       const bool& metricsDumpEnabled)
    : exposer_(std::make_unique<prometheus::Exposer>(bindAddress, 1)),
      counters_custom_collector_(std::make_shared<ConcordCustomCollector<prometheus::Counter>>(
          std::chrono::seconds(metricsDumpInterval), metricsDumpEnabled)),
      gauges_custom_collector_(std::make_shared<ConcordCustomCollector<prometheus::Gauge>>(
          std::chrono::seconds(metricsDumpInterval), metricsDumpEnabled)),
      histogram_custom_collector_(std::make_shared<ConcordCustomCollector<prometheus::Histogram>>(
          std::chrono::seconds(metricsDumpInterval), metricsDumpEnabled)),
      summary_custom_collector_(std::make_shared<ConcordCustomCollector<prometheus::Summary>>(
          std::chrono::seconds(metricsDumpInterval), metricsDumpEnabled)) {
  exposer_->RegisterCollectable(counters_custom_collector_);
  exposer_->RegisterCollectable(gauges_custom_collector_);
  exposer_->RegisterCollectable(histogram_custom_collector_);
  exposer_->RegisterCollectable(summary_custom_collector_);
}

PrometheusRegistry::PrometheusRegistry(const std::string& bindAddress)
    : PrometheusRegistry(bindAddress, defaultMetricsDumpInterval, true) {}

void PrometheusRegistry::scrapeRegistry(std::shared_ptr<prometheus::Collectable> registry) {
  if (!exposer_) return;
  exposer_->RegisterCollectable(registry);
}

prometheus::Family<prometheus::Counter>& PrometheusRegistry::createCounterFamily(
    const std::string& name, const std::string& help, const std::map<std::string, std::string>& labels) {
  return counters_custom_collector_->createFamily(name, help, labels);
}

prometheus::Counter& PrometheusRegistry::createCounter(prometheus::Family<prometheus::Counter>& source,
                                                       const std::map<std::string, std::string>& labels) {
  return source.Add(labels);
}

prometheus::Counter& PrometheusRegistry::createCounter(const std::string& name,
                                                       const std::string& help,
                                                       const std::map<std::string, std::string>& labels) {
  return createCounter(createCounterFamily(name, help, labels), {});
}

prometheus::Family<prometheus::Gauge>& PrometheusRegistry::createGaugeFamily(
    const std::string& name, const std::string& help, const std::map<std::string, std::string>& labels) {
  return gauges_custom_collector_->createFamily(name, help, labels);
}
prometheus::Gauge& PrometheusRegistry::createGauge(prometheus::Family<prometheus::Gauge>& source,
                                                   const std::map<std::string, std::string>& labels) {
  return source.Add(labels);
}
prometheus::Gauge& PrometheusRegistry::createGauge(const std::string& name,
                                                   const std::string& help,
                                                   const std::map<std::string, std::string>& labels) {
  return createGauge(createGaugeFamily(name, help, labels), {});
}

prometheus::Family<prometheus::Histogram>& PrometheusRegistry::createHistogramFamily(
    const std::string& name, const std::string& help, const std::map<std::string, std::string>& labels) {
  return histogram_custom_collector_->createFamily(name, help, labels);
}
prometheus::Histogram& PrometheusRegistry::createHistogram(prometheus::Family<prometheus::Histogram>& source,
                                                           const std::map<std::string, std::string>& labels,
                                                           const std::vector<double>& buckets) {
  return source.Add(labels, buckets);
}
prometheus::Histogram& PrometheusRegistry::createHistogram(const std::string& name,
                                                           const std::string& help,
                                                           const std::map<std::string, std::string>& labels,
                                                           const std::vector<double>& buckets) {
  return createHistogram(createHistogramFamily(name, help, labels), {}, buckets);
}
prometheus::Family<prometheus::Summary>& PrometheusRegistry::createSummaryFamily(
    const std::string& name, const std::string& help, const std::map<std::string, std::string>& labels) {
  return summary_custom_collector_->createFamily(name, help, labels);
}
prometheus::Summary& PrometheusRegistry::createSummary(prometheus::Family<prometheus::Summary>& source,
                                                       const std::map<std::string, std::string>& labels,
                                                       const prometheus::Summary::Quantiles& quantiles,
                                                       std::chrono::milliseconds max_age,
                                                       int age_buckets) {
  return source.Add(labels, quantiles, max_age, age_buckets);
}

prometheus::Summary& PrometheusRegistry::createSummary(const std::string& name,
                                                       const std::string& help,
                                                       const std::map<std::string, std::string>& labels,
                                                       const prometheus::Summary::Quantiles& quantiles,
                                                       std::chrono::milliseconds max_age,
                                                       int age_buckets) {
  return createSummary(createSummaryFamily(name, help, labels), {}, quantiles, max_age, age_buckets);
}
PrometheusRegistry::PrometheusRegistry()
    : counters_custom_collector_(
          std::make_shared<ConcordCustomCollector<prometheus::Counter>>(std::chrono::seconds(1), true)),
      gauges_custom_collector_(
          std::make_shared<ConcordCustomCollector<prometheus::Gauge>>(std::chrono::seconds(1), true)),
      histogram_custom_collector_(
          std::make_shared<ConcordCustomCollector<prometheus::Histogram>>(std::chrono::seconds(1), true)),
      summary_custom_collector_(
          std::make_shared<ConcordCustomCollector<prometheus::Summary>>(std::chrono::seconds(1), true)) {}

template <typename T>
prometheus::Family<T>& ConcordCustomCollector<T>::createFamily(const std::string& name,
                                                               const std::string& help,
                                                               const std::map<std::string, std::string>& labels) {
  std::lock_guard<std::mutex> lock(lock_);
  return *(*metrics_.insert(metrics_.end(), std::make_shared<prometheus::Family<T>>(name, help, labels)));
}

template <typename T>
std::vector<prometheus::MetricFamily> ConcordCustomCollector<T>::Collect() const {
  std::lock_guard<std::mutex> lock(lock_);
  std::vector<prometheus::MetricFamily> res;
  for (const std::shared_ptr<Family<T>>& f : metrics_) {
    const auto& tmp = f->Collect();
    res.insert(res.end(), std::move_iterator(tmp.begin()), std::move_iterator(tmp.end()));
  }
  if (!res.empty()) {
    auto currTime =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
    if (dump_metrics_enabled_ && (currTime - last_dump_time_ >= dumpInterval_)) {
      last_dump_time_ = currTime;
      LOG_INFO(logger_, "prometheus metrics dump: " + prometheus::TextSerializer().Serialize(res));
    }
  }
  return res;
}
}  // namespace concord::utils
