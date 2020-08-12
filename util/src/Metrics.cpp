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

#include "Metrics.hpp"
#include <stdexcept>
#include <sstream>

using namespace std;

namespace concordMetrics {

const char* const kGaugeName = "gauge";
const char* const kStatusName = "status";
const char* const kCounterName = "counter";

template <typename T>
T FindValue(const char* const val_type, const string& val_name, const vector<string>& names, const vector<T>& values) {
  for (size_t i = 0; i < names.size(); i++) {
    if (names[i] == val_name) {
      return values[i];
    }
  }
  ostringstream oss;
  oss << "Invalid " << val_type << " name: " << val_name;
  throw invalid_argument(oss.str());
}

Component::Handle<Gauge> Component::RegisterGauge(const string& name, const uint64_t val) {
  names_.gauge_names_.emplace_back(name);
  values_.gauges_.emplace_back(Gauge(val));
  return Component::Handle<Gauge>(values_.gauges_, values_.gauges_.size() - 1);
}

Component::Handle<Status> Component::RegisterStatus(const string& name, const string& val) {
  names_.status_names_.emplace_back(name);
  values_.statuses_.emplace_back(Status(val));
  return Component::Handle<Status>(values_.statuses_, values_.statuses_.size() - 1);
}

Component::Handle<Counter> Component::RegisterCounter(const string& name, const uint64_t val) {
  names_.counter_names_.emplace_back(name);
  values_.counters_.emplace_back(Counter(val));
  return Component::Handle<Counter>(values_.counters_, values_.counters_.size() - 1);
}

Component::Handle<Summary> Component::RegisterSummary(const std::string& name,
                                                      const Summary::InitQuantiles& quantiles) {
  names_.summary_names_.emplace_back(name);
  values_.summaries_.emplace_back(Summary(quantiles));
  return Component::Handle<Summary>(values_.summaries_, values_.summaries_.size() - 1);
}

std::list<Metric> Component::CollectGauges() {
  std::list<Metric> ret;
  for (size_t i = 0; i < names_.gauge_names_.size(); i++) {
    ret.emplace_back(Metric{name_, names_.gauge_names_[i], values_.gauges_[i]});
  }
  return ret;
}

std::list<Metric> Component::CollectCounters() {
  std::list<Metric> ret;
  for (size_t i = 0; i < names_.counter_names_.size(); i++) {
    ret.emplace_back(Metric{name_, names_.counter_names_[i], values_.counters_[i]});
  }
  return ret;
}

std::list<Metric> Component::CollectStatuses() {
  std::list<Metric> ret;
  for (size_t i = 0; i < names_.status_names_.size(); i++) {
    ret.emplace_back(Metric{name_, names_.status_names_[i], values_.statuses_[i]});
  }
  return ret;
}

std::list<Metric> Component::CollectSummaries() {
  std::list<Metric> ret;
  for (size_t i = 0; i < names_.summary_names_.size(); i++) {
    ret.emplace_back(Metric{name_, names_.summary_names_[i], values_.summaries_[i].Collect()});
  }
  return ret;
}

void Aggregator::RegisterComponent(Component& component) {
  std::lock_guard<std::mutex> lock(lock_);
  components_.insert(make_pair(component.Name(), component));
}

// Throws if the component doesn't exist.
// This is only called from the component itself so it will never actually
// throw.
void Aggregator::UpdateValues(const string& name, Values&& values) {
  std::lock_guard<std::mutex> lock(lock_);
  components_.at(name).SetValues(std::move(values));
}

Gauge Aggregator::GetGauge(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  auto& component = components_.at(component_name);
  return FindValue(kGaugeName, val_name, component.names_.gauge_names_, component.values_.gauges_);
}

Status Aggregator::GetStatus(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  auto& component = components_.at(component_name);
  return FindValue(kStatusName, val_name, component.names_.status_names_, component.values_.statuses_);
}

Counter Aggregator::GetCounter(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  auto& component = components_.at(component_name);
  return FindValue(kCounterName, val_name, component.names_.counter_names_, component.values_.counters_);
}

Summary Aggregator::GetSummary(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  auto& component = components_.at(component_name);
  return FindValue(kCounterName, val_name, component.names_.summary_names_, component.values_.summaries_);
}

// Generate a JSON string of all aggregated components. To save space we don't
// add any newline characters.
std::string Aggregator::ToJson() {
  ostringstream oss;
  std::lock_guard<std::mutex> lock(lock_);

  // Add the object opening
  oss << "{\"Components\":[";

  // Add all the components
  for (auto it = components_.begin(); it != components_.end(); ++it) {
    // Add a comma between every component
    if (it != components_.begin()) {
      oss << ",";
    }
    oss << it->second.ToJson();
  }

  // Add the object end
  oss << "]}";

  return oss.str();
}
std::list<Metric> Aggregator::CollectGauges() {
  std::lock_guard<std::mutex> lock(lock_);
  std::list<Metric> ret;
  for (auto& comp : components_) {
    const auto& gauges = comp.second.CollectGauges();
    ret.insert(ret.end(), gauges.begin(), gauges.end());
  }
  return ret;
}
std::list<Metric> Aggregator::CollectCounters() {
  std::lock_guard<std::mutex> lock(lock_);
  std::list<Metric> ret;
  for (auto& comp : components_) {
    const auto& counters = comp.second.CollectCounters();
    ret.insert(ret.end(), counters.begin(), counters.end());
  }
  return ret;
}

std::list<Metric> Aggregator::CollectStatuses() {
  std::lock_guard<std::mutex> lock(lock_);
  std::list<Metric> ret;
  for (auto& comp : components_) {
    const auto& statuses = comp.second.CollectStatuses();
    ret.insert(ret.end(), statuses.begin(), statuses.end());
  }
  return ret;
}

std::list<Metric> Aggregator::CollectSummaries() {
  std::lock_guard<std::mutex> lock(lock_);
  std::list<Metric> ret;
  for (auto& comp : components_) {
    const auto& summaries = comp.second.CollectSummaries();
    ret.insert(ret.end(), summaries.begin(), summaries.end());
  }
  return ret;
}

// Generate a JSON string of the component. To save space we don't add any
// newline characters.
std::string Component::ToJson() {
  ostringstream oss;

  // Add the object opening and component name
  oss << "{\"Name\":\"" << name_ << "\",";

  // Add any gauges
  oss << "\"Gauges\":{";

  for (size_t i = 0; i < names_.gauge_names_.size(); i++) {
    if (i != 0) {
      oss << ",";
    }
    oss << "\"" << names_.gauge_names_[i] << "\":" << values_.gauges_[i].Get() << "";
  }

  // End gauges
  oss << "},";

  // Add any status
  oss << "\"Statuses\":{";

  for (size_t i = 0; i < names_.status_names_.size(); i++) {
    if (i != 0) {
      oss << ",";
    }
    oss << "\"" << names_.status_names_[i] << "\":"
        << "\"" << values_.statuses_[i].Get() << "\"";
  }

  // End status
  oss << "},";

  // Add any counters
  oss << "\"Counters\":{";

  for (size_t i = 0; i < names_.counter_names_.size(); i++) {
    if (i != 0) {
      oss << ",";
    }
    oss << "\"" << names_.counter_names_[i] << "\":" << values_.counters_[i].Get() << "";
  }

  // End counters
  oss << "},";

  // Add any Summary
  oss << "\"Summaries\":{";

  for (size_t i = 0; i < names_.summary_names_.size(); i++) {
    if (i != 0) {
      oss << ",";
    }
    oss << "\"" << names_.summary_names_[i] << "\":" << values_.summaries_[i].ToJson() << "";
  }

  // End counters
  oss << "}";

  // End component
  oss << "}";

  return oss.str();
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
Summary::SummaryDescription Summary::Collect() {
  auto data = summary_->Collect();
  std::vector<Quantile> quantile;
  for (auto& q : data.summary.quantile) {
    quantile.emplace_back(Quantile(q.quantile, q.value));
  }
  return {data.summary.sample_count, data.summary.sample_sum, quantile};
}
void Summary::Observe(double value) { summary_->Observe(value); }
Summary::Summary(const Summary::InitQuantiles& quantiles) {
  prometheus::Summary::Quantiles quantiles_;
  for (auto& q : quantiles) {
    quantiles_.emplace_back(prometheus::detail::CKMSQuantiles::Quantile(std::get<0>(q), std::get<1>(q)));
  }
  summary_ = std::make_shared<prometheus::Summary>(quantiles_);
}
}  // namespace concordMetrics
