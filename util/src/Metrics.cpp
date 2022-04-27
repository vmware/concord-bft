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
#include "kvstream.h"
#include <stdexcept>
#include <sstream>
#include <algorithm>

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

/******************************** Class Component ********************************/

Component::Handle<Gauge> Component::RegisterGauge(const string& name, const uint64_t val) {
  names_.gauge_names_.emplace_back(name);
  values_.gauges_.emplace_back(Gauge(val));
  return Component::Handle<Gauge>(values_.gauges_, values_.gauges_.size() - 1, metricsEnabled_);
}

Component::Handle<Gauge> Component::RegisterGauge(const string& name,
                                                  const uint64_t val,
                                                  const std::unordered_map<std::string, std::string>& tag_map) {
  names_.gauge_names_.emplace_back(name);
  tags_.gauge_tags_.emplace_back(tag_map);
  values_.gauges_.emplace_back(Gauge(val));
  return Component::Handle<Gauge>(values_.gauges_, values_.gauges_.size() - 1, metricsEnabled_);
}

Component::Handle<Status> Component::RegisterStatus(const string& name, const string& val) {
  names_.status_names_.emplace_back(name);
  values_.statuses_.emplace_back(Status(val));
  return Component::Handle<Status>(values_.statuses_, values_.statuses_.size() - 1, metricsEnabled_);
}

Component::Handle<Counter> Component::RegisterCounter(const string& name, const uint64_t val) {
  names_.counter_names_.emplace_back(name);
  values_.counters_.emplace_back(Counter(val));
  return Component::Handle<Counter>(values_.counters_, values_.counters_.size() - 1, metricsEnabled_);
}

Component::Handle<Counter> Component::RegisterCounter(const string& name,
                                                      const uint64_t val,
                                                      const std::unordered_map<std::string, std::string>& tag_map) {
  names_.counter_names_.emplace_back(name);
  tags_.counter_tags_.emplace_back(tag_map);
  values_.counters_.emplace_back(Counter(val));
  return Component::Handle<Counter>(values_.counters_, values_.counters_.size() - 1, metricsEnabled_);
}

Component::Handle<AtomicCounter> Component::RegisterAtomicCounter(const std::string& name, const uint64_t val) {
  names_.atomic_counter_names_.emplace_back(name);
  values_.atomic_counters_.emplace_back(AtomicCounter(val));
  return Component::Handle<AtomicCounter>(
      values_.atomic_counters_, values_.atomic_counters_.size() - 1, metricsEnabled_);
}

Component::Handle<AtomicGauge> Component::RegisterAtomicGauge(const std::string& name, uint64_t val) {
  names_.atomic_gauge_names_.emplace_back(name);
  values_.atomic_gauges_.emplace_back(AtomicGauge(val));
  return Component::Handle<AtomicGauge>(values_.atomic_gauges_, values_.atomic_gauges_.size() - 1, metricsEnabled_);
}

std::list<Metric> Component::CollectGauges() {
  if (!metricsEnabled_) return list<Metric>();
  std::list<Metric> ret;
  for (size_t i = 0; i < names_.gauge_names_.size(); i++) {
    if (tags_.gauge_tags_.size() > i) {
      ret.emplace_back(Metric{name_, names_.gauge_names_[i], values_.gauges_[i], tags_.gauge_tags_[i]});
    } else {
      ret.emplace_back(Metric{name_, names_.gauge_names_[i], values_.gauges_[i]});
    }
  }
  for (std::size_t i = 0; i < names_.atomic_gauge_names_.size(); i++) {
    ret.emplace_back(Metric{name_, names_.atomic_gauge_names_[i], Gauge(values_.atomic_gauges_[i].Get())});
  }
  return ret;
}

std::list<Metric> Component::CollectCounters() {
  if (!metricsEnabled_) return list<Metric>();
  std::list<Metric> ret;
  for (size_t i = 0; i < names_.counter_names_.size(); i++) {
    if (tags_.counter_tags_.size() > i) {
      ret.emplace_back(Metric{name_, names_.counter_names_[i], values_.counters_[i], tags_.counter_tags_[i]});
    } else {
      ret.emplace_back(Metric{name_, names_.counter_names_[i], values_.counters_[i]});
    }
  }
  for (std::size_t i = 0; i < names_.atomic_counter_names_.size(); i++) {
    ret.emplace_back(Metric{name_, names_.atomic_counter_names_[i], Counter(values_.atomic_counters_[i].Get())});
  }
  return ret;
}

std::list<Metric> Component::CollectStatuses() {
  if (!metricsEnabled_) return list<Metric>();
  std::list<Metric> ret;
  for (size_t i = 0; i < names_.status_names_.size(); i++) {
    ret.emplace_back(Metric{name_, names_.status_names_[i], values_.statuses_[i]});
  }
  return ret;
}

void Component::UpdateAggregator() {
  if (!metricsEnabled_) return;
  Values copy = values_;
  if (auto aggregator = aggregator_.lock()) aggregator->UpdateValues(name_, std::move(copy));
}

// Generate a JSON string of the component. To save space we don't add any newline characters.
std::string Component::ToJson() {
  if (!metricsEnabled_) return "";
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

  for (size_t i = 0; i < names_.atomic_counter_names_.size(); i++) {
    if (i != 0 || names_.counter_names_.size() > 0) {
      oss << ",";
    }
    oss << "\"" << names_.atomic_counter_names_[i] << "\":" << values_.atomic_counters_[i].Get() << "";
  }

  // End counters
  oss << "}";

  // End component
  oss << "}";

  return oss.str();
}

/******************************** Class Aggregator ********************************/

void Aggregator::RegisterComponent(Component& component) {
  std::lock_guard<std::mutex> lock(lock_);
  components_.insert(make_pair(component.Name(), component));
}

// Throws if the component doesn't exist.
// This is only called from the component itself, so it will never actually throw.
void Aggregator::UpdateValues(const string& name, Values&& values) {
  if (!metricsEnabled_) return;
  std::lock_guard<std::mutex> lock(lock_);
  try {
    components_.at(name).SetValues(std::move(values));
  } catch (const std::out_of_range& e) {
    throw std::out_of_range("components_.at() failed for name = " + name);
  }
}

Gauge Aggregator::GetGauge(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  try {
    auto& component = components_.at(component_name);
    auto& gauges = component.names_.gauge_names_;
    if (std::find(gauges.begin(), gauges.end(), val_name) != gauges.end()) {
      return FindValue(kGaugeName, val_name, component.names_.gauge_names_, component.values_.gauges_);
    }
    auto atomic_gauge =
        FindValue(kCounterName, val_name, component.names_.atomic_gauge_names_, component.values_.atomic_gauges_);
    return Gauge(atomic_gauge.Get());
  } catch (const std::out_of_range& e) {
    throw std::out_of_range("components_.at() failed for component_name = " + component_name);
  }
}

Status Aggregator::GetStatus(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  try {
    auto& component = components_.at(component_name);
    return FindValue(kStatusName, val_name, component.names_.status_names_, component.values_.statuses_);
  } catch (const std::out_of_range& e) {
    throw std::out_of_range("components_.at() failed for component_name = " + component_name);
  }
}

Counter Aggregator::GetCounter(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  try {
    auto& component = components_.at(component_name);
    auto& counters = component.names_.counter_names_;
    if (std::find(counters.begin(), counters.end(), val_name) != counters.end()) {
      return FindValue(kCounterName, val_name, component.names_.counter_names_, component.values_.counters_);
    }
    auto atomic_counter =
        FindValue(kCounterName, val_name, component.names_.atomic_counter_names_, component.values_.atomic_counters_);
    return Counter(atomic_counter.Get());
  } catch (const std::out_of_range& e) {
    throw std::out_of_range("components_.at() failed for component_name = " + component_name);
  }
}

// Generate a JSON string of all aggregated components. To save space we don't
// add any newline characters.
std::string Aggregator::ToJson() {
  if (!metricsEnabled_) return "";
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
  if (!metricsEnabled_) return std::list<Metric>();
  std::lock_guard<std::mutex> lock(lock_);
  std::list<Metric> ret;
  for (auto& comp : components_) {
    const auto& gauges = comp.second.CollectGauges();
    ret.insert(ret.end(), gauges.begin(), gauges.end());
  }
  return ret;
}
std::list<Metric> Aggregator::CollectCounters() {
  if (!metricsEnabled_) return std::list<Metric>();
  std::lock_guard<std::mutex> lock(lock_);
  std::list<Metric> ret;
  for (auto& comp : components_) {
    const auto& counters = comp.second.CollectCounters();
    ret.insert(ret.end(), counters.begin(), counters.end());
  }
  return ret;
}

std::list<Metric> Aggregator::CollectStatuses() {
  if (!metricsEnabled_) return std::list<Metric>();
  std::lock_guard<std::mutex> lock(lock_);
  std::list<Metric> ret;
  for (auto& comp : components_) {
    const auto& statuses = comp.second.CollectStatuses();
    ret.insert(ret.end(), statuses.begin(), statuses.end());
  }
  return ret;
}

}  // namespace concordMetrics
