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

#include <atomic>
#include <stdint.h>
#include <map>
#include <vector>
#include <mutex>
#include <memory>
#include <list>
#include <variant>
#include "Statistics.hpp"

namespace concordMetrics {
template <class T>
class BasicGauge;
template <class T>
class BasicCounter;

using Gauge = BasicGauge<uint64_t>;
using Counter = BasicCounter<uint64_t>;
using AtomicGauge = BasicGauge<std::atomic_uint64_t>;
using AtomicCounter = BasicCounter<std::atomic_uint64_t>;

// Forward declarations since Aggregator requires these types.
class Component;
class Values;
class Status;
typedef struct metric_ Metric;

// An aggregator maintains metrics for multiple components. Components
// maintain a handle to the aggregator and update it periodically with
// all their metric values. Therefore, the state of all metrics is eventually
// consistent.
//
// The Aggregator is the type responsible for reporting metrics for the entire
// system. A process should have a single aggregator, and any service
// responsible for reporting system metrics should read it from the aggregator.
class Aggregator {
 public:
  Gauge GetGauge(const std::string& component_name, const std::string& val_name);
  Status GetStatus(const std::string& component_name, const std::string& val_name);
  Counter GetCounter(const std::string& component_name, const std::string& val_name);

  std::list<Metric> CollectGauges();
  std::list<Metric> CollectCounters();
  std::list<Metric> CollectStatuses();
  std::list<Metric> CollectSummaries();
  // Generate a JSON formatted string
  std::string ToJson();

 private:
  void RegisterComponent(Component& component);
  void UpdateValues(const std::string& name, Values&& values);

  std::map<std::string, Component> components_;
  std::mutex lock_;

  friend class Component;
};

// A Gauge is a an integer value that shows the current value of something. It
// can only be varied by directly setting and getting it.
template <class T>
class BasicGauge {
 public:
  explicit BasicGauge(const uint64_t val) : val_(val) {}
  BasicGauge(const BasicGauge& gauge) { val_ = (unsigned long)gauge.val_; }
  BasicGauge& operator=(const BasicGauge& gauge) {
    val_ = (unsigned long)gauge.val_;
    return *this;
  }
  void Inc() { ++val_; }
  void Dec() { --val_; }
  void Set(const uint64_t val) { val_ = val; }
  uint64_t Get() { return val_; }

 private:
  T val_;
};

template <class T>
class BasicCounter {
 public:
  explicit BasicCounter(const uint64_t val) : val_(val) {}
  BasicCounter(const BasicCounter& counter) { val_ = (uint64_t)counter.val_; }
  BasicCounter& operator=(const BasicCounter& counter) {
    val_ = (uint64_t)counter.val_;
    return *this;
  }
  uint64_t Inc(uint64_t val = 1) {
    val_ += val;
    return val_;
  }
  uint64_t Get() { return val_; }

 private:
  T val_;
};

// Status is a text based representation of a value. It's used for things that
// don't have strictly numeric representations, like the current state of the
// BFT or the last message received.
class Status {
 public:
  explicit Status(const std::string& val) : val_(val) {}

  void Set(const std::string& val) { val_ = val; }
  std::string Get() { return val_; }

 private:
  std::string val_;
};

// A generic struct that may represent a counter or a gauge
// the motivation is to eliminate that need to know the exact
// metric name before getting it from the aggregator
struct metric_ {
  std::string component;
  std::string name;
  std::variant<Counter, Gauge, Status, SummaryDescription> value;
};

class Values {
 private:
  std::vector<Gauge> gauges_;
  std::vector<Status> statuses_;
  std::vector<Counter> counters_;
  std::vector<AtomicCounter> atomic_counters_;
  std::vector<AtomicGauge> atomic_gauges_;

  friend class Component;
  friend class Aggregator;
};

// We keep the names of values in separate vecs since they remain constant for
// the life of the program. When we update the component in the aggregator we
// don't have to copy all the names every time. We just have to do it once
// during initialization.
class Names {
 private:
  std::vector<std::string> gauge_names_;
  std::vector<std::string> status_names_;
  std::vector<std::string> counter_names_;
  std::vector<std::string> atomic_counter_names_;
  std::vector<std::string> atomic_gauge_names_;

  friend class Component;
  friend class Aggregator;
};

// A Component stores Values of different types and is updated on the local
// thread. Components are sent to an Aggregator periodically. Components are
// optimized for fast update access.
class Component {
 public:
  // A Handle allows for fast access to the underlying value inside the
  // component via an index operation.
  //
  // Note that the handle cannot live longer than the Component, and should
  // never be used from a separate thread.
  //
  // In concord-bft we expect components to live for the lifetime of the program
  // and handles to be member variables used to update values on the same
  // thread.
  template <typename T>
  class Handle {
   public:
    Handle(std::vector<T>& values, size_t index) : values_(values), index_(index) {}
    T& Get() { return values_[index_]; }

   private:
    std::vector<T>& values_;
    size_t index_;
  };

  Component(const std::string& name, std::shared_ptr<Aggregator> aggregator) : aggregator_(aggregator), name_(name) {}
  std::string Name() { return name_; }

  // Create a Gauge, add it to the component and return a reference to the
  // gauge.
  Handle<Gauge> RegisterGauge(const std::string& name, const uint64_t val);
  Handle<Status> RegisterStatus(const std::string& name, const std::string& val);
  Handle<Counter> RegisterCounter(const std::string& name, const uint64_t val);
  Handle<Counter> RegisterCounter(const std::string& name) { return RegisterCounter(name, 0); }
  Handle<AtomicCounter> RegisterAtomicCounter(const std::string& name, const uint64_t val);
  Handle<AtomicCounter> RegisterAtomicCounter(const std::string& name) { return RegisterAtomicCounter(name, 0); }
  Handle<AtomicGauge> RegisterAtomicGauge(const std::string& name, const uint64_t val);

  std::list<Metric> CollectGauges();
  std::list<Metric> CollectCounters();
  std::list<Metric> CollectStatuses();
  // Register the component with the aggregator.
  // This *must* be done after all values are registered in this component.
  // If registration happens before all registration of the values, then the
  // names will not properly exist in the aggregator, since only values get
  // updated at runtime for performance reasons.
  void Register() {
    if (auto aggregator = aggregator_.lock()) {
      aggregator->RegisterComponent(*this);
    }
  }

  // Update the values in the aggregator
  void UpdateAggregator() {
    Values copy = values_;
    if (auto aggregator = aggregator_.lock()) {
      aggregator->UpdateValues(name_, std::move(copy));
    }
  }

  // Change the aggregator used by the component
  //
  // Register the component with the new aggregator.
  void SetAggregator(std::shared_ptr<Aggregator> aggregator) {
    aggregator_ = aggregator;
    Register();
  }

  // Generate a JSON formatted string
  std::string ToJson();

 private:
  friend class Aggregator;

  void SetValues(Values&& values) { values_ = values; }

  std::weak_ptr<Aggregator> aggregator_;
  std::string name_;

  Names names_;
  Values values_;
};

typedef concordMetrics::Component::Handle<concordMetrics::Gauge> GaugeHandle;
typedef concordMetrics::Component::Handle<concordMetrics::Status> StatusHandle;
typedef concordMetrics::Component::Handle<concordMetrics::Counter> CounterHandle;
typedef concordMetrics::Component::Handle<concordMetrics::AtomicCounter> AtomicCounterHandle;
typedef concordMetrics::Component::Handle<concordMetrics::AtomicGauge> AtomicGaugeHandle;

}  // namespace concordMetrics
