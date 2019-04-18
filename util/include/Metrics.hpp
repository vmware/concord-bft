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

#ifndef CONCORD_BFT_METRICS_HPP
#define CONCORD_BFT_METRICS_HPP

#include <stdint.h>
#include <map>
#include <vector>
#include <mutex>
#include <memory>

namespace concordMetrics {

// Forward declarations since Aggregator requires these types.
class Component;
class Values;
class Gauge;
class Status;
class Counter;

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
  Gauge GetGauge(const std::string& component_name,
                 const std::string& val_name);
  Status GetStatus(const std::string& component_name,
                   const std::string& val_name);
  Counter GetCounter(const std::string& component_name,
                     const std::string& val_name);

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
class Gauge {
 public:
  explicit Gauge(const uint64_t val) : val_(val) {}

  void Set(const uint64_t val) { val_ = val; }
  uint64_t Get() { return val_; }

 private:
  uint64_t val_;
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

class Counter {
 public:
  explicit Counter(const uint64_t val) : val_(val) {}

  // Increment the counter and return the value after incrementing.
  uint64_t Inc() { return ++val_; }

  uint64_t Get() { return val_; }

 private:
  uint64_t val_;
};

class Values {
 private:
  std::vector<Gauge> gauges_;
  std::vector<Status> statuses_;
  std::vector<Counter> counters_;

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
    Handle(std::vector<T>& values, size_t index)
        : values_(values), index_(index) {}
    T& Get() { return values_[index_]; }

   private:
    std::vector<T>& values_;
    size_t index_;
  };

  Component(const std::string& name, std::shared_ptr<Aggregator> aggregator)
      : aggregator_(aggregator), name_(name) {}
  std::string Name() { return name_; }

  // Create a Gauge, add it to the component and return a reference to the
  // gauge.
  Handle<Gauge> RegisterGauge(const std::string& name, const uint64_t val);
  Handle<Status> RegisterStatus(const std::string& name,
                                const std::string& val);
  Handle<Counter> RegisterCounter(const std::string& name, const uint64_t val);

  // Register the component with the aggregator.
  // This *must* be done after all values are registered in this component.
  // If registration happens before all registration of the values, then the
  // names will not properly exist in the aggregator, since only values get
  // updated at runtime for performance reasons.
  void Register() { aggregator_->RegisterComponent(*this); }

  void UpdateAggregator() {
    Values copy = values_;
    aggregator_->UpdateValues(name_, std::move(copy));
  }

  // Generate a JSON formatted string
  std::string ToJson();

 private:
  friend class Aggregator;

  void SetValues(Values&& values) { values_ = values; }

  std::shared_ptr<Aggregator> aggregator_;
  std::string name_;

  Names names_;
  Values values_;
};

}  // namespace concordMetrics

#endif  // CONCORD_BFT_METRICS_HPP
