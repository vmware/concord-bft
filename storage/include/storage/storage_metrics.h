// Copyright 2020 VMware, all rights reserved

#pragma once

#include "util/periodic_call.hpp"
#include "util/Metrics.hpp"

namespace concord {
namespace storage {

// Base class for storage metrics, indeferent of the storage type.
// specific storage implamantations (rocksDB being the main one) inherit from this class for metrics reporting
class StorageMetricsBase {
 public:
  concordMetrics::Component metrics_;
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) { metrics_.SetAggregator(aggregator); }

 protected:
  // protected constructor to avoid creation of objects of this class
  StorageMetricsBase(concordMetrics::Component metricsComponent, const size_t updateMetricsIntervalMillisec)
      : metrics_(metricsComponent), update_metrics_interval_millisec_(updateMetricsIntervalMillisec) {}

  virtual ~StorageMetricsBase() {
    if (update_metrics_.get() != nullptr) {
      // in case this method wasn't reset by derived class.
      update_metrics_.reset();
    }
  }

  // the main func the derived class should implement
  virtual void updateMetrics() = 0;

  // periodic update of metrics:
  std::unique_ptr<concord::util::PeriodicCall> update_metrics_;
  const size_t update_metrics_interval_millisec_;
};

}  // namespace storage
}  // namespace concord
