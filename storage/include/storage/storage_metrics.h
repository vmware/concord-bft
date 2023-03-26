// Copyright 2020 VMware, all rights reserved

#pragma once

#include "util/periodic_call.hpp"
#include "util/Metrics.hpp"

namespace concord {
namespace storage {

/*
 * A metrics class for the in memory storage type.
 * We collect the metrics in the same way we do in the whole project; Once an operation has been executed we count it
 * in a local metric variable and once in a while we update the aggregator.
 * Recall that the in memory db is quite simple and therefor it has only few relevant metrics to collect.
 */
class InMemoryStorageMetrics {
 public:
  concordMetrics::Component metrics_;
  concordMetrics::AtomicCounterHandle keys_reads_;
  concordMetrics::AtomicCounterHandle total_read_bytes_;
  concordMetrics::AtomicCounterHandle keys_writes_;
  concordMetrics::AtomicCounterHandle total_written_bytes_;

 private:
  std::unique_ptr<concord::util::PeriodicCall> update_metrics_ = nullptr;

 public:
  InMemoryStorageMetrics()
      : metrics_("storage_inmemory", std::make_shared<concordMetrics::Aggregator>()),
        keys_reads_(metrics_.RegisterAtomicCounter("storage_inmemory_total_read_keys")),
        total_read_bytes_(metrics_.RegisterAtomicCounter("storage_inmemory_total_read_bytes")),
        keys_writes_(metrics_.RegisterAtomicCounter("storage_inmemory_total_written_keys")),
        total_written_bytes_(metrics_.RegisterAtomicCounter("storage_inmemory_total_written_bytes")) {
    metrics_.Register();
    update_metrics_ = std::make_unique<concord::util::PeriodicCall>([this]() { updateMetrics(); }, 100);
  }
  ~InMemoryStorageMetrics() { update_metrics_.reset(); }
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) { metrics_.SetAggregator(aggregator); }
  void updateMetrics() { metrics_.UpdateAggregator(); }
};

}  // namespace storage
}  // namespace concord