// Copyright 2020 VMware, all rights reserved

#pragma once

#include "Metrics.hpp"

namespace concord {
namespace storage {

class StorageMetrics {
 public:
  concordMetrics::Component metrics_;
  concordMetrics::CounterHandle keys_reads_;
  concordMetrics::CounterHandle total_read_bytes_;
  concordMetrics::CounterHandle keys_writes_;
  concordMetrics::CounterHandle total_written_bytes_;
  concordMetrics::GaugeHandle total_db_disk_size_;

  /*
   * TODO: more metrics to add?
   * 1. #deletes
   */
  StorageMetrics()
      : metrics_("storage", std::make_shared<concordMetrics::Aggregator>()),
        keys_reads_(metrics_.RegisterCounter("storage_total_read_keys")),
        total_read_bytes_(metrics_.RegisterCounter("storage_total_read_bytes")),
        keys_writes_(metrics_.RegisterCounter("storage_total_written_keys")),
        total_written_bytes_(metrics_.RegisterCounter("storage_total_written_bytes")),
        total_db_disk_size_(metrics_.RegisterGauge("total_db_disk_size", 0)) {
    metrics_.Register();
  }
};

}  // namespace storage
}  // namespace concord