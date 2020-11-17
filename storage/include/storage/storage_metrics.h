// Copyright 2020 VMware, all rights reserved

#pragma once

#include "Metrics.hpp"
#include <mutex>
#ifdef USE_ROCKSDB
#include <rocksdb/statistics.h>
#include <unordered_map>
#include <vector>
#include <rocksdb/sst_file_manager.h>
#endif

namespace concord {
namespace storage {

/*
 * A metrics class for the in memory storage type.
 * We collect the metrics in the same way we do in the whole project; Once an operation has been executed we count it
 * in a local metric variable and once in a while we update the aggregator.
 * Recall that the in memory db is quite simple and therefor it has only few relevant metrics to collect.
 */
class InMemoryStorageMetrics {
  std::chrono::seconds metrics_update_interval_ = std::chrono::seconds(5);  // TODO: move to configuration
  std::chrono::steady_clock::time_point last_metrics_update_ = std::chrono::steady_clock::now();
  std::mutex lock_;

 public:
  concordMetrics::Component metrics_;
  concordMetrics::AtomicCounterHandle keys_reads_;
  concordMetrics::AtomicCounterHandle total_read_bytes_;
  concordMetrics::AtomicCounterHandle keys_writes_;
  concordMetrics::AtomicCounterHandle total_written_bytes_;

  InMemoryStorageMetrics()
      : metrics_("storage_inmemory", std::make_shared<concordMetrics::Aggregator>()),
        keys_reads_(metrics_.RegisterAtomicCounter("storage_inmemory_total_read_keys")),
        total_read_bytes_(metrics_.RegisterAtomicCounter("storage_inmemory_total_read_bytes")),
        keys_writes_(metrics_.RegisterAtomicCounter("storage_inmemory_total_written_keys")),
        total_written_bytes_(metrics_.RegisterAtomicCounter("storage_inmemory_total_written_bytes")) {
    metrics_.Register();
  }

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) { metrics_.SetAggregator(aggregator); }

  void tryToUpdateMetrics() {
    std::lock_guard<std::mutex> lock(lock_);
    if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - last_metrics_update_) >
        metrics_update_interval_) {
      metrics_.UpdateAggregator();
      last_metrics_update_ = std::chrono::steady_clock::now();
    }
  }
};
#ifdef USE_ROCKSDB
/*
 * This is a metric class for rocksdb storage type.
 * As rocksDB already contains many informative metrics, we would like to reuse them and expose them using concord
 * metrics framework. Alas, reading all rocksDB metrics on each operation may harm performance. Therefor, once in while,
 * we read rocksdb metrics, update concord metrics as well as the aggregator. Notice, that in this case there is no need
 * to collect the metrics after each operation but just collect once in a while rocksdb metrics and publish them.
 *
 * In order to enable flexibility (and rocksdb metrics configuration in the future), we dynamically create concord-bft
 * metrics w.r.t rocksdb configuration list. Even so, as we collect the metrics once in a while (and not on each single
 * operation) the overhead of that approach is negligible.
 */
class RocksDbStorageMetrics {
  concordMetrics::Component rocksdb_comp_;
  std::unordered_map<::rocksdb::Tickers, concordMetrics::AtomicGaugeHandle> active_tickers_;
  concordMetrics::AtomicGaugeHandle total_db_disk_size_;

  std::shared_ptr<::rocksdb::SstFileManager> sstFm;
  std::shared_ptr<::rocksdb::Statistics> statistics;

  std::chrono::seconds metrics_update_interval_ = std::chrono::seconds(5);  // TODO: move to configuration
  std::chrono::steady_clock::time_point last_metrics_update_ = std::chrono::steady_clock::now();
  std::mutex lock_;

 public:
  RocksDbStorageMetrics(const std::vector<::rocksdb::Tickers>& tickers)
      : rocksdb_comp_("storage_rocksdb", std::make_shared<concordMetrics::Aggregator>()),
        total_db_disk_size_(rocksdb_comp_.RegisterAtomicGauge("storage_rocksdb_total_db_disk_size", 0)) {
    for (const auto& pair : ::rocksdb::TickersNameMap) {
      if (std::find(tickers.begin(), tickers.end(), pair.first) != tickers.end()) {
        auto metric_suffix = pair.second;
        std::replace(metric_suffix.begin(), metric_suffix.end(), '.', '_');
        active_tickers_.emplace(pair.first, rocksdb_comp_.RegisterAtomicGauge("storage_" + metric_suffix, 0));
      }
    }
    rocksdb_comp_.Register();
  }

  /*
   * For now, we have a hardcoded default metrics configuration list.
   * In the future we will add a rocksdb configuration file to enable flexibility.
   */
  RocksDbStorageMetrics()
      : RocksDbStorageMetrics({::rocksdb::Tickers::NUMBER_KEYS_WRITTEN,
                               ::rocksdb::Tickers::NUMBER_KEYS_READ,
                               ::rocksdb::Tickers::BYTES_WRITTEN,
                               ::rocksdb::Tickers::BYTES_READ,
                               ::rocksdb::Tickers::COMPACT_READ_BYTES,
                               ::rocksdb::Tickers::COMPACT_WRITE_BYTES,
                               ::rocksdb::Tickers::FLUSH_WRITE_BYTES,
                               ::rocksdb::Tickers::STALL_MICROS}) {}

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    rocksdb_comp_.SetAggregator(aggregator);
  }

  void setMetricsDataSources(std::shared_ptr<::rocksdb::SstFileManager> sourceSstFm,
                             std::shared_ptr<::rocksdb::Statistics> sourceStatistics) {
    sstFm = sourceSstFm;
    statistics = sourceStatistics;
  }

  void tryToUpdateMetrics() {
    std::lock_guard<std::mutex> lock(lock_);
    if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - last_metrics_update_) >
        metrics_update_interval_) {
      for (auto& pair : active_tickers_) {
        pair.second.Get().Set(statistics->getTickerCount(pair.first));
      }
      total_db_disk_size_.Get().Set(sstFm->GetTotalSize());
      rocksdb_comp_.UpdateAggregator();
      last_metrics_update_ = std::chrono::steady_clock::now();
    }
  }
};
#endif

}  // namespace storage
}  // namespace concord