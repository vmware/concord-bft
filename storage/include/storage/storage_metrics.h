// Copyright 2020 VMware, all rights reserved

#pragma once

#include "periodic_call.hpp"
#include "Metrics.hpp"
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
  std::unique_ptr<concord::util::PeriodicCall> update_metrics_ = nullptr;

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
                               ::rocksdb::Tickers::STALL_MICROS,
                               ::rocksdb::Tickers::BLOCK_CACHE_MISS,
                               ::rocksdb::Tickers::BLOCK_CACHE_HIT,
                               ::rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL,
                               ::rocksdb::Tickers::BLOOM_FILTER_FULL_POSITIVE,
                               ::rocksdb::Tickers::BLOOM_FILTER_FULL_TRUE_POSITIVE}) {}

  ~RocksDbStorageMetrics() { update_metrics_.reset(); }
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    rocksdb_comp_.SetAggregator(aggregator);
  }

  void setMetricsDataSources(std::shared_ptr<::rocksdb::SstFileManager> sourceSstFm,
                             std::shared_ptr<::rocksdb::Statistics> sourceStatistics) {
    sstFm = sourceSstFm;
    statistics = sourceStatistics;
    update_metrics_ = std::make_unique<concord::util::PeriodicCall>([this]() { updateMetrics(); }, 100);
  }

  void updateMetrics() {
    if (!sstFm || !statistics) return;
    for (auto& pair : active_tickers_) {
      pair.second.Get().Set(statistics->getTickerCount(pair.first));
    }
    total_db_disk_size_.Get().Set(sstFm->GetTotalSize());
    rocksdb_comp_.UpdateAggregator();
  }
};
#endif

}  // namespace storage
}  // namespace concord