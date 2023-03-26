// Copyright 2020 VMware, all rights reserved

#pragma once

#include "util/periodic_call.hpp"
#include "util/Metrics.hpp"
#ifdef USE_ROCKSDB
#include <rocksdb/statistics.h>
#include <unordered_map>
#include <vector>
#include <rocksdb/sst_file_manager.h>

#include <rocksdb/utilities/memory_util.h>
#include "rocksdb/client.h"
#include <unordered_set>

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
  static constexpr size_t update_metrics_interval_millisec = 100;  // every 100msec
  static constexpr size_t update_mem_usage_metrics_factor =
      600;  // update_metrics_interval_millisec * 600 = every 1 minute

 public:
  concordMetrics::Component metrics_;
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) { metrics_.SetAggregator(aggregator); }

  RocksDbStorageMetrics(const std::vector<::rocksdb::Tickers> &tickers,
                        const concord::storage::rocksdb::Client &owningClient)
      : metrics_({"storage_rocksdb", std::make_shared<concordMetrics::Aggregator>()}),
        owning_client_(owningClient),
        total_db_disk_size_(metrics_.RegisterAtomicGauge("storage_rocksdb_total_db_disk_size", 0)),
        all_mem_tables_ram_usage_(metrics_.RegisterAtomicGauge("storage_rocksdb_mem_tables_ram_usage", 0)),
        all_unflushed_mem_tables_ram_usage_(
            metrics_.RegisterAtomicGauge("storage_rocksdb_unflushed_mem_tables_ram_usage", 0)),
        block_caches_ram_usage_(
            metrics_.RegisterAtomicGauge("storage_rocksdb_column_families_block_cache_ram_usage", 0)),
        indexes_and_filters_ram_usage_(metrics_.RegisterAtomicGauge("storage_rocksdb_indexes_and_filter_ram_usage", 0)),
        rocksdb_total_ram_usage_(metrics_.RegisterAtomicGauge("storage_rocksdb_total_ram_usage", 0)) {
    for (const auto &pair : ::rocksdb::TickersNameMap) {
      if (std::find(tickers.begin(), tickers.end(), pair.first) != tickers.end()) {
        auto metric_suffix = pair.second;
        std::replace(metric_suffix.begin(), metric_suffix.end(), '.', '_');
        active_tickers_.emplace(pair.first, metrics_.RegisterAtomicGauge("storage_" + metric_suffix, 0));
      }
    }
    metrics_.Register();
  }

  /*
   * For now, we have a hardcoded default metrics configuration list.
   * In the future we may add a rocksdb configuration file to enable flexibility.
   */
  RocksDbStorageMetrics(const concord::storage::rocksdb::Client &owningClient)
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
                               ::rocksdb::Tickers::BLOOM_FILTER_FULL_TRUE_POSITIVE},
                              owningClient) {}

  ~RocksDbStorageMetrics() { update_metrics_.reset(); }

  void setMetricsDataSources(std::shared_ptr<::rocksdb::SstFileManager> sourceSstFm,
                             std::shared_ptr<::rocksdb::Statistics> sourceStatistics);

  // update and print to log rocksdb RAM usage
  void updateDBMemUsageMetrics(){};

  // periodically running function to update metrics. this func and its sub-funcs aren't
  // thread safe. It's the Client responsibility to temporarily disable this periodic func
  // if configs change during runtime.
  void updateMetrics() {
    static size_t entry_count{0};

    // if isn't initialized yet or disabled - return
    if (!sstFm_ || !statistics_) return;

    // we don't update mem usage every call since it's pretty heavy on resources and isn't needed in a very high
    // frequency
    if (entry_count % update_mem_usage_metrics_factor == 0) {
      // enter here only once every update_mem_usage_metrics_factor calls of updateMetrics()
      updateDBMemUsageMetrics();
    }
    entry_count++;

    // update all tickers
    for (auto &pair : active_tickers_) {
      pair.second.Get().Set(statistics_->getTickerCount(pair.first));
    }
    // upodate total size
    total_db_disk_size_.Get().Set(sstFm_->GetTotalSize());

    metrics_.UpdateAggregator();
  }

  void setMetricsDataSources(std::shared_ptr<::rocksdb::SstFileManager> sourceSstFm,
                             std::shared_ptr<::rocksdb::Statistics> sourceStatistics) {
    sstFm_ = sourceSstFm;
    statistics_ = sourceStatistics;

    update_metrics_ =
        std::make_unique<concord::util::PeriodicCall>([this]() { updateMetrics(); }, update_metrics_interval_millisec);
  }

 private:
  // const ref to the Client enclosing this obj, in order to use RocksDB APIs it has
  const concord::storage::rocksdb::Client &owning_client_;
  // map of all tickers we monitor into our metrics
  std::unordered_map<::rocksdb::Tickers, concordMetrics::AtomicGaugeHandle> active_tickers_;
  // total disk size
  concordMetrics::AtomicGaugeHandle total_db_disk_size_;
  // RAM usage of all mem tables
  concordMetrics::AtomicGaugeHandle all_mem_tables_ram_usage_;
  // RAM usage of all unflushed mem tables
  concordMetrics::AtomicGaugeHandle all_unflushed_mem_tables_ram_usage_;
  // RAM usage of block caches - one metric for all CFs. CFs may share the same block cache or not.
  concordMetrics::AtomicGaugeHandle block_caches_ram_usage_;
  // RAM usage of indexing, bloom filters and other related data rocksdb keeps for better performance
  concordMetrics::AtomicGaugeHandle indexes_and_filters_ram_usage_;
  // total RAM usage - sum of all_mem_tables_ram_usage_, all_unflushed_mem_tables_ram_usage_, block_caches_ram_usage_
  // and indexes_and_filters_ram_usage_
  concordMetrics::AtomicGaugeHandle rocksdb_total_ram_usage_;

  // maps rocksdb mem usage metrics to concord metrics
  std::map<::rocksdb::MemoryUtil::UsageType, concordMetrics::AtomicGaugeHandle> rocksdb_to_concord_metrics_map_{
      {::rocksdb::MemoryUtil::UsageType::kMemTableTotal, all_mem_tables_ram_usage_},
      {::rocksdb::MemoryUtil::UsageType::kMemTableUnFlushed, all_unflushed_mem_tables_ram_usage_},
      {::rocksdb::MemoryUtil::UsageType::kTableReadersTotal, indexes_and_filters_ram_usage_},
      {::rocksdb::MemoryUtil::UsageType::kCacheTotal, block_caches_ram_usage_}};

  std::shared_ptr<::rocksdb::SstFileManager> sstFm_;
  std::shared_ptr<::rocksdb::Statistics> statistics_;
  std::unique_ptr<concord::util::PeriodicCall> update_metrics_ = nullptr;
};

// // updates the rocksDB mem usage metrics
// void Client::RocksDbStorageMetrics::updateDBMemUsageMetrics() {
//   // get column families block caches pointers.
//   // important note:
//   // 1. Different column families may use the same block cache or a different one.
//   // In order to correctly report the mem usage we need to consider only unique instances of ::rocksdb::Cache .
//   // 2. The caches config may change in runtime. we need to check which instances of ::rocksdb::Cache are active.
//   // Due to both reasons above, we define a std::unordered_set here, and send it to GetApproximateMemoryUsageByType
//   API. std::unordered_set<const ::rocksdb::Cache *> block_caches_raw{};

//   for (const auto &[cf_name, cf_handle] : owning_client_.cf_handles_) {
//     UNUSED(cf_name);
//     ::rocksdb::ColumnFamilyDescriptor cf_desc;
//     cf_handle->GetDescriptor(&cf_desc);
//     auto *cf_table_options =
//         reinterpret_cast<::rocksdb::BlockBasedTableOptions *>(cf_desc.options.table_factory->GetOptions());

//     block_caches_raw.emplace(cf_table_options->block_cache.get());
//   }

//   // GetApproximateMemoryUsageByType writes output into usage_by_type
//   std::map<::rocksdb::MemoryUtil::UsageType, uint64_t> usage_by_type;
//   std::vector<::rocksdb::DB *> dbs{owning_client_.dbInstance_.get()};

//   if (block_caches_raw.size() == 0) {
//     // if there are no caches defined:
//     // 1. GetApproximateMemoryUsageByType will fail on segmentation fault (true to rocksdb ver 6.8.1)
//     // 2. It's not a real deployment where we always use caches (it's probably a unit test)
//     // Hence, do nothing and return
//     return;
//   }
//   ::rocksdb::MemoryUtil::GetApproximateMemoryUsageByType(dbs, block_caches_raw, &usage_by_type);

//   uint64_t total_usage{0};
//   uint64_t num_bytes;

//   // go over the results written into usage_by_type, write to each matching concord metric and add to total_usage
//   sum. for (auto &[rocks_db_usage_type_name, concord_metric_handle] : rocksdb_to_concord_metrics_map_) {
//     if (usage_by_type.count(rocks_db_usage_type_name)) {
//       num_bytes = usage_by_type[rocks_db_usage_type_name];
//       concord_metric_handle.Get().Set(num_bytes);
//       total_usage += num_bytes;
//     } else {
//       LOG_WARN(
//           owning_client_.logger(),
//           std::to_string(rocks_db_usage_type_name)
//               << " doesn't exist in ::rocksdb::MemoryUtil::UsageType, API may have changed! setting metric to zero");
//     }
//   }
//   rocksdb_total_ram_usage_.Get().Set(total_usage);

//   LOG_INFO(owning_client_.logger(),
//            "RocksDB Memory usage report. Total: "
//                << rocksdb_total_ram_usage_.Get().Get() << " Bytes, "
//                << "Mem tables: " << all_mem_tables_ram_usage_.Get().Get() << " Bytes, "
//                << "Unflushed Mem tables: " << all_unflushed_mem_tables_ram_usage_.Get().Get() << " Bytes, "
//                << "Table readers (indexes and filters): " << indexes_and_filters_ram_usage_.Get().Get() << " Bytes, "
//                << "Block caches: " << block_caches_ram_usage_.Get().Get() << " Bytes");
// }

#endif

}  // namespace storage
}  // namespace concord