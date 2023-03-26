// Copyright 2018 VMware, all rights reserved

/**
 * @file RocksDBClient.h
 *
 *  @brief Header file containing the RocksDBClientIterator and RocksDBClient
 *  class definitions.
 *
 *  Objects of RocksDBClientIterator contain an iterator for database along with
 *  a pointer to the client object.
 *
 *  Objects of RocksDBClient signify connections with RocksDB database. They
 *  contain variables for storing the database directory path, connection object
 *  and an optional comparator.
 *
 */

#pragma once

// hanan
#ifdef USE_ROCKSDB

#include "util/periodic_call.hpp"
#include "util/Metrics.hpp"

#include <rocksdb/statistics.h>
#include <unordered_map>
#include <vector>
#include <rocksdb/sst_file_manager.h>

#include <rocksdb/utilities/memory_util.h>
#include <unordered_set>

#endif

#ifdef USE_ROCKSDB
#include "log/logger.hpp"
#include <rocksdb/db.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/utilities/checkpoint.h>
#include "storage/db_interface.h"
//#include "storage/storage_metrics.h"

#include <map>
#include <optional>
#include <vector>

namespace concord {
namespace storage {
namespace rocksdb {

class Client;

// hanan2

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
  static constexpr size_t update_metrics_interval_millisec = 50;  // every 100msec
  static constexpr size_t update_mem_usage_metrics_factor =
      1;  // update_metrics_interval_millisec * 600 = every 1 minute

 public:
  concordMetrics::Component metrics_;
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) { metrics_.SetAggregator(aggregator); }

  RocksDbStorageMetrics(const std::vector<::rocksdb::Tickers>& tickers,
                        const concord::storage::rocksdb::Client& owningClient)
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
    for (const auto& pair : ::rocksdb::TickersNameMap) {
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
  RocksDbStorageMetrics(const concord::storage::rocksdb::Client& owningClient)
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

  void updateDBMemUsageMetrics();

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
    for (auto& pair : active_tickers_) {
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

  // private:
  // const ref to the Client enclosing this obj, in order to use RocksDB APIs it has
  const concord::storage::rocksdb::Client& owning_client_;
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

#endif

class ClientIterator : public concord::storage::IDBClient::IDBClientIterator {
  friend class Client;

 public:
  ClientIterator(const Client* _parentClient, logging::Logger);
  ~ClientIterator() { delete m_iter; }

  // Inherited via IDBClientIterator
  KeyValuePair first() override;
  KeyValuePair last() override;
  KeyValuePair seekAtLeast(const concordUtils::Sliver& _searchKey) override;
  KeyValuePair seekAtMost(const concordUtils::Sliver& _searchKey) override;
  KeyValuePair previous() override;
  KeyValuePair next() override;
  KeyValuePair getCurrent() override;
  bool isEnd() override;
  concordUtils::Status getStatus() override;

 private:
  logging::Logger logger;

  ::rocksdb::Iterator* m_iter;

  // Reference to the RocksDBClient
  const Client* m_parentClient;

  concordUtils::Status m_status;
};

class Client : public concord::storage::IDBClient {
  friend class RocksDbStorageMetrics;

 public:
  Client(const std::string& _dbPath) : m_dbPath(_dbPath), storage_metrics_(RocksDbStorageMetrics(*this)) {}
  Client(const std::string& _dbPath, std::unique_ptr<const ::rocksdb::Comparator>&& comparator)
      : m_dbPath(_dbPath), comparator_(std::move(comparator)), storage_metrics_(RocksDbStorageMetrics(*this)) {}

  ~Client() {
    // Clear column family handles before the DB as handle destruction calls a DB instance member and we want that to
    // happen before we delete the DB pointer.
    cf_handles_.clear();
    if (txn_db_) {
      // If we're using a TransactionDB, it wraps the base DB, so release it
      // instead of releasing the base DB.
      (void)dbInstance_.release();
      delete txn_db_;
    }
  }

  void init(bool readOnly = false) override;
  concordUtils::Status get(const concordUtils::Sliver& _key, concordUtils::Sliver& _outValue) const override;
  concordUtils::Status get(const concordUtils::Sliver& _key,
                           char*& buf,
                           uint32_t bufSize,
                           uint32_t& _realSize) const override;
  concordUtils::Status has(const Sliver& _key) const override;
  IDBClientIterator* getIterator() const override;
  concordUtils::Status freeIterator(IDBClientIterator* _iter) const override;
  concordUtils::Status put(const concordUtils::Sliver& _key, const concordUtils::Sliver& _value) override;
  concordUtils::Status del(const concordUtils::Sliver& _key) override;
  concordUtils::Status multiGet(const KeysVector& _keysVec, ValuesVector& _valuesVec) override;
  concordUtils::Status multiPut(const SetOfKeyValuePairs& _keyValueMap, bool sync = false) override;
  concordUtils::Status multiDel(const KeysVector& _keysVec) override;
  concordUtils::Status rangeDel(const Sliver& _beginKey, const Sliver& _endKey) override;
  ::rocksdb::Iterator* getNewRocksDbIterator() const;
  bool isNew() override;
  ITransaction* beginTransaction() override;
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) override {
    storage_metrics_.setAggregator(aggregator);
  }
  std::string getPath() const override { return m_dbPath; }
  void setCheckpointPath(const std::string& path) override { dbCheckpointPath_ = path; }
  std::string getCheckpointPath() const override { return dbCheckpointPath_; }
  std::string getPathForCheckpoint(std::uint64_t checkpointId) const override;
  concordUtils::Status createCheckpoint(const uint64_t& checkPointId) override;
  std::vector<uint64_t> getListOfCreatedCheckpoints() const override;
  void removeCheckpoint(const uint64_t& checkPointId) const override;
  void removeAllCheckpoints() const override;

  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.storage.rocksdb");
    return logger_;
  }

 private:
  struct Options {
    std::string filepath;

    // Any RocksDB customization that cannot be completed in an init file can be done here.
    std::function<void(::rocksdb::Options&, std::vector<::rocksdb::ColumnFamilyDescriptor>&)> completeInit;

    // These are for backwards compatibility. Users of the nativeClient should use `filepath` and `completeInit`
    // instead.
    ::rocksdb::Options db_options;
    void applyOptimizations();
  };

  // Initialize a DB.
  // If Options are provided, use them as is.
  // If Options are not provided, try to load them from an options file.
  // If `applyOptimizations` is set, apply optimizations on top of the provided or the loaded ones.
  void initDB(bool readOnly, const std::optional<Options>&, bool applyOptimizations);
  void initDBFromFile(bool readOnly, const Options& user_options);
  void openRocksDB(bool readOnly,
                   const ::rocksdb::Options& db_options,
                   std::vector<::rocksdb::ColumnFamilyDescriptor>& cf_descs);
  concordUtils::Status launchBatchJob(::rocksdb::WriteBatch& _batchJob, bool sync = false);
  concordUtils::Status get(const concordUtils::Sliver& _key, std::string& _value) const;
  bool keyIsBefore(const concordUtils::Sliver& _lhs, const concordUtils::Sliver& _rhs) const;
  bool columnFamilyIsEmpty(::rocksdb::ColumnFamilyHandle*) const;

  // Column family unique pointers that are managed solely by Client. This allows us to use a raw
  // pointer in CfDeleter as we know the column family unique pointer will not be moved out of Client.
  struct CfDeleter {
    void operator()(::rocksdb::ColumnFamilyHandle* h) const noexcept {
      if (!client_->dbInstance_->DestroyColumnFamilyHandle(h).ok()) {
        std::terminate();
      }
    }
    Client* client_{nullptr};
  };
  using CfUniquePtr = std::unique_ptr<::rocksdb::ColumnFamilyHandle, CfDeleter>;

  // Guard against double init.
  bool initialized_{false};

  // Database path on directory (used for connection).
  std::string m_dbPath;
  // Database checkpoint directory
  std::string dbCheckpointPath_;  // default val = m_dbPath + "_checkpoint"

  // Database object (created on connection).
  std::unique_ptr<::rocksdb::DB> dbInstance_;
  ::rocksdb::OptimisticTransactionDB* txn_db_ = nullptr;
  std::unique_ptr<const ::rocksdb::Comparator> comparator_;
  std::map<std::string, CfUniquePtr> cf_handles_;

  // Metrics
  mutable RocksDbStorageMetrics storage_metrics_;
  std::unique_ptr<::rocksdb::Checkpoint> dbCheckPoint_;

  friend class NativeClient;
};

::rocksdb::Slice toRocksdbSlice(const concordUtils::Sliver& _s);
concordUtils::Sliver fromRocksdbSlice(::rocksdb::Slice _s);
concordUtils::Sliver copyRocksdbSlice(::rocksdb::Slice _s);

}  // namespace rocksdb
}  // namespace storage
}  // namespace concord
#endif  // USE_ROCKSDB
