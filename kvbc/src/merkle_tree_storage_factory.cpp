// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//

#include "merkle_tree_storage_factory.h"

#include "merkle_tree_db_adapter.h"
#include "memorydb/client.h"
#include "storage/merkle_tree_key_manipulator.h"
#include "rocksdb/client.h"
#include "rocksdb/native_client.h"

#include <rocksdb/filter_policy.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>

#include <vector>

namespace concord::kvbc::v2MerkleTree {
#ifdef USE_ROCKSDB

std::shared_ptr<rocksdb::Statistics> completeRocksDBConfiguration(
    ::rocksdb::Options& db_options,
    std::vector<::rocksdb::ColumnFamilyDescriptor>& cf_descs,
    std::size_t rocksdbLruCacheBytes) {
  auto table_options = ::rocksdb::BlockBasedTableOptions{};
  table_options.block_cache = ::rocksdb::NewLRUCache(rocksdbLruCacheBytes);
  table_options.filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10, false));
  db_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Use the same block cache and table options for all column familes for now.
  for (auto& d : cf_descs) {
    auto* cf_table_options =
        reinterpret_cast<::rocksdb::BlockBasedTableOptions*>(d.options.table_factory->GetOptions());
    cf_table_options->block_cache = table_options.block_cache;
    cf_table_options->filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10, false));
  }
  return db_options.statistics;
}

RocksDBStorageFactory::RocksDBStorageFactory(const std::string& dbPath,
                                             const std::unordered_set<concord::kvbc::Key>& nonProvableKeySet,
                                             const std::shared_ptr<concord::performance::PerformanceManager>& pm)
    : dbPath_{dbPath}, nonProvableKeySet_{nonProvableKeySet}, pm_{pm} {}

RocksDBStorageFactory::RocksDBStorageFactory(const std::string& dbPath,
                                             const std::string& dbConfPath,
                                             std::size_t rocksdbLruCacheBytes,
                                             const std::shared_ptr<concord::performance::PerformanceManager>& pm)
    : dbPath_{dbPath}, dbConfPath_{dbConfPath}, rocksdbLruCacheBytes_{rocksdbLruCacheBytes}, pm_{pm} {}

IStorageFactory::DatabaseSet RocksDBStorageFactory::newDatabaseSet() const {
  auto ret = IStorageFactory::DatabaseSet{};
  if (!dbConfPath_) {
    ret.dataDBClient = std::make_shared<storage::rocksdb::Client>(dbPath_);
    ret.dataDBClient->init();
  } else {
    const auto rocksdbLruCacheBytes = rocksdbLruCacheBytes_;
    auto opts = storage::rocksdb::NativeClient::UserOptions{
        *dbConfPath_,
        [rocksdbLruCacheBytes](::rocksdb::Options& db_options,
                               std::vector<::rocksdb::ColumnFamilyDescriptor>& cf_descs) {
          completeRocksDBConfiguration(db_options, cf_descs, rocksdbLruCacheBytes);
        }};
    auto db = storage::rocksdb::NativeClient::newClient(dbPath_, false, opts);
    ret.dataDBClient = db->asIDBClient();
  }
  ret.metadataDBClient = ret.dataDBClient;
  ret.dbAdapter = std::make_unique<DBAdapter>(ret.dataDBClient, true, nonProvableKeySet_, pm_);
  return ret;
}

std::unique_ptr<storage::IMetadataKeyManipulator> RocksDBStorageFactory::newMetadataKeyManipulator() const {
  return std::make_unique<storage::v2MerkleTree::MetadataKeyManipulator>();
}

std::unique_ptr<storage::ISTKeyManipulator> RocksDBStorageFactory::newSTKeyManipulator() const {
  return std::make_unique<storage::v2MerkleTree::STKeyManipulator>();
}
#endif

IStorageFactory::DatabaseSet MemoryDBStorageFactory::newDatabaseSet() const {
  auto ret = IStorageFactory::DatabaseSet{};
  ret.dataDBClient = std::make_shared<storage::memorydb::Client>();
  ret.dataDBClient->init();
  ret.metadataDBClient = ret.dataDBClient;
  ret.dbAdapter = std::make_unique<DBAdapter>(ret.dataDBClient);
  return ret;
}

std::unique_ptr<storage::IMetadataKeyManipulator> MemoryDBStorageFactory::newMetadataKeyManipulator() const {
  return std::make_unique<storage::v2MerkleTree::MetadataKeyManipulator>();
}

std::unique_ptr<storage::ISTKeyManipulator> MemoryDBStorageFactory::newSTKeyManipulator() const {
  return std::make_unique<storage::v2MerkleTree::STKeyManipulator>();
}

}  // namespace concord::kvbc::v2MerkleTree
