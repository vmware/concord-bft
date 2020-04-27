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

#include "direct_kv_storage_factory.h"

#include "direct_kv_db_adapter.h"
#include "memorydb/client.h"
#include "memorydb/key_comparator.h"
#include "storage/direct_kv_key_manipulator.h"
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"

#ifdef USE_S3_OBJECT_STORE
#include "object_store/object_store_client.hpp"
#endif

#include <chrono>

namespace concord::kvbc::v1DirectKeyValue {

namespace {
#ifdef USE_ROCKSDB
auto createRocksDBClient(const std::string &dbPath) {
  // Since the client doesn't own the comparator, use the same instance for all clients. This is RocksDB's default
  // behavior - using a static instance returned by BytewiseComparator(). See the ::rocksdb::Options class.
  static const auto comparator = storage::rocksdb::KeyComparator{new DBKeyComparator{}};
  return std::make_shared<storage::rocksdb::Client>(dbPath, &comparator);
}
#endif
}  // namespace

#ifdef USE_ROCKSDB
IStorageFactory::DatabaseSet RocksDBStorageFactory::newDatabaseSet() const {
  auto ret = IStorageFactory::DatabaseSet{};
  ret.dataDBClient = createRocksDBClient(dbPath_);
  ret.dataDBClient->init();
  ret.metadataDBClient = ret.dataDBClient;
  ret.dbAdapter = std::make_unique<DBAdapter>(ret.dataDBClient);
  return ret;
}

std::unique_ptr<storage::IMetadataKeyManipulator> RocksDBStorageFactory::newMetadataKeyManipulator() const {
  return std::make_unique<storage::v1DirectKeyValue::MetadataKeyManipulator>();
}

std::unique_ptr<storage::ISTKeyManipulator> RocksDBStorageFactory::newSTKeyManipulator() const {
  return std::make_unique<storage::v1DirectKeyValue::STKeyManipulator>();
}
#endif

IStorageFactory::DatabaseSet MemoryDBStorageFactory::newDatabaseSet() const {
  auto ret = IStorageFactory::DatabaseSet{};
  const auto comparator = storage::memorydb::KeyComparator{new DBKeyComparator{}};
  ret.dataDBClient = std::make_shared<storage::memorydb::Client>(comparator);
  ret.dataDBClient->init();
  ret.metadataDBClient = ret.dataDBClient;
  ret.dbAdapter = std::make_unique<DBAdapter>(ret.dataDBClient);
  return ret;
}

std::unique_ptr<storage::IMetadataKeyManipulator> MemoryDBStorageFactory::newMetadataKeyManipulator() const {
  return std::make_unique<storage::v1DirectKeyValue::MetadataKeyManipulator>();
}

std::unique_ptr<storage::ISTKeyManipulator> MemoryDBStorageFactory::newSTKeyManipulator() const {
  return std::make_unique<storage::v1DirectKeyValue::STKeyManipulator>();
}

#if defined(USE_S3_OBJECT_STORE) && defined(USE_ROCKSDB)
IStorageFactory::DatabaseSet S3StorageFactory::newDatabaseSet() const {
  auto ret = IStorageFactory::DatabaseSet{};

  ret.metadataDBClient = createRocksDBClient(metadataDBPath_);
  ret.metadataDBClient->init();
  ret.dataDBClient = std::make_shared<storage::ObjectStoreClient>(new storage::s3::Client{s3Conf_});
  ret.dataDBClient->init();

  const auto prefix = std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  auto dataKeyGenerator = std::make_unique<S3KeyGenerator>(prefix);
  ret.dbAdapter = std::make_unique<DBAdapter>(ret.dataDBClient, std::move(dataKeyGenerator), true);

  return ret;
}

std::unique_ptr<storage::IMetadataKeyManipulator> S3StorageFactory::newMetadataKeyManipulator() const {
  return std::make_unique<storage::v1DirectKeyValue::MetadataKeyManipulator>();
}

std::unique_ptr<storage::ISTKeyManipulator> S3StorageFactory::newSTKeyManipulator() const {
  return std::make_unique<storage::v1DirectKeyValue::STKeyManipulator>();
}
#endif

}  // namespace concord::kvbc::v1DirectKeyValue
