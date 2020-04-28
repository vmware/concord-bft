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

namespace concord::kvbc::v2MerkleTree {

#ifdef USE_ROCKSDB
RocksDBStorageFactory::RocksDBStorageFactory(const std::string &dbPath) : dbPath_{dbPath} {}

IStorageFactory::DatabaseSet RocksDBStorageFactory::newDatabaseSet() const {
  auto ret = IStorageFactory::DatabaseSet{};
  ret.dataDBClient = std::make_shared<storage::rocksdb::Client>(dbPath_);
  ret.dataDBClient->init();
  ret.metadataDBClient = ret.dataDBClient;
  ret.dbAdapter = std::make_unique<DBAdapter>(ret.dataDBClient);
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
