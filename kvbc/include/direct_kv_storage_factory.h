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

#pragma once

#include "storage_factory_interface.h"
#include "PerformanceManager.hpp"

#ifdef USE_S3_OBJECT_STORE
#include "s3/client.hpp"
#endif

#include <string>

namespace concord::kvbc::v1DirectKeyValue {

#ifdef USE_ROCKSDB
class RocksDBStorageFactory : public IStorageFactory {
 public:
  RocksDBStorageFactory(const std::string &dbPath,
                        const std::shared_ptr<concord::performance::PerformanceManager> &pm =
                            std::make_shared<concord::performance::PerformanceManager>())
      : dbPath_{dbPath}, pm_{pm} {}

 public:
  DatabaseSet newDatabaseSet() const override;
  std::unique_ptr<storage::IMetadataKeyManipulator> newMetadataKeyManipulator() const override;
  std::unique_ptr<storage::ISTKeyManipulator> newSTKeyManipulator() const override;
  std::string path() const override { return dbPath_; }

 private:
  const std::string dbPath_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
};
#endif

class MemoryDBStorageFactory : public IStorageFactory {
 public:
  DatabaseSet newDatabaseSet() const override;
  std::unique_ptr<storage::IMetadataKeyManipulator> newMetadataKeyManipulator() const override;
  std::unique_ptr<storage::ISTKeyManipulator> newSTKeyManipulator() const override;
  std::string path() const override { return ""; }
};

#if defined(USE_S3_OBJECT_STORE) && defined(USE_ROCKSDB)
class S3StorageFactory : public IStorageFactory {
 public:
  S3StorageFactory(const std::string &metadataDBPath, const storage::s3::StoreConfig &s3Conf)
      : metadataDBPath_{metadataDBPath}, s3Conf_{s3Conf} {}

 public:
  DatabaseSet newDatabaseSet() const override;
  std::unique_ptr<storage::IMetadataKeyManipulator> newMetadataKeyManipulator() const override;
  std::unique_ptr<storage::ISTKeyManipulator> newSTKeyManipulator() const override;
  std::string path() const override { return ""; }

 private:
  const std::string metadataDBPath_;
  const storage::s3::StoreConfig s3Conf_;
};
#endif

}  // namespace concord::kvbc::v1DirectKeyValue
