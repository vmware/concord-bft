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
#include "kv_types.hpp"
#include "PerformanceManager.hpp"

#include <optional>
#include <string>
#include <unordered_set>

namespace concord::kvbc::v2MerkleTree {

#ifdef USE_ROCKSDB
class RocksDBStorageFactory : public IStorageFactory {
 public:
  RocksDBStorageFactory(
      const std::string& dbPath,
      const std::unordered_set<concord::kvbc::Key>& nonProvableKeySet = std::unordered_set<concord::kvbc::Key>{},
      const std::shared_ptr<concord::performance::PerformanceManager>& pm =
          std::make_shared<concord::performance::PerformanceManager>());

 public:
  DatabaseSet newDatabaseSet() const override;
  std::unique_ptr<storage::IMetadataKeyManipulator> newMetadataKeyManipulator() const override;
  std::unique_ptr<storage::ISTKeyManipulator> newSTKeyManipulator() const override;

 private:
  const std::string dbPath_;
  const std::unordered_set<concord::kvbc::Key> nonProvableKeySet_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
};
#endif

class MemoryDBStorageFactory : public IStorageFactory {
 public:
  DatabaseSet newDatabaseSet() const override;
  std::unique_ptr<storage::IMetadataKeyManipulator> newMetadataKeyManipulator() const override;
  std::unique_ptr<storage::ISTKeyManipulator> newSTKeyManipulator() const override;
};

}  // namespace concord::kvbc::v2MerkleTree
