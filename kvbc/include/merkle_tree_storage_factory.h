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

#include <optional>
#include <string>

namespace concord::kvbc::v2MerkleTree {

#ifdef USE_ROCKSDB
class RocksDBStorageFactory : public IStorageFactory {
 public:
  RocksDBStorageFactory(const std::string &dbPath);

 public:
  DatabaseSet newDatabaseSet() const override;
  DatabaseSet newDatabaseSet(bool readOnly) const override;
  std::unique_ptr<storage::IMetadataKeyManipulator> newMetadataKeyManipulator() const override;
  std::unique_ptr<storage::ISTKeyManipulator> newSTKeyManipulator() const override;

 private:
  const std::string dbPath_;
};
#endif

class MemoryDBStorageFactory : public IStorageFactory {
 public:
  DatabaseSet newDatabaseSet() const override;
  DatabaseSet newDatabaseSet(bool readOnly) const override;
  std::unique_ptr<storage::IMetadataKeyManipulator> newMetadataKeyManipulator() const override;
  std::unique_ptr<storage::ISTKeyManipulator> newSTKeyManipulator() const override;
};

}  // namespace concord::kvbc::v2MerkleTree
