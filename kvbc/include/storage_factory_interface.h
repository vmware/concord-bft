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

#include "db_adapter_interface.h"
#include "storage/db_interface.h"
#include "storage/key_manipulator_interface.h"

#include <memory>

namespace concord::kvbc {

// Creates a family of related storage objects that should be used together across the system.
// All methods throw on errors and never return null pointers.
class IStorageFactory {
 public:
  struct DatabaseSet {
    // The data DB client is used to store application blockchain data, e.g. key/values, blocks, etc.
    std::shared_ptr<storage::IDBClient> dataDBClient;

    // The metadata DB client is used to store state transfer data, Concord-BFT protocol metadata, etc.
    // All data that is not application data is written through this DB client.
    std::shared_ptr<storage::IDBClient> metadataDBClient;

    // The DB adapter instance is used to store a blockchain in the data DB through dataDBClient .
    std::unique_ptr<IDbAdapter> dbAdapter;
  };

  // Create a set of database objects at once. Objects from one set can only be used throughout the system with other
  // objects from the same set. Using objects from different sets is not supported and might lead to undefined behavior.
  // Note: The returned DB clients and the adapter are initialized and ready to use.
  virtual DatabaseSet newDatabaseSet() const = 0;

  // Create a metadata key manipulator. It is allowed to create multiple instances and use them together with objects
  // from the database set.
  virtual std::unique_ptr<storage::IMetadataKeyManipulator> newMetadataKeyManipulator() const = 0;

  // Create a state transfer key manipulator. It is allowed to create multiple instances and use them together with
  // objects from the database set.
  virtual std::unique_ptr<storage::ISTKeyManipulator> newSTKeyManipulator() const = 0;

  virtual std::string path() const = 0;

  virtual ~IStorageFactory() = default;
};

}  // namespace concord::kvbc
