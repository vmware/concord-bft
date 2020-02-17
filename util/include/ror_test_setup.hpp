// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// This is helper code to be used with tests related to RO replica.
// To create RoRAppState, we should provide 2 IDBClients: for local and object store access.
// The USE_ROCKSDB flag controlls the local IDBClient (in memory or rocksdb) and the TEST_ROR_APPSTATE_OSTORE_IN_MEMORY
// flag controls object store IBDClient (real Object store, in memory or rocksdb).
// is no value

#pragma once

#include "storage/db_interface.h"
#include "ror_app_state.hpp"

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
using namespace concord::storage::rocksdb;
using namespace concord::storage::blockchain::v1DirectKeyValue;
#else
#include "memorydb/client.h"
using namespace concord::storage::memorydb;
#endif

#ifdef TEST_ROR_APPSTATE_OSTORE_IN_MEMORY
#include "memorydb/client.h"
using namespace concord::storage::memorydb;
using namespace concord::storage::blockchain::v1DirectKeyValue;
#endif

using namespace concord::storage;

class SimpleIntegerKeyComparator : public IDBClient::IKeyComparator {
 int composedKeyComparison(const char* _a_data, size_t _a_length, const char* _b_data, size_t _b_length) override {
   assert(_a_data);
   assert(_b_data);
   assert(_a_length == sizeof(char));
   assert(_b_length == sizeof(char));
   char keyA = *reinterpret_cast<const char*>(_a_data);
   char keyB = *reinterpret_cast<const char*>(_b_data);
   return keyA - keyB;
 }
};

 std::shared_ptr<IDBClient> set_local_client() {
#ifdef USE_ROCKSDB
    return std::static_pointer_cast<IDBClient>(std::make_shared< concord::storage::rocksdb::Client>(
        "unit_test_db_meta", ::rocksdb::BytewiseComparator()));
#else
    return std::static_pointer_cast<IDBClient>(
      std::make_shared<memorydb::Client>(KeyComparator(new SimpleIntegerKeyComparator())));
#endif
}

  std::shared_ptr<IDBClient> set_object_store_client() {
#ifdef TEST_ROR_APPSTATE_OBJECTSTORE
   S3_StoreConfig config;
    ObjectStoreBehavior b{true, true};
    config.bucketName = "blockchain-dev-asx";
    config.accessKey = "blockchain-dev";
    config.protocol = "HTTP";
    config.url = "10.70.30.244:9020";
    config.secretKey = "Rz0mdbUNGJBxqdzprn5XGSXPr2AfkgcQsYS4y698";
    return std::static_pointer_cast<IDBClient>(
      std::make_shared<ObjectStoreClient>(config, b));
#elif USE_ROCKSDB
  return std::static_pointer_cast<IDBClient>(std::make_shared< concord::storage::rocksdb::Client>(
        "unit_test_db_object_store", ::rocksdb::BytewiseComparator()));
#else
   return std::static_pointer_cast<IDBClient>(
      std::make_shared<memorydb::Client>(KeyComparator(new SimpleIntegerKeyComparator())));
#endif
}


