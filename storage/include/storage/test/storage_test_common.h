// Concord
//
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

#pragma once

#include "gtest/gtest.h"

#include "memorydb/client.h"
#include "rocksdb/client.h"
#include "rocksdb/native_client.h"
#include "sliver.hpp"
#include "storage/db_interface.h"
#include "util/filesystem.hpp"

#include <unistd.h>

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

inline constexpr auto defaultDbId = std::size_t{0};

#ifdef USE_ROCKSDB
inline const auto rocksDbPathPrefix = std::string{"/tmp/storage_test_rocksdb"};

// Support multithreaded runs by appending the thread ID to the RocksDB path.
inline std::string rocksDbPath(std::size_t dbId) {
  std::stringstream ss;
  ss << '_' << dbId << '_' << getpid() << '_' << std::this_thread::get_id();
  return rocksDbPathPrefix + ss.str();
}

inline void cleanup(std::size_t dbId = defaultDbId) { fs::remove_all(rocksDbPath(dbId)); }
#else
inline void cleanup(std::size_t = defaultDbId) {}
#endif

struct TestMemoryDb {
  static std::shared_ptr<concord::storage::IDBClient> create(std::size_t dbId = defaultDbId) {
    auto db = std::make_shared<concord::storage::memorydb::Client>();
    db->init();
    return db;
  }

  static void cleanup(std::size_t = defaultDbId) {}

  static std::string type() { return "memorydb"; }
};

#ifdef USE_ROCKSDB
struct TestRocksDb {
  static std::shared_ptr<::concord::storage::IDBClient> create(std::size_t dbId = defaultDbId) {
    // Create the RocksDB client with the default lexicographical comparator.
    auto db = std::make_shared<::concord::storage::rocksdb::Client>(rocksDbPath(dbId));
    db->init();
    return db;
  }

  static std::shared_ptr<::concord::storage::rocksdb::NativeClient> createNative(std::size_t dbId = defaultDbId) {
    const auto readOnly = false;
    return ::concord::storage::rocksdb::NativeClient::newClient(
        rocksDbPath(dbId), readOnly, ::concord::storage::rocksdb::NativeClient::DefaultOptions{});
  }

  static std::shared_ptr<::concord::storage::rocksdb::NativeClient> createNative(
      const ::concord::storage::rocksdb::NativeClient::DefaultOptions &opts, std::size_t dbId = defaultDbId) {
    const auto readOnly = false;
    return ::concord::storage::rocksdb::NativeClient::newClient(rocksDbPath(dbId), readOnly, opts);
  }

  static std::shared_ptr<::concord::storage::rocksdb::NativeClient> createNative(
      const ::concord::storage::rocksdb::NativeClient::ExistingOptions &opts, std::size_t dbId = defaultDbId) {
    const auto readOnly = false;
    return ::concord::storage::rocksdb::NativeClient::newClient(rocksDbPath(dbId), readOnly, opts);
  }

  static std::shared_ptr<::concord::storage::rocksdb::NativeClient> createNative(
      const ::concord::storage::rocksdb::NativeClient::UserOptions &opts, std::size_t dbId = defaultDbId) {
    const auto readOnly = false;
    return ::concord::storage::rocksdb::NativeClient::newClient(rocksDbPath(dbId), readOnly, opts);
  }

  static void cleanup(std::size_t dbId = defaultDbId) { ::cleanup(dbId); }

  static std::string type() { return "RocksDB"; }
};

struct TestRocksDbSnapshot {
  static std::string type() { return "RocksDBSnapshot"; }
  static void cleanup(const int16_t checkPointId) { ::cleanup(checkPointId); }
  static std::shared_ptr<::concord::storage::rocksdb::NativeClient> createNative(const std::string &checkPointPath) {
    const auto readOnly = false;
    return ::concord::storage::rocksdb::NativeClient::newClient(
        checkPointPath, readOnly, ::concord::storage::rocksdb::NativeClient::DefaultOptions{});
  }
};
#endif

template <typename ParamType>
class ParametrizedTest : public ::testing::TestWithParam<ParamType> {
  void SetUp() override { cleanup(); }
  void TearDown() override { cleanup(); }
};

// Generate test name suffixes based on the DB client type.
struct TypePrinter {
  template <typename ParamType>
  std::string operator()(const ::testing::TestParamInfo<ParamType> &info) const {
    return info.param->type();
  }
};

inline concordUtils::Sliver getSliverOfSize(std::size_t size, char content = 'a') { return std::string(size, content); }

inline const auto defaultData = std::string{"defaultData"};
inline const auto defaultSliver = concordUtils::Sliver::copy(defaultData.c_str(), defaultData.size());
