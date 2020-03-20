// Copyright 2020 VMware, all rights reserved

#pragma once

#include "gtest/gtest.h"

#include "memorydb/client.h"
#include "storage/db_interface.h"
#include "rocksdb/client.h"

#include <memory>
#include <sstream>
#include <string>
#include <thread>

#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
#error "Missing filesystem support"
#endif

inline const auto rocksDbPathPrefix = std::string{"/tmp/sparse_merkle_storage_test_rocksdb"};

// Support multithreaded runs by appending the thread ID to the RocksDB path.
inline std::string rocksDbPath() {
  std::stringstream ss;
  ss << std::this_thread::get_id();
  return rocksDbPathPrefix + ss.str();
}

struct TestMemoryDb {
  static std::shared_ptr<concord::storage::IDBClient> create() {
    return std::make_shared<concord::storage::memorydb::Client>();
  }

  static std::string type() { return "memorydb"; }
};

struct TestRocksDb {
  static std::shared_ptr<::concord::storage::IDBClient> create() {
    fs::remove_all(rocksDbPath());
    // Create the RocksDB client with the default lexicographical comparator.
    return std::make_shared<::concord::storage::rocksdb::Client>(rocksDbPath());
  }

  static std::string type() { return "RocksDB"; }
};

template <typename ParamType>
class ParametrizedTest : public ::testing::TestWithParam<ParamType> {
  void SetUp() override { fs::remove_all(rocksDbPath()); }
  void TearDown() override { fs::remove_all(rocksDbPath()); }
};
