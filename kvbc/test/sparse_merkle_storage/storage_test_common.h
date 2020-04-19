// Copyright 2020 VMware, all rights reserved

#pragma once

#include "gtest/gtest.h"

#include "block_digest.h"
#include "kv_types.hpp"
#include "memorydb/client.h"
#include "rocksdb/client.h"
#include "sliver.hpp"
#include "sparse_merkle/base_types.h"
#include "storage/db_interface.h"

#include <cstdint>
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

#ifdef USE_ROCKSDB
inline const auto rocksDbPathPrefix = std::string{"/tmp/sparse_merkle_storage_test_rocksdb"};

// Support multithreaded runs by appending the thread ID to the RocksDB path.
inline std::string rocksDbPath() {
  std::stringstream ss;
  ss << std::this_thread::get_id();
  return rocksDbPathPrefix + ss.str();
}

inline void cleanup() { fs::remove_all(rocksDbPath()); }
#else
inline void cleanup() {}
#endif

inline ::concord::kvbc::BlockDigest blockDigest(concord::kvbc::BlockId blockId, const concordUtils::Sliver &block) {
  return ::bftEngine::SimpleBlockchainStateTransfer::computeBlockDigest(blockId, block.data(), block.length());
}

struct TestMemoryDb {
  static std::shared_ptr<concord::storage::IDBClient> create() {
    cleanup();
    auto db = std::make_shared<concord::storage::memorydb::Client>();
    db->init();
    return db;
  }

  static std::string type() { return "memorydb"; }
};

#ifdef USE_ROCKSDB
struct TestRocksDb {
  static std::shared_ptr<::concord::storage::IDBClient> create() {
    cleanup();
    // Create the RocksDB client with the default lexicographical comparator.
    auto db = std::make_shared<::concord::storage::rocksdb::Client>(rocksDbPath());
    db->init();
    return db;
  }

  static std::string type() { return "RocksDB"; }
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

inline auto getHash(const std::string &str) {
  auto hasher = ::concord::kvbc::sparse_merkle::Hasher{};
  return hasher.hash(str.data(), str.size());
}

inline auto getHash(const concordUtils::Sliver &sliver) {
  auto hasher = ::concord::kvbc::sparse_merkle::Hasher{};
  return hasher.hash(sliver.data(), sliver.length());
}

inline auto getBlockDigest(const std::string &data) { return getHash(data).dataArray(); }
inline concordUtils::Sliver getSliverOfSize(std::size_t size, char content = 'a') { return std::string(size, content); }

inline const auto defaultBlockId = ::concord::kvbc::BlockId{42};
inline const auto defaultData = std::string{"defaultData"};
inline const auto defaultSliver = concordUtils::Sliver::copy(defaultData.c_str(), defaultData.size());
inline const auto maxNumKeys = 16u;
