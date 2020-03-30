// Copyright 2020 VMware, all rights reserved

#pragma once

#include "gtest/gtest.h"

#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "kv_types.hpp"
#include "memorydb/client.h"
#include "rocksdb/client.h"
#include "storage/db_interface.h"

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

inline ::bftEngine::SimpleBlockchainStateTransfer::StateTransferDigest blockDigest(concord::kvbc::BlockId blockId,
                                                                                   const concordUtils::Sliver &block) {
  namespace st = ::bftEngine::SimpleBlockchainStateTransfer;
  auto digest = st::StateTransferDigest{};
  st::computeBlockDigest(blockId, block.data(), block.length(), &digest);
  return digest;
}

struct TestMemoryDb {
  static std::shared_ptr<concord::storage::IDBClient> create() {
    cleanup();
    return std::make_shared<concord::storage::memorydb::Client>();
  }

  static std::string type() { return "memorydb"; }
};

#ifdef USE_ROCKSDB
struct TestRocksDb {
  static std::shared_ptr<::concord::storage::IDBClient> create() {
    cleanup();
    // Create the RocksDB client with the default lexicographical comparator.
    return std::make_shared<::concord::storage::rocksdb::Client>(rocksDbPath());
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
