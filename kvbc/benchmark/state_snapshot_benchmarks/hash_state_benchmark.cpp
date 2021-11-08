// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "assertUtils.hpp"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "categorization/column_families.h"
#include "categorization/details.h"
#include "hex_tools.h"
#include "kv_types.hpp"
#include "rocksdb/native_client.h"
#include "sha_hash.hpp"

#include <boost/program_options.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>

#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

namespace po = boost::program_options;

using concord::kvbc::categorization::detail::BLOCK_MERKLE_LATEST_KEY_VERSION_CF;
using concord::kvbc::categorization::detail::BLOCK_MERKLE_KEYS_CF;
using namespace concordUtils;
using namespace concord::kvbc;
using namespace concord::kvbc::categorization;
using namespace concord::kvbc::categorization::detail;
using namespace concord::storage::rocksdb;
using namespace concord::util;

namespace benchmark {

std::pair<po::options_description, po::variables_map> parseArgs(int argc, char* argv[]) {
  auto desc = po::options_description("Allowed options");

  // clang-format off
  desc.add_options()
    ("help", "Show usage.")

    ("rocksdb-path",
      po::value<std::string>(),
      "The path to the RocksDB data directory.")

    ("rocksdb-cache-size",
      po::value<std::int64_t>()->default_value(4294967296), // 4GB
      "RocksDB block cache size in bytes.")

    ("multiget-batch-size",
      po::value<std::int64_t>()->default_value(100),
      "The number of values read at once via RocksDB multiGet().")

    ("report-progress-key-count",
      po::value<std::int64_t>()->default_value(100000),
      "Report progress periodically after that much keys have been iterated.")

    ("rocksdb-config-file",
      po::value<std::string>(),
      "The path to the RocksDB configuration file.");
  // clang-format on

  auto config = po::variables_map{};
  po::store(po::parse_command_line(argc, argv, desc), config);
  po::notify(config);
  return std::make_pair(desc, config);
}

void completeRocksdbConfiguration(::rocksdb::Options& db_options,
                                  std::vector<::rocksdb::ColumnFamilyDescriptor>& cf_descs,
                                  size_t cache_size) {
  auto table_options = ::rocksdb::BlockBasedTableOptions{};
  table_options.block_cache = ::rocksdb::NewLRUCache(cache_size);
  table_options.filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10, false));
  db_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Use the same block cache and table options for all column familes for now.
  for (auto& d : cf_descs) {
    auto* cf_table_options =
        reinterpret_cast<::rocksdb::BlockBasedTableOptions*>(d.options.table_factory->GetOptions());
    cf_table_options->block_cache = table_options.block_cache;
    cf_table_options->filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10, false));
  }
}

class Time {
 public:
  auto elapsed() const { return std::chrono::steady_clock::now() - start_; }
  auto elapsedSeconds() const { return std::chrono::duration_cast<std::chrono::seconds>(elapsed()).count(); }

 private:
  const std::chrono::steady_clock::time_point start_ = std::chrono::steady_clock::now();
};

template <typename T>
auto hash(const T& buf) {
  return SHA2_256{}.digest(buf.data(), buf.size());
}

struct PerformanceReport {
  PerformanceReport(std::uint64_t bytes_read, std::int64_t elapsed_sec)
      : mb_read{bytes_read / 1024 / 1024}, mb_per_sec{mb_read / (elapsed_sec <= 0 ? 1 : elapsed_sec)} {}

  std::uint64_t mb_read{0};
  std::uint64_t mb_per_sec{0};
};

// As iterating, form a blockchain:
//
//  h0 = hash("a")
//  h1 = hash(h0 || hash(key1) || value1)
//  h2 = hash(h1 || hash(key2) || value2)
//  ...
//  hN = hash(hN-1 || hash(keyN) || valueN)
//
// where hash is SHA2-256 and || means concatenation.
//
// Note that keys are ordered lexicographically on key hash and not the key itself. This is about to change in a future
// commit when the BLOCK_MERKLE_LATEST_KEY_VERSION_CF column family starts using keys instead of key hashes.
int run(int argc, char* argv[]) {
  const auto [desc, config] = parseArgs(argc, argv);

  if (config.count("help")) {
    std::cout << desc << std::endl;
    return EXIT_SUCCESS;
  }

  if (config["rocksdb-path"].empty() || config["rocksdb-config-file"].empty()) {
    std::cerr << desc << std::endl;
    return EXIT_FAILURE;
  }

  const auto rocksdb_path = config["rocksdb-path"].as<std::string>();
  const auto multiget_batch_size = config["multiget-batch-size"].as<std::int64_t>();
  const auto rocksdb_cache_size = config["rocksdb-cache-size"].as<std::int64_t>();
  const auto report_key_count = config["report-progress-key-count"].as<std::int64_t>();
  const auto rocksdb_conf = config["rocksdb-config-file"].as<std::string>();

  if (multiget_batch_size < 1) {
    std::cerr << "multiget-batch-size must be greater than or equal to 1" << std::endl;
    return EXIT_FAILURE;
  } else if (rocksdb_cache_size < 8192) {
    std::cerr << "rocksdb-cache-size must be greater than or equal to 8192" << std::endl;
    return EXIT_FAILURE;
  } else if (report_key_count < 1) {
    std::cerr << "report-progress-key-count must be greater than or equal to 1" << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << "Hashing state with a multiGet() batch size = " << multiget_batch_size
            << ", RocksDB block cache size = " << rocksdb_cache_size << " bytes, configuration file = " << rocksdb_conf
            << ", DB path = " << rocksdb_path << std::endl;

  auto completeInit = [rocksdb_cache_size](auto& db_options, auto& cf_descs) {
    completeRocksdbConfiguration(db_options, cf_descs, rocksdb_cache_size);
  };

  auto opts = NativeClient::UserOptions{rocksdb_conf, completeInit};
  const auto read_only = true;
  auto db = NativeClient::newClient(config["rocksdb-path"].as<std::string>(), read_only, opts);

  // Start with an arbitrary hash.
  auto current_hash = SHA2_256{}.digest("a", 1);
  const auto time = Time{};
  auto multiget_serialized_keys = std::vector<Buffer>{};
  auto multiget_keys = std::vector<VersionedKey>{};
  auto value_slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};
  auto iterated = 0ull;
  auto bytes_read = 0ull;
  auto deleted = 0ull;

  auto print_report = [&time, &bytes_read, &iterated, &deleted]() {
    const auto elapsed_sec = time.elapsedSeconds();
    const auto report = PerformanceReport{bytes_read, elapsed_sec};
    std::cout << "elapsed (" << elapsed_sec << "sec = " << elapsed_sec / 60 << "min), iterated keys = " << iterated
              << ", deleted keys = " << deleted << ", MB read = " << report.mb_read
              << ", MB/sec = " << report.mb_per_sec << std::endl;
  };

  auto hash_batch = [&]() {
    if (multiget_serialized_keys.empty()) {
      return;
    }

    db->multiGet(BLOCK_MERKLE_KEYS_CF, multiget_serialized_keys, value_slices, statuses);
    for (auto i = 0ull; i < value_slices.size(); ++i) {
      ConcordAssert(statuses[i].ok());
      bytes_read += (multiget_serialized_keys[i].size() + value_slices[i].size());
      auto h = SHA2_256{};
      h.init();
      h.update(current_hash.data(), current_hash.size());
      h.update(multiget_keys[i].key_hash.value.data(), multiget_keys[i].key_hash.value.size());
      h.update(value_slices[i].data(), value_slices[i].size());
      current_hash = h.finish();
    }
    multiget_serialized_keys.clear();
    multiget_keys.clear();
    value_slices.clear();
    statuses.clear();
  };

  auto it = db->getIterator(BLOCK_MERKLE_LATEST_KEY_VERSION_CF);
  it.first();
  while (it) {
    auto ver_key = VersionedKey{};

    const auto key_view = it.keyView();
    const auto value_view = it.valueView();
    ConcordAssertEQ(key_view.size(), ver_key.key_hash.value.size());
    ConcordAssertEQ(value_view.size(), sizeof(BlockId));

    bytes_read += (key_view.size() + value_view.size());

    // Fill in the versioned key that we will use for lookup in the BLOCK_MERKLE_KEYS_CF column family.
    std::copy(key_view.cbegin(), key_view.cend(), ver_key.key_hash.value.begin());

    // Get the key version.
    auto version = LatestKeyVersion{};
    deserialize(value_view, version);
    const auto tagged_version = TaggedVersion{version.block_id};

    // Move the iterator.
    it.next();
    iterated++;
    if (iterated % report_key_count == 0) {
      print_report();
    }

    // If the key is deleted, we won't hash it and we skip it.
    if (tagged_version.deleted) {
      ++deleted;
      continue;
    }

    ver_key.version = tagged_version.version;
    multiget_serialized_keys.push_back(serialize(ver_key));
    multiget_keys.push_back(ver_key);

    if (multiget_serialized_keys.size() == static_cast<std::uint32_t>(multiget_batch_size)) {
      hash_batch();
    }
  }

  // Hash any leftovers in the last batch.
  hash_batch();

  std::cout << std::endl << "Completed with a multiGet() batch size = " << multiget_batch_size << std::endl;
  print_report();
  std::cout << "state hash = " << bufferToHex(current_hash.data(), current_hash.size()) << std::endl;

  return EXIT_SUCCESS;
}

}  // namespace benchmark

int main(int argc, char* argv[]) {
  try {
    return benchmark::run(argc, argv);
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Unknown error" << std::endl;
  }
  return EXIT_FAILURE;
}
