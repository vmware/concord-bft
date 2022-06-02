// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
//

#define KVBCBENCH 1

#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>

#include <boost/program_options.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <rocksdb/db.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/perf_context.h>

#include "categorization/base_types.h"
#include "categorization/column_families.h"
#include "categorization/updates.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "kvbc_adapter/replica_adapter.hpp"
#include "performance_handler.h"
#include "rocksdb/native_client.h"
#include "diagnostics.h"
#include "diagnostics_server.h"
#include "input.h"
#include "pre_execution.h"
#include "ReplicaResources.h"

using namespace std;

namespace concord::kvbc::bench {

using categorization::TaggedVersion;

// Since we generate all keys up front we need to prevent explosive memory growth.
//
// This limits overall entropy. We can make it more advanced by generating new keys in a separate
// threaad and atomically swapping when needed by block addition. For now we keep it simple.
static constexpr size_t MAX_MEMORY_SIZE_FOR_KV_DEFAULT = 1024 * 1024 * 1024 * 4ul;  // 4GB

static constexpr size_t CACHE_SIZE_DEFAULT = 1024 * 1024 * 1024 * 16ul;  // 16 GB

std::pair<po::options_description, po::variables_map> parseArgs(int argc, char** argv) {
  auto desc = po::options_description("Allowed options");
  // clang-format off
  desc.add_options()
    ("help", "show usage")

    /*********************************
     General Config
     *********************************/
    ("rocksdb-path",
     po::value<std::string>()->default_value("./rocksdbdata"s),
     "The location of the rocksdb data directory")

    ("batch-size",
      po::value<size_t>()->default_value(1),
      "The multiple of keys and values stored across all categories per block")

    ("total-blocks",
     po::value<size_t>()->default_value(1000)->notifier([] (size_t v) {
       if (v < std::thread::hardware_concurrency()) {
          throw po::validation_error{po::validation_error::invalid_option_value, "total-blocks", std::to_string(v)};
       }}),
     "Number of total blocks to add during the test. Must be at least the number of CPUs in the system.")

    ("stats-dump-period-in-blocks",
    po::value<size_t>()->default_value(10000),
    "The number of blocks to output RocksDB statistics.")

    ("max-memory-for-kv-gen",
    po::value<size_t>()->default_value(MAX_MEMORY_SIZE_FOR_KV_DEFAULT),
    "Maximum amount of memory to allocate for random kv pairs during key-value generation")

    ("pre-execution-concurrency",
    po::value<size_t>()->default_value(10),
    "Amount of pre-execution tasks to run during block generation")

    ("pre-execution-delay-ms",
    po::value<size_t>()->default_value(50),
    "Time it takes to run a pre-execution request.")

    ("rocksdb-cache-size",
    po::value<size_t>()->default_value(CACHE_SIZE_DEFAULT),
    "Rocksdb Block Cache size")

    /*********************************
     Block Merkle Category Config
     *********************************/
    ("num-block-merkle-keys-add",
     po::value<size_t>()->default_value(12),
     "Number of block merkle keys added to a block")

    ("num-block-merkle-keys-delete",
     po::value<size_t>()->default_value(5),
     "Number of block merkle keys to delete in a block")

    ("block-merkle-key-size",
     po::value<size_t>()->default_value(100),
     "Size of a block merkle key in bytes")

    ("block-merkle-value-size",
    po::value<size_t>()->default_value(500),
    "Size of a block merkle value in bytes")

    ("max-total-block-merkle-read-keys",
    po::value<size_t>()->default_value(1024*100),
    "Total number of keys to keep in memory for random reads and conflict detection.")

    ("num-block-merkle-read-keys-per-transaction",
    po::value<size_t>()->default_value(50),
    "The number of keys to read during (pre-)execution and conflict detection for the BlockMerkle category")

    /*********************************
     Immutable Category Config
     Note that immutable keys are never read during pre-execution or checked for conflicts.
     *********************************/
    ("num-immutable-keys-add",
     po::value<size_t>()->default_value(4),
     "Number of immutable keys added to a block")

    ("immutable-key-size",
     po::value<size_t>()->default_value(100),
     "Size of a immutable key in bytes")

    ("immutable-value-size",
    po::value<size_t>()->default_value(1500),
    "Size of a immutable value in bytes")

    /*********************************
     Versioned KV Category Config
     *********************************/
    ("num-versioned-keys-add",
     po::value<size_t>()->default_value(10),
     "Number of versioned keys added to a block")

    ("num-versioned-keys-delete",
     po::value<size_t>()->default_value(0),
     "Number of versioned keys to delete in a block")

    ("versioned-key-size",
     po::value<size_t>()->default_value(100),
     "Size of a versioned key in bytes")

    ("versioned-value-size",
    po::value<size_t>()->default_value(500),
    "Size of a versioned value in bytes")

    ("max-total-versioned-read-keys",
    po::value<size_t>()->default_value(1024*100),
    "Total number of versioned keys to keep in memory for random reads and conflict detection.")

    ("num-versioned-read-keys-per-transaction",
    po::value<size_t>()->default_value(50),
    "The number of versioned keys to read during (pre-)execution and conflict detection.");

  // clang-format on

  auto config = po::variables_map{};
  po::store(po::parse_command_line(argc, argv, desc), config);
  po::notify(config);
  return std::make_pair(desc, config);
}

void printHistograms() {
  auto& registrar = diagnostics::RegistrarSingleton::getInstance();
  registrar.perf.snapshot("bench");
  auto data = registrar.perf.get("bench");
  cout << registrar.perf.toString(data) << endl;
}

void printRocksDbProperty(std::shared_ptr<storage::rocksdb::NativeClient>& db,
                          ::rocksdb::ColumnFamilyHandle* cf_handle,
                          std::string& property) {
  std::string out;
  db->rawDB().GetProperty(cf_handle, property, &out);
  cout << "    " << property << ": " << out << endl;
}

void printRocksDbProperties(std::shared_ptr<storage::rocksdb::NativeClient>& db,
                            ::rocksdb::ColumnFamilyHandle* cf_handle) {
  for (auto& prop : std::array{"rocksdb.block-cache-capacity"s,
                               "rocksdb.block-cache-usage"s,
                               "rocksdb.block-cache-pinned-usage"s,
                               "rocksdb.estimate-table-readers-mem"s,
                               "rocksdb.cur-size-all-mem-tables"s,
                               "rocksdb.size-all-mem-tables"s,
                               "rocksdb.block-cache-pinned-usage"s,
                               "rocksdb.num-running-compactions"s,
                               "rocksdb.background-errors"s,
                               "rocksdb.estimate-num-keys"s,
                               "rocksdb.estimate-table-readers-mem"s,
                               "rocksdb.num-snapshots"s,
                               "rocksdb.num-live-versions"s,
                               "rocksdb.estimate-live-data-size"s,
                               "rocksdb.live-sst-files-size"s,
                               "rocksdb.options-statistics"s,
                               "rocksdb.stats"s}) {
    printRocksDbProperty(db, cf_handle, prop);
  }
}

void printRocksDbProperties(std::shared_ptr<storage::rocksdb::NativeClient>& db) {
  cout << "RocksDB Properties: " << endl;
  for (auto& cf : db->columnFamilies()) {
    cout << "  Column Family: " << cf << endl;
    printRocksDbProperties(db, db->columnFamilyHandle(cf));
  }
}

std::shared_ptr<rocksdb::Statistics> completeRocksdbConfiguration(
    ::rocksdb::Options& db_options, std::vector<::rocksdb::ColumnFamilyDescriptor>& cf_descs, size_t cache_size) {
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
  return db_options.statistics;
}

size_t numMerkleVersionsToRead(const po::variables_map& config, size_t num_read_keys) {
  return std::min(config["num-block-merkle-read-keys-per-transaction"].as<size_t>(), num_read_keys);
}
size_t numVersionedVersionsToRead(const po::variables_map& config, size_t num_read_keys) {
  return std::min(config["num-versioned-read-keys-per-transaction"].as<size_t>(), num_read_keys);
}

PreExecConfig preExecConfig(const po::variables_map& config,
                            size_t num_merkle_read_keys,
                            size_t num_versioned_read_keys) {
  auto pre_exec_config = PreExecConfig{};
  pre_exec_config.concurrency = config["pre-execution-concurrency"].as<size_t>();
  pre_exec_config.delay = std::chrono::milliseconds(config["pre-execution-delay-ms"].as<size_t>());
  pre_exec_config.num_block_merkle_keys_to_read = numMerkleVersionsToRead(config, num_merkle_read_keys);
  pre_exec_config.num_versioned_keys_to_read = numVersionedVersionsToRead(config, num_versioned_read_keys);
  return pre_exec_config;
}

void addBlocks(const po::variables_map& config,
               std::shared_ptr<storage::rocksdb::NativeClient>& db,
               adapter::ReplicaBlockchain& kvbc,
               InputData& input,
               std::shared_ptr<diagnostics::Recorder>& add_block_recorder,
               std::shared_ptr<diagnostics::Recorder>& conflict_detection_recorder) {
  auto stats_dump_period_in_blocks = config["stats-dump-period-in-blocks"].as<size_t>();
  auto total_blocks = config["total-blocks"].as<size_t>();
  auto generated_input_blocks = numBlocks(config);
  auto num_merkle_versions_to_read = numMerkleVersionsToRead(config, input.block_merkle_read_keys.size());
  auto num_versioned_versions_to_read = numVersionedVersionsToRead(config, input.ver_read_keys.size());

  if (total_blocks > generated_input_blocks) {
    std::cout << "More memory needed than allocated. Reusing generated blocks. This requires copying."
              << KVLOG(total_blocks, generated_input_blocks);
  }

  auto batch_size = config["batch-size"].as<size_t>();
  for (auto i = 1u; i <= total_blocks; i++) {
    // Print Memory Stats every 10k blocks
    if (i % stats_dump_period_in_blocks == 0) {
      cout << "Adding Block " << i << endl;
      printRocksDbProperties(db);
    }
    // We need to read a set of key versions for each request in a block
    for (auto j = 0u; j <= batch_size; j++) {
      // Generate a random offset in the read_keys and then create vector to pass in.
      auto merkle_start = randomReadIter(input.block_merkle_read_keys, num_merkle_versions_to_read);
      auto merkle_conflict_keys = std::vector<std::string>(merkle_start, merkle_start + num_merkle_versions_to_read);
      auto versioned_start = randomReadIter(input.ver_read_keys, num_versioned_versions_to_read);
      auto versioned_conflict_keys =
          std::vector<std::string>(versioned_start, versioned_start + num_versioned_versions_to_read);

      {
        diagnostics::TimeRecorder<> guard(*conflict_detection_recorder);
        // Simulate a conflict detection check
        auto merkle_versions = std::vector<std::optional<TaggedVersion>>{};
        auto versioned_versions = std::vector<std::optional<TaggedVersion>>{};
        kvbc.multiGetLatestVersion(kCategoryMerkle, merkle_conflict_keys, merkle_versions);
        kvbc.multiGetLatestVersion(kCategoryVersioned, versioned_conflict_keys, versioned_versions);
      }
    }

    {
      diagnostics::TimeRecorder<> guard(*add_block_recorder);
      auto updates = categorization::Updates{};

      // Unfortunately we must copy if total_blocks > number of input blocks generated.
      if (total_blocks > generated_input_blocks) {
        auto index = (i - 1) % generated_input_blocks;
        auto merkle_input = input.block_merkle_input[index];
        auto versioned_updates = input.ver_updates[index];
        auto immutable_updates = input.imm_updates[index];
        updates.add(kCategoryMerkle, categorization::BlockMerkleUpdates(std::move(merkle_input)));
        updates.add(kCategoryImmutable, std::move(immutable_updates));
        updates.add(kCategoryVersioned, std::move(versioned_updates));
        kvbc.add(std::move(updates));
      } else {
        auto&& merkle_input = std::move(input.block_merkle_input[i - 1]);
        updates.add(kCategoryMerkle, categorization::BlockMerkleUpdates(std::move(merkle_input)));
        kvbc.add(std::move(updates));
      }
    }
  }
}

}  // namespace concord::kvbc::bench

using namespace concord::kvbc::bench;
using namespace concord;

int main(int argc, char** argv) {
  auto& registrar = diagnostics::RegistrarSingleton::getInstance();
  DEFINE_SHARED_RECORDER(add_block_recorder, 1, 500000, 3, diagnostics::Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(conflict_detection_recorder, 1, 100000, 3, diagnostics::Unit::MICROSECONDS);
  registrar.perf.registerComponent("bench", {add_block_recorder, conflict_detection_recorder});
  concord::diagnostics::Server diagnostics_server;

  try {
    auto [desc, config] = parseArgs(argc, argv);

    if (config.count("help")) {
      cout << desc << endl;
      return 1;
    }

    diagnostics_server.start(registrar, INADDR_ANY, 6888);

    cout << "Starting Input Data Generation..." << endl;
    auto start = std::chrono::steady_clock::now();
    auto input = createBlockInput(config);
    auto end = std::chrono::steady_clock::now();
    cout << "Input Data Generation completed in " << chrono::duration_cast<chrono::seconds>(end - start).count()
         << " seconds." << endl;

    auto rocksdb_stats = std::shared_ptr<::rocksdb::Statistics>{};
    auto rocksdb_cache_size = config["rocksdb-cache-size"].as<size_t>();
    auto completeInit = [&rocksdb_stats, rocksdb_cache_size](auto& db_options, auto& cf_descs) {
      rocksdb_stats = completeRocksdbConfiguration(db_options, cf_descs, rocksdb_cache_size);
    };
    auto opts = storage::rocksdb::NativeClient::UserOptions{"kvbcbench_rocksdb_opts.ini", completeInit};
    auto db = storage::rocksdb::NativeClient::newClient(config["rocksdb-path"].as<std::string>(), false, opts);
    auto kvbc =
        kvbc::adapter::ReplicaBlockchain(db,
                                         false,
                                         std::map<std::string, kvbc::categorization::CATEGORY_TYPE>{
                                             {kCategoryMerkle, kvbc::categorization::CATEGORY_TYPE::block_merkle},
                                             {kCategoryImmutable, kvbc::categorization::CATEGORY_TYPE::immutable},
                                             {kCategoryVersioned, kvbc::categorization::CATEGORY_TYPE::versioned_kv}});

    auto pre_exec_config = preExecConfig(config, input.block_merkle_read_keys.size(), input.ver_read_keys.size());
    auto pre_exec_sim = PreExecutionSimulator(pre_exec_config, input.block_merkle_read_keys, input.ver_read_keys, kvbc);
    pre_exec_sim.start();

    cout << "Starting to Add Blocks..." << endl;
    start = std::chrono::steady_clock::now();
    addBlocks(config, db, kvbc, input, add_block_recorder, conflict_detection_recorder);
    end = std::chrono::steady_clock::now();
    auto add_block_duration = chrono::duration_cast<chrono::milliseconds>(end - start).count();
    cout << "Adding blocks completed in = " << add_block_duration / 1000.0 << " seconds" << endl << endl;

    pre_exec_sim.stop();

    printRocksDbProperties(db);
    printHistograms();

    cout << "Avg. Throughput = " << config["total-blocks"].as<size_t>() / (add_block_duration / 1000.0) << " blocks/s"
         << endl;
  } catch (exception& e) {
    diagnostics_server.stop();
    cerr << e.what() << endl;
    return -1;
  }

  diagnostics_server.stop();
  return 0;
}
