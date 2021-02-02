#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <random>

#include <boost/program_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <rocksdb/db.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>

#include "categorization/base_types.h"
#include "categorization/column_families.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "categorization/block_merkle_category.h"
#include "performance_handler.h"
#include "rocksdb/native_client.h"
#include "diagnostics.h"
#include "diagnostics_server.h"

using namespace std;
namespace po = boost::program_options;

namespace concord::kvbc::bench {

using categorization::BlockMerkleInput;
using categorization::TaggedVersion;

using ReadKeys = std::vector<std::string>;

// Since we generate all keys up front we need to prevent explosive memory growth.
//
// This limits overall entropy. We can make it more advanced by generating new keys in a separate
// threaad and atomically swapping when needed by block addition. For now we keep it simple.
static constexpr size_t MAX_MEMORY_SIZE_FOR_KV_DEFAULT = 1024 * 1024 * 1024;  // 1GB

po::variables_map parseArgs(int argc, char** argv) {
  auto desc = po::options_description("Allowed options");
  // clang-format off
  desc.add_options()
    ("rocksdb-path",
     po::value<std::string>()->default_value("./rocksdbdata"s),
     "The location of the rocksdb data directory")

    ("num-block-merkle-keys-add",
     po::value<size_t>()->default_value(25),
     "Number of block merkle keys added to a block")

    ("num-block-merkle-keys-delete",
     po::value<size_t>()->default_value(5),
     "Number of block merkle keys to delete in a block")

    ("batch-size",
      po::value<size_t>()->default_value(1),
      "The multiple of keys and values stored across all categories per block")

    ("total-blocks",
     po::value<size_t>()->default_value(1000),
     "Number of total blocks to add during the test.")

    ("block-merkle-key-size",
     po::value<size_t>()->default_value(100),
     "Size of a block merkle key in bytes")

    ("block-merkle-value-size",
    po::value<size_t>()->default_value(1024),
    "Size of a block merkle value in bytes")

    ("max-total-block-merkle-read-keys",
    po::value<size_t>()->default_value(1024*100),
    "Total number of keys to keep in memory for random reads and conflict detection.")

    ("num-block-merkle-read-keys-per-transaction",
    po::value<size_t>()->default_value(100),
    "The number of keys to read during (pre-)execution and conflict detection for the BlockMerkle category")

    ("max-memory-for-kv-gen",
    po::value<size_t>()->default_value(MAX_MEMORY_SIZE_FOR_KV_DEFAULT),
    "Maximum amount of memory to allocate for random kv pairs during key-value generation");

  // clang-format on

  auto config = po::variables_map{};
  po::store(po::parse_command_line(argc, argv, desc), config);
  return config;
}

void printHistograms() {
  auto& registrar = diagnostics::RegistrarSingleton::getInstance();
  registrar.perf.snapshot("bench");
  auto data = registrar.perf.get("bench");
  cout << registrar.perf.toString(data) << endl;
}

std::shared_ptr<rocksdb::Statistics> completeRocksdbConfiguration(
    ::rocksdb::Options& db_options, std::vector<::rocksdb::ColumnFamilyDescriptor>& cf_descs) {
  static constexpr size_t CACHE_SIZE = 1024 * 1024 * 1024 * 4ul;  // 4 GB
  auto table_options = ::rocksdb::BlockBasedTableOptions{};
  table_options.block_cache = ::rocksdb::NewLRUCache(CACHE_SIZE);
  table_options.filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10, false));
  db_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Use the same block cache and table options for all column familes for now.
  for (auto& d : cf_descs) {
    auto* cf_table_options =
        reinterpret_cast<::rocksdb::BlockBasedTableOptions*>(d.options.table_factory->GetOptions());
    cf_table_options->block_cache = table_options.block_cache;
    cf_table_options->filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10, false));

    // We never seek on versions
    if (d.name == categorization::detail::BLOCK_MERKLE_LATEST_KEY_VERSION_CF) {
      d.options.OptimizeForPointLookup(CACHE_SIZE);
    }
  }
  return db_options.statistics;
}

std::pair<std::vector<categorization::BlockMerkleInput>, ReadKeys> createBlockInput(const po::variables_map& config) {
  auto total_blocks = config["total-blocks"].as<size_t>();
  auto key_size = config["block-merkle-key-size"].as<size_t>();
  auto value_size = config["block-merkle-value-size"].as<size_t>();
  auto batch_size = config["batch-size"].as<size_t>();
  auto num_adds = config["num-block-merkle-keys-add"].as<size_t>() * batch_size;
  auto num_deletes = config["num-block-merkle-keys-delete"].as<size_t>() * batch_size;
  auto max_memory_for_kv = config["max-memory-for-kv-gen"].as<size_t>();
  auto max_read_keys = config["max-total-block-merkle-read-keys"].as<size_t>();

  const auto block_size = (key_size + value_size) * num_adds + key_size * num_deletes;
  const size_t num_blocks = std::min(max_memory_for_kv / block_size, total_blocks);
  auto blocks = std::vector<BlockMerkleInput>{};
  blocks.reserve(num_blocks);

  // Run generation in parallel
  auto num_cpus = std::thread::hardware_concurrency();
  cout << "Generating blocks in parallel across " << num_cpus << " cpus" << endl;
  auto blocks_per_thread = num_blocks / num_cpus;

  // Each thread fills in their subset of keys in a pre-allocated vector. There is no race here, and
  // no need for locks as the buffer segments in each thread are disjoint.
  auto total_read_keys = std::min(num_adds * num_blocks, max_read_keys);
  auto read_keys_per_thread = total_read_keys / num_cpus;
  auto read_keys = ReadKeys(total_read_keys);

  auto gen = [&](size_t thread_id) -> std::vector<categorization::BlockMerkleInput> {
    static thread_local std::mt19937 generator;
    auto distribution = std::uniform_int_distribution<unsigned short>(0, 255);
    auto rand_char = [&]() -> uint8_t { return static_cast<uint8_t>(distribution(generator)); };
    size_t read_key_start = thread_id * read_keys_per_thread;
    size_t read_key_end = read_key_start + read_keys_per_thread;
    auto output = std::vector<BlockMerkleInput>{};
    output.reserve(blocks_per_thread);
    for (auto j = 0u; j < blocks_per_thread; j++) {
      auto input = BlockMerkleInput{};
      for (auto i = 0u; i < num_adds; i++) {
        auto key = std::string{};
        auto val = std::string{};
        key.reserve(key_size);
        val.reserve(value_size);
        generate_n(std::back_inserter(key), key_size, rand_char);
        generate_n(std::back_inserter(val), value_size, rand_char);
        if (read_key_start != read_key_end) {
          read_keys[read_key_start] = key;
          read_key_start++;
        }
        input.kv.emplace(key, val);
      }

      for (auto i = 0u; i < num_deletes; i++) {
        auto key = std::string{};
        key.reserve(key_size);
        generate_n(std::back_inserter(key), key_size, rand_char);
        input.deletes.push_back(key);
      }
      output.push_back(input);
    }
    return output;
  };

  auto output_futures = std::vector<std::future<std::vector<categorization::BlockMerkleInput>>>{};
  output_futures.reserve(num_cpus);

  // Launch random key-value generation in parallel
  for (auto i = 0u; i < num_cpus; i++) {
    output_futures.push_back(std::async(std::launch::async, gen, i));
  }
  // Merge the results
  for (auto i = 0u; i < num_cpus; i++) {
    auto batch = output_futures[i].get();
    blocks.insert(blocks.end(), batch.begin(), batch.end());
  }
  return std::make_pair(blocks, read_keys);
}

void addBlocks(const po::variables_map& config,
               std::shared_ptr<storage::rocksdb::NativeClient>& db,
               std::vector<BlockMerkleInput>& input,
               std::vector<string>& read_keys,
               std::shared_ptr<diagnostics::Recorder>& add_block_recorder,
               std::shared_ptr<diagnostics::Recorder>& conflict_detection_recorder) {
  auto cat = categorization::detail::BlockMerkleCategory{db};
  auto total_blocks = config["total-blocks"].as<size_t>();
  auto max_memory_for_kv = config["max-memory-for-kv-gen"].as<size_t>();
  auto num_versions_to_read =
      std::min(config["num-block-merkle-read-keys-per-transaction"].as<size_t>(), read_keys.size());

  const auto input_blocks = input.size();
  if (total_blocks > input_blocks) {
    std::cout << "More memory needed than allocated. Reusing generated blocks. This requires copying."
              << KVLOG(total_blocks, input_blocks, max_memory_for_kv);
  }

  auto max_read_offset = read_keys.size() - num_versions_to_read;
  for (auto i = 1u; i <= total_blocks; i++) {
    // Generate a random offset in the read_keys and then create vector to pass in.
    auto start = read_keys.begin() + (rand() % max_read_offset);
    auto conflict_keys = std::vector<std::string>(start, start + num_versions_to_read);
    {
      diagnostics::TimeRecorder<> guard(*conflict_detection_recorder);
      // Simulate a conflict detection check
      auto versions = std::vector<std::optional<TaggedVersion>>{};
      cat.multiGetLatestVersion(conflict_keys, versions);
    }

    {
      diagnostics::TimeRecorder<> guard(*add_block_recorder);
      auto batch = db->getBatch();

      // Unfortunately we must copy if total_blocks > number of input blocks generated.
      if (total_blocks > input_blocks) {
        auto block = input[(i - 1) % input_blocks];
        cat.add(i, std::move(block), batch);
      } else {
        auto&& block = std::move(input[i - 1]);
        cat.add(i, std::move(block), batch);
      }
      db->write(std::move(batch));
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

  // Start the diagnostics server
  concord::diagnostics::Server diagnostics_server;
  diagnostics_server.start(registrar, INADDR_ANY, 6888);

  try {
    auto config = parseArgs(argc, argv);

    cout << "Starting Input Data Generation..." << endl;
    auto start = std::chrono::steady_clock::now();
    auto [input, read_keys] = createBlockInput(config);
    auto end = std::chrono::steady_clock::now();
    cout << "Input Data Generation completed in " << chrono::duration_cast<chrono::seconds>(end - start).count()
         << " seconds." << endl;

    auto rocksdb_stats = std::shared_ptr<::rocksdb::Statistics>{};
    auto completeInit = [&rocksdb_stats](auto& db_options, auto& cf_descs) {
      rocksdb_stats = completeRocksdbConfiguration(db_options, cf_descs);
    };
    auto opts = storage::rocksdb::NativeClient::UserOptions{"kvbcbench_rocksdb_opts.ini", completeInit};
    auto db = storage::rocksdb::NativeClient::newClient(config["rocksdb-path"].as<std::string>(), false, opts);

    cout << "Starting to Add Blocks..." << endl;
    start = std::chrono::steady_clock::now();
    addBlocks(config, db, input, read_keys, add_block_recorder, conflict_detection_recorder);
    end = std::chrono::steady_clock::now();
    auto add_block_duration = chrono::duration_cast<chrono::milliseconds>(end - start).count();
    cout << "Adding blocks completed in = " << add_block_duration / 1000.0 << " seconds" << endl << endl;

    printHistograms();

    cout << "Avg. Throughput = " << config["total-blocks"].as<size_t>() / (add_block_duration / 1000.0) << " blocks/s"
         << endl;

  } catch (exception& e) {
    diagnostics_server.stop();
    cerr << e.what() << "\n";
    return -1;
  }
  diagnostics_server.stop();
  return 0;
}
