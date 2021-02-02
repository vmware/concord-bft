#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <random>

#include <boost/program_options.hpp>

#include "categorization/base_types.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "categorization/block_merkle_category.h"
#include "rocksdb/native_client.h"
#include "diagnostics.h"
#include "diagnostics_server.h"

using namespace std;
namespace po = boost::program_options;

namespace concord::kvbc::bench {

using categorization::BlockMerkleInput;
using categorization::TaggedVersion;

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

    ("max-memory-for-kv-gen",
    po::value<size_t>()->default_value(MAX_MEMORY_SIZE_FOR_KV_DEFAULT),
    "Maximum amount of memory to allocate for random kv pairs during key-value generation")

    ("conflict-key-divisor",
    po::value<size_t>()->default_value(3),
    "The number of keys in a block to perform conflict detection with");

  // clang-format on

  auto config = po::variables_map{};
  po::store(po::parse_command_line(argc, argv, desc), config);
  return config;
}

std::vector<categorization::BlockMerkleInput> createBlockInput(const po::variables_map& config) {
  auto total_blocks = config["total-blocks"].as<size_t>();
  auto key_size = config["block-merkle-key-size"].as<size_t>();
  auto value_size = config["block-merkle-value-size"].as<size_t>();
  auto batch_size = config["batch-size"].as<size_t>();
  auto num_adds = config["num-block-merkle-keys-add"].as<size_t>() * batch_size;
  auto num_deletes = config["num-block-merkle-keys-delete"].as<size_t>() * batch_size;
  auto max_memory_for_kv = config["max-memory-for-kv-gen"].as<size_t>();

  const auto block_size = (key_size + value_size) * num_adds + key_size * num_deletes;
  const size_t num_blocks = std::min(max_memory_for_kv / block_size, total_blocks);
  auto blocks = std::vector<BlockMerkleInput>{};
  blocks.reserve(num_blocks);

  // Run generation in parallel
  auto num_cpus = std::thread::hardware_concurrency();
  cout << "Generating blocks in parallel across " << num_cpus << " cpus" << endl;
  auto blocks_per_thread = num_blocks / num_cpus;

  auto gen = [&]() -> std::vector<categorization::BlockMerkleInput> {
    static thread_local std::mt19937 generator;
    auto distribution = std::uniform_int_distribution<uint8_t>(1, 255);
    auto rand_char = [&]() -> uint8_t { return distribution(generator); };
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
    output_futures.push_back(std::async(std::launch::async, gen));
  }
  // Merge the results
  for (auto i = 0u; i < num_cpus; i++) {
    auto batch = output_futures[i].get();
    blocks.insert(blocks.end(), batch.begin(), batch.end());
  }
  return blocks;
}

std::vector<std::vector<std::string>> createConflictKeys(const std::vector<BlockMerkleInput>& input,
                                                         size_t conflict_keys_divisor) {
  auto conflict_keys = std::vector<std::vector<std::string>>(input.size());

  auto num_keys = input[0].kv.size() / conflict_keys_divisor;
  cout << "Num Conflict Keys Per Block = " << num_keys << endl;
  for (auto i = 1u; i < input.size(); i++) {
    // Read keys come from the prior block so we actually are looking up keys that exist.
    auto it = input[i - 1].kv.begin();
    for (auto j = 0u; j < num_keys; j++) {
      conflict_keys[i].push_back(it->first);
      it++;
    }
  }

  return conflict_keys;
}

void addBlocks(const po::variables_map& config,
               std::vector<BlockMerkleInput>& input,
               const std::vector<std::vector<string>>& conflict_keys,
               std::shared_ptr<diagnostics::Recorder>& add_block_recorder) {
  auto db = storage::rocksdb::NativeClient::newClient(
      config["rocksdb-path"].as<std::string>(), false, storage::rocksdb::NativeClient::ExistingOptions{});
  auto cat = categorization::detail::BlockMerkleCategory{db};
  auto total_blocks = config["total-blocks"].as<size_t>();
  auto max_memory_for_kv = config["max-memory-for-kv-gen"].as<size_t>();

  const auto input_blocks = input.size();
  if (total_blocks > input_blocks) {
    std::cout << "More memory needed than allocated. Reusing generated blocks. This requires copying."
              << KVLOG(total_blocks, input_blocks, max_memory_for_kv);
  }

  for (auto i = 1u; i <= total_blocks; i++) {
    // Simulate a conflict detection check
    auto versions = std::vector<std::optional<TaggedVersion>>{};
    cat.multiGetLatestVersion(conflict_keys[(i - 1) % conflict_keys.size()], versions);

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
  DEFINE_SHARED_RECORDER(add_block_recorder, 1, 100000, 3, diagnostics::Unit::MICROSECONDS);
  registrar.perf.registerComponent("bench", {add_block_recorder});

  // Start the diagnostics server
  concord::diagnostics::Server diagnostics_server;
  diagnostics_server.start(registrar, INADDR_ANY, 6888);

  try {
    auto config = parseArgs(argc, argv);

    cout << "Starting Input Data Generation..." << endl;

    auto input = std::vector<BlockMerkleInput>{};
    {
      auto start = std::chrono::steady_clock::now();
      input = createBlockInput(config);
      auto end = std::chrono::steady_clock::now();
      cout << "Input Data Generation completed in " << chrono::duration_cast<chrono::seconds>(end - start).count()
           << " seconds." << endl;
    }

    auto conflict_keys = std::vector<std::vector<std::string>>{};
    {
      cout << "Generating Conflict Detection Keys from Input Data" << endl;
      auto conflict_keys_divisor = config["conflict-key-divisor"].as<size_t>();
      auto start = std::chrono::steady_clock::now();
      conflict_keys = createConflictKeys(input, conflict_keys_divisor);
      auto end = std::chrono::steady_clock::now();
      cout << "Conflict Detection Key Generation completed in "
           << chrono::duration_cast<chrono::seconds>(end - start).count() << " seconds." << endl;
    }

    cout << "Starting to Add Blocks..." << endl;
    auto start = std::chrono::steady_clock::now();
    addBlocks(config, input, conflict_keys, add_block_recorder);
    auto end = std::chrono::steady_clock::now();
    auto add_block_duration = chrono::duration_cast<chrono::milliseconds>(end - start).count();

    cout << "Adding blocks completed in = " << add_block_duration / 1000.0 << " seconds" << endl;
    cout << "Avg. Throughput = " << config["total-blocks"].as<size_t>() / (add_block_duration / 1000.0) << " blocks/s"
         << endl;

    auto add_block_histogram = concord::diagnostics::Histogram(add_block_recorder);
    add_block_histogram.takeSnapshot();
    cout << "Add Block:\n" << concord::diagnostics::HistogramData(add_block_histogram) << endl;

  } catch (exception& e) {
    diagnostics_server.stop();
    cerr << e.what() << "\n";
    return -1;
  }
  diagnostics_server.stop();
  return 0;
}
