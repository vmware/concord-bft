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

#pragma once

#include <vector>
#include <boost/program_options/variables_map.hpp>

#include "categorized_kvbc_msgs.cmf.hpp"

namespace concord::kvbc::bench {

namespace po = boost::program_options;
using categorization::BlockMerkleInput;
using ReadKeys = std::vector<std::string>;

struct InputData {
  std::vector<BlockMerkleInput> block_merkle_input;
  ReadKeys block_merkle_read_keys;
};

InputData createBlockInput(const po::variables_map& config) {
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
  std::cout << "Generating blocks in parallel across " << num_cpus << " cpus" << std::endl;
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
  return InputData{blocks, read_keys};
}

}  // namespace concord::kvbc::bench
