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

#include <future>
#include <iostream>
#include <random>
#include <thread>
#include <vector>
#include <boost/program_options/variables_map.hpp>

#include "categorized_kvbc_msgs.cmf.hpp"
#include "categorization/updates.h"

const std::string kCategoryMerkle = "block_merkle";
const std::string kCategoryVersioned = "ver_test";
const std::string kCategoryImmutable = "imm_test";

namespace concord::kvbc::bench {

namespace po = boost::program_options;
using categorization::BlockMerkleInput;
using categorization::ImmutableUpdates;
using categorization::VersionedUpdates;
using ReadKeys = std::vector<std::string>;

struct InputData {
  std::vector<BlockMerkleInput> block_merkle_input;
  ReadKeys block_merkle_read_keys;
  std::vector<ImmutableUpdates> imm_updates;
  std::vector<VersionedUpdates> ver_updates;
  ReadKeys ver_read_keys;
};

inline size_t blockSize(const po::variables_map& config) {
  // General config
  auto batch_size = config["batch-size"].as<size_t>();

  // BlockMerkle config
  auto key_size_merkle = config["block-merkle-key-size"].as<size_t>();
  auto value_size_merkle = config["block-merkle-value-size"].as<size_t>();
  auto num_adds_merkle = config["num-block-merkle-keys-add"].as<size_t>() * batch_size;
  auto num_deletes_merkle = config["num-block-merkle-keys-delete"].as<size_t>() * batch_size;

  // Immutable config
  auto num_adds_immutable = config["num-immutable-keys-add"].as<size_t>() * batch_size;
  auto key_size_immutable = config["immutable-key-size"].as<size_t>();
  auto value_size_immutable = config["immutable-value-size"].as<size_t>();

  // Versioned KV config
  auto key_size_versioned = config["versioned-key-size"].as<size_t>();
  auto value_size_versioned = config["versioned-value-size"].as<size_t>();
  auto num_adds_versioned = config["num-versioned-keys-add"].as<size_t>() * batch_size;
  auto num_deletes_versioned = config["num-versioned-keys-delete"].as<size_t>() * batch_size;

  const auto merkle_size =
      (key_size_merkle + value_size_merkle) * num_adds_merkle + key_size_merkle * num_deletes_merkle;
  const auto versioned_size =
      (key_size_versioned + value_size_versioned) * num_adds_versioned + key_size_versioned * num_deletes_versioned;
  const auto immutable_size = (key_size_immutable + value_size_immutable) * num_adds_immutable;
  return merkle_size + versioned_size + immutable_size;
}

inline size_t numBlocks(const po::variables_map& config) {
  auto total_blocks = config["total-blocks"].as<size_t>();
  auto max_memory_for_kv = config["max-memory-for-kv-gen"].as<size_t>();
  auto block_size = blockSize(config);
  return std::min(max_memory_for_kv / block_size, total_blocks);
}

// Create block input of a given type by applying UpdateFn in parallel
// read_keys will be filled in by the generator if needed.
template <typename Input, typename UpdateFn>
inline std::vector<Input> parallelInputGen(size_t num_blocks, UpdateFn f) {
  // Run generation in parallel
  auto num_cpus = std::thread::hardware_concurrency();

  // One future per thread
  auto futures = std::vector<std::future<std::vector<Input>>>{};
  futures.reserve(num_cpus);

  // Launch random key-value generation in parallel
  for (auto i = 0u; i < num_cpus; i++) {
    futures.push_back(std::async(std::launch::async, f, i));
  }

  auto rv = std::vector<Input>{};
  rv.reserve(num_blocks);

  // Merge the results
  for (auto i = 0u; i < num_cpus; i++) {
    for (auto&& input : futures[i].get()) {
      rv.emplace_back(std::move(input));
    }
  }
  return rv;
}

template <typename GenRandChar>
inline std::string randKey(GenRandChar rand_char, size_t key_size) {
  auto key = std::string{};
  key.reserve(key_size);
  generate_n(std::back_inserter(key), key_size, rand_char);
  return key;
}

template <typename GenRandChar>
inline std::pair<std::string, std::string> randKvPair(GenRandChar rand_char, size_t key_size, size_t value_size) {
  auto key = std::string{};
  auto val = std::string{};
  key.reserve(key_size);
  val.reserve(value_size);
  generate_n(std::back_inserter(key), key_size, rand_char);
  generate_n(std::back_inserter(val), value_size, rand_char);
  return std::make_pair(key, val);
}

// Return the total number of read keys along with a factory for a generator for a vector of BlockMerkleInput.
inline auto makeBlockMerkleInputGenerator(const po::variables_map& config) {
  auto key_size = config["block-merkle-key-size"].as<size_t>();
  auto value_size = config["block-merkle-value-size"].as<size_t>();
  auto batch_size = config["batch-size"].as<size_t>();
  auto num_adds = config["num-block-merkle-keys-add"].as<size_t>() * batch_size;
  auto num_deletes = config["num-block-merkle-keys-delete"].as<size_t>() * batch_size;
  auto max_read_keys = config["max-total-block-merkle-read-keys"].as<size_t>();

  auto num_blocks = numBlocks(config);
  auto num_cpus = std::thread::hardware_concurrency();
  auto blocks_per_thread = num_blocks / num_cpus;

  // Each thread fills in their subset of keys in a pre-allocated vector. There is no race here, and
  // no need for locks as the buffer segments in each thread are disjoint.
  auto total_read_keys = std::min(num_adds * num_blocks, max_read_keys);
  auto read_keys_per_thread = total_read_keys / num_cpus;

  // Return a factory that takes a reference to readKeys and returns a generator for BlockMerkle input
  auto makeGen = [=](ReadKeys& read_keys) {
    auto gen = [=, &read_keys](size_t thread_id) -> std::vector<BlockMerkleInput> {
      static thread_local std::mt19937 generator;
      auto distribution = std::uniform_int_distribution<unsigned short>(0, 255);
      auto rand_char = [&]() -> uint8_t { return static_cast<uint8_t>(distribution(generator)); };
      size_t read_key_start = thread_id * read_keys_per_thread;
      size_t read_key_end = read_key_start + read_keys_per_thread;
      auto output = std::vector<BlockMerkleInput>{};
      output.reserve(blocks_per_thread);
      std::cout << "BlockMerkle thread " << thread_id << " generating " << blocks_per_thread << " blocks with "
                << num_adds + num_deletes << " keys per block and " << read_keys_per_thread << " read keys"
                << std::endl;
      for (auto j = 0u; j < blocks_per_thread; j++) {
        auto input = BlockMerkleInput{};
        for (auto i = 0u; i < num_adds; i++) {
          auto [key, value] = randKvPair(rand_char, key_size, value_size);
          if (read_key_start != read_key_end) {
            read_keys[read_key_start] = key;
            read_key_start++;
          }
          input.kv.emplace(key, value);
        }

        for (auto i = 0u; i < num_deletes; i++) {
          auto key = randKey(rand_char, key_size);
          input.deletes.push_back(key);
        }
        output.push_back(input);
      }
      return output;
    };
    return gen;
  };
  return std::make_pair(total_read_keys, makeGen);
}

// Return the total number of read keys along with a factory for a generator for a vector of VersionedUpdates.
inline auto makeVersionedUpdateGenerator(const po::variables_map& config) {
  auto key_size = config["versioned-key-size"].as<size_t>();
  auto value_size = config["versioned-value-size"].as<size_t>();
  auto batch_size = config["batch-size"].as<size_t>();
  auto num_adds = config["num-versioned-keys-add"].as<size_t>() * batch_size;
  auto num_deletes = config["num-versioned-keys-delete"].as<size_t>() * batch_size;
  auto max_read_keys = config["max-total-versioned-read-keys"].as<size_t>();

  auto num_blocks = numBlocks(config);
  auto num_cpus = std::thread::hardware_concurrency();
  auto blocks_per_thread = num_blocks / num_cpus;

  // Each thread fills in their subset of keys in a pre-allocated vector. There is no race here, and
  // no need for locks as the buffer segments in each thread are disjoint.
  auto total_read_keys = std::min(num_adds * num_blocks, max_read_keys);
  auto read_keys_per_thread = total_read_keys / num_cpus;

  // Return a factory that takes a reference to readKeys and returns a generator for VersionedUpdates input
  auto makeGen = [=](ReadKeys& read_keys) {
    auto gen = [=, &read_keys](size_t thread_id) -> std::vector<VersionedUpdates> {
      static thread_local std::mt19937 generator;
      auto distribution = std::uniform_int_distribution<unsigned short>(0, 255);
      auto rand_char = [&]() -> uint8_t { return static_cast<uint8_t>(distribution(generator)); };
      size_t read_key_start = thread_id * read_keys_per_thread;
      size_t read_key_end = read_key_start + read_keys_per_thread;
      auto output = std::vector<VersionedUpdates>{};
      output.reserve(blocks_per_thread);
      std::cout << "Versioned thread " << thread_id << " generating " << blocks_per_thread << " blocks with "
                << num_adds + num_deletes << " keys per block and " << read_keys_per_thread << " read keys"
                << std::endl;
      for (auto j = 0u; j < blocks_per_thread; j++) {
        auto updates = VersionedUpdates{};
        for (auto i = 0u; i < num_adds; i++) {
          auto [key, val] = randKvPair(rand_char, key_size, value_size);
          if (read_key_start != read_key_end) {
            read_keys[read_key_start] = key;
            read_key_start++;
          }
          updates.addUpdate(std::move(key), std::move(val));
        }

        for (auto i = 0u; i < num_deletes; i++) {
          auto key = randKey(rand_char, key_size);
          updates.addDelete(std::move(key));
        }
        output.emplace_back(std::move(updates));
      }
      return output;
    };
    return gen;
  };
  return std::make_pair(total_read_keys, makeGen);
}

// Return a generator that will create a vector of ImmutableUpdates based on the given configuration.
inline auto makeImmutableUpdateGenerator(const po::variables_map& config) {
  auto key_size = config["immutable-key-size"].as<size_t>();
  auto value_size = config["immutable-value-size"].as<size_t>();
  auto batch_size = config["batch-size"].as<size_t>();
  auto num_adds = config["num-immutable-keys-add"].as<size_t>() * batch_size;

  auto num_blocks = numBlocks(config);
  auto num_cpus = std::thread::hardware_concurrency();
  auto blocks_per_thread = num_blocks / num_cpus;

  auto gen = [=](size_t thread_id) -> std::vector<ImmutableUpdates> {
    static thread_local std::mt19937 generator;
    auto distribution = std::uniform_int_distribution<unsigned short>(0, 255);
    auto rand_char = [&]() -> uint8_t { return static_cast<uint8_t>(distribution(generator)); };
    auto output = std::vector<ImmutableUpdates>{};
    output.reserve(blocks_per_thread);
    std::cout << "Immutable thread " << thread_id << " generating " << blocks_per_thread << " blocks with " << num_adds
              << " keys per block." << std::endl;
    for (auto j = 0u; j < blocks_per_thread; j++) {
      auto updates = ImmutableUpdates{};
      for (auto i = 0u; i < num_adds; i++) {
        auto [key, value] = randKvPair(rand_char, key_size, value_size);
        updates.addUpdate(std::move(key), ImmutableUpdates::ImmutableValue{std::move(value), {}});
      }
      output.emplace_back(std::move(updates));
    }
    return output;
  };
  return gen;
}

inline InputData createBlockInput(const po::variables_map& config) {
  auto num_blocks = numBlocks(config);
  std::cout << "Generated Input Data for " << num_blocks << " blocks, with block size = " << blockSize(config)
            << " bytes." << std::endl;

  auto data = InputData{};

  // Create BlockMerkle Input
  auto [total_read_keys_merkle, make_block_merkle_gen] = makeBlockMerkleInputGenerator(config);
  data.block_merkle_read_keys = ReadKeys(total_read_keys_merkle);
  data.block_merkle_input =
      parallelInputGen<BlockMerkleInput>(num_blocks, make_block_merkle_gen(data.block_merkle_read_keys));

  // Create Versioned KeyValue input
  auto [total_read_keys_versioned, make_versioned_gen] = makeVersionedUpdateGenerator(config);
  data.ver_read_keys = ReadKeys(total_read_keys_versioned);
  data.ver_updates = parallelInputGen<VersionedUpdates>(num_blocks, make_versioned_gen(data.ver_read_keys));

  auto immutable_gen = makeImmutableUpdateGenerator(config);
  data.imm_updates = parallelInputGen<ImmutableUpdates>(num_blocks, immutable_gen);

  return data;
}

}  // namespace concord::kvbc::bench
