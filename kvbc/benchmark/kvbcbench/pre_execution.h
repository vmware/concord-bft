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

#include <chrono>
#include <iostream>
#include <thread>

#include "categorization/block_merkle_category.h"
#include "categorization/kv_blockchain.h"
#include "input.h"

namespace concord::kvbc::bench {

struct PreExecConfig {
  size_t concurrency;
  std::chrono::milliseconds delay;
  size_t num_block_merkle_keys_to_read;
  size_t num_versioned_keys_to_read;
};

// We read a number of block merkle keys and versioned keys, and then sleep for a given time to
// simulate pre-execution.
class PreExecutionSimulator {
 public:
  PreExecutionSimulator(const PreExecConfig& config,
                        const ReadKeys& merkle_read_keys,
                        const ReadKeys& versioned_read_keys,
                        categorization::KeyValueBlockchain& kvbc)
      : config_(config), merkle_read_keys_(merkle_read_keys), versioned_read_keys_(versioned_read_keys), kvbc_(kvbc) {}

  void start() {
    std::cout << "Starting Simulated Pre-Execution Reads" << std::endl;
    for (auto i = 0u; i < config_.concurrency; i++) {
      threads_.emplace_back([this]() {
        // Only alloacate the read_keys vector once
        auto merkle_read_keys = std::vector<std::string>{};
        merkle_read_keys.reserve(config_.num_block_merkle_keys_to_read);
        auto merkle_values = std::vector<std::optional<categorization::Value>>{};
        auto versioned_read_keys = std::vector<std::string>{};
        versioned_read_keys.reserve(config_.num_versioned_keys_to_read);
        auto versioned_values = std::vector<std::optional<categorization::Value>>{};
        while (!stop_) {
          merkle_read_keys.clear();
          merkle_values.clear();
          versioned_read_keys.clear();
          versioned_values.clear();
          auto merkle_start = randomMerkleKeyIter();
          auto versioned_start = randomVersionedKeyIter();

          std::copy(
              merkle_start, merkle_start + config_.num_block_merkle_keys_to_read, std::back_inserter(merkle_read_keys));
          kvbc_.multiGetLatest(kCategoryMerkle, merkle_read_keys, merkle_values);
          std::copy(versioned_start,
                    versioned_start + config_.num_versioned_keys_to_read,
                    std::back_inserter(versioned_read_keys));
          kvbc_.multiGetLatest(kCategoryVersioned, versioned_read_keys, versioned_values);
          std::this_thread::sleep_for(config_.delay);
        }
      });
    }
  }

  void stop() {
    std::cout << "Stopping Simulated Pre-Execution Reads" << std::endl;
    stop_ = true;
    for (auto& thread : threads_) {
      thread.join();
    }
  }

 private:
  std::vector<std::string>::const_iterator randomMerkleKeyIter() {
    auto max_read_offset = merkle_read_keys_.size() - config_.num_block_merkle_keys_to_read;
    return merkle_read_keys_.begin() + (rand() % max_read_offset);
  }

  std::vector<std::string>::const_iterator randomVersionedKeyIter() {
    auto max_read_offset = versioned_read_keys_.size() - config_.num_versioned_keys_to_read;
    return versioned_read_keys_.begin() + (rand() % max_read_offset);
  }

  std::atomic_bool stop_ = false;

  std::vector<std::thread> threads_;

  const PreExecConfig config_;
  const ReadKeys& merkle_read_keys_;
  const ReadKeys& versioned_read_keys_;

  categorization::KeyValueBlockchain& kvbc_;
};

}  // namespace concord::kvbc::bench
