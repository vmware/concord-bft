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
#include "input.h"

namespace concord::kvbc::bench {

struct PreExecConfig {
  size_t concurrency;
  std::chrono::milliseconds delay;
  size_t num_block_merkle_keys_to_read;
};

class PreExecutionSimulator {
 public:
  PreExecutionSimulator(const PreExecConfig& config,
                        const ReadKeys& read_keys,
                        categorization::detail::BlockMerkleCategory& cat)
      : config_(config), read_keys_(read_keys), cat_(cat) {}

  void start() {
    std::cout << "Starting Simulated Pre-Execution Reads" << std::endl;
    for (auto i = 0u; i < config_.concurrency; i++) {
      threads_.emplace_back([this]() {
        // Only alloacate the read_keys vector once
        auto read_keys = std::vector<std::string>{};
        read_keys.reserve(config_.num_block_merkle_keys_to_read);
        auto values = std::vector<std::optional<categorization::Value>>{};
        while (!stop_) {
          read_keys.clear();
          values.clear();
          auto start = randomKeyIter();
          std::copy(start, start + config_.num_block_merkle_keys_to_read, std::back_inserter(read_keys));
          cat_.multiGetLatest(read_keys, values);
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
  std::vector<std::string>::const_iterator randomKeyIter() {
    auto max_read_offset = read_keys_.size() - config_.num_block_merkle_keys_to_read;
    return read_keys_.begin() + (rand() % max_read_offset);
  }
  std::atomic_bool stop_ = false;

  std::vector<std::thread> threads_;

  const PreExecConfig config_;
  const ReadKeys& read_keys_;
  // TODO: Replace this with kv_blockchain for more general reads
  categorization::detail::BlockMerkleCategory& cat_;
};

}  // namespace concord::kvbc::bench
