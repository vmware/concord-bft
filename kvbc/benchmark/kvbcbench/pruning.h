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

#include <condition_variable>
#include <chrono>
#include <mutex>
#include <thread>

#include "categorization/kv_blockchain.h"

namespace concord::kvbc::bench {

class Pruning {
 public:
  Pruning(size_t prune_blocks, categorization::KeyValueBlockchain& kvbc) : prune_blocks_{prune_blocks}, kvbc_{kvbc} {
    if (prune_blocks) {
      thread_ = std::thread{[&]() {
        auto l = std::unique_lock{start_mtx_};
        if (!start_) {
          start_signal_.wait(l, [&]() { return start_; });
        }
        l.unlock();
        kvbc_.deleteBlocksUntil(prune_blocks_ + 1);
      }};
    }
  }

  void start() {
    auto l = std::unique_lock{start_mtx_};
    start_ = true;
    l.unlock();
    start_signal_.notify_one();
    start_time_ = std::chrono::steady_clock::now();
  }

  void stop() {
    if (prune_blocks_) {
      thread_.join();
      duration_ =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time_).count();
    }
  }

  auto duration() const { return duration_; }

  auto blocks() const { return prune_blocks_; }

 private:
  const size_t prune_blocks_{0};
  categorization::KeyValueBlockchain& kvbc_;

  std::chrono::time_point<std::chrono::steady_clock> start_time_;
  int64_t duration_{1};

  std::condition_variable start_signal_;
  std::mutex start_mtx_;
  bool start_{false};
  std::thread thread_;
};

}  // namespace concord::kvbc::bench
