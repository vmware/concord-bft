// Copyright 2021 VMware, all rights reserved
//
// This convenience header combines different DB adapter implementations.

#pragma once

#include <cstdint>
#include <mutex>
namespace concord::kvbc::pruning {
class ConcurrentPruningManager {
 public:
  struct TicksConfiguration {
    uint32_t tick_period_seconds;
    uint64_t batch_blocks_num;
  };
  static ConcurrentPruningManager& instance() {
    static ConcurrentPruningManager manager_;
    return manager_;
  }

  void setTicksConfiguration(const TicksConfiguration& conf) {
    std::unique_lock<std::mutex> l(lock_);
    latestTicksConfiguration_ = conf;
  }
  const TicksConfiguration& getLatestTicksConfiguration() {
    std::unique_lock<std::mutex> l(lock_);
    return latestTicksConfiguration_;
  }

 private:
  TicksConfiguration latestTicksConfiguration_;
  std::mutex lock_;
};
}  // namespace concord::kvbc::pruning