// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "Timers.hpp"
#include "Logger.hpp"
#include "Metrics.hpp"

#include <mutex>
#include <condition_variable>
#include <atomic>
#include <boost/lockfree/queue.hpp>

// This thread-safe memory pool implementation is intended for the raw memory chunks operations. During the pool
// initialization, a pre-defined number (minChunksNum_) of raw memory chunks of size chunkSize_ gets allocated.
// In case there is no available chunk, a new one gets dynamically allocated - up to a maximum number (maxChunksNum_).
// No support for different memory sizes provided.

namespace concordUtil {

class RawMemoryPoolMetrics {
 public:
  RawMemoryPoolMetrics(Timers& timers)
      : component_{"memoryPoolMetrics", std::make_shared<concordMetrics::Aggregator>()},
        availableChunks_{component_.RegisterGauge("availableChunks", 0)},
        allocatedChunks_{component_.RegisterGauge("allocatedChunks", 0)},
        timers_(timers) {
    component_.Register();
    const std::chrono::milliseconds period{100};
    metricsTimer_ = timers_.add(period, Timers::Timer::RECURRING, [this](Timers::Handle h) { updateAggregator(); });
  }

  ~RawMemoryPoolMetrics() { timers_.cancel(metricsTimer_); }

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
    component_.SetAggregator(aggregator);
  }

  void increaseAvailableChunksNum() { availableChunks_++; }
  void decreaseAvailableChunksNum() { availableChunks_--; }
  void increaseAllocatedChunksNum() { allocatedChunks_++; }
  void decreaseAllocatedChunksNum() { allocatedChunks_--; }

  void updateAggregator() { component_.UpdateAggregator(); }

 private:
  concordMetrics::Component component_;
  concordMetrics::GaugeHandle availableChunks_;
  concordMetrics::GaugeHandle allocatedChunks_;
  Timers& timers_;
  Timers::Handle metricsTimer_;
};

class RawMemoryPool {
 public:
  RawMemoryPool(uint32_t chunkSize, Timers& timers);
  virtual ~RawMemoryPool();

  void allocatePool(int32_t minChunks, int32_t maxChunks);
  int32_t getNumOfAvailableChunks() const { return numOfAvailableChunks_; }
  int32_t getNumOfAllocatedChunks() const { return numOfAllocatedChunks_; }
  bool isPoolFull() const { return numOfAllocatedChunks_ == maxChunksNum_; }
  char* getChunk();
  void returnChunk(char*);
  void stopPool() { stopWorking_ = true; }

  RawMemoryPool(const RawMemoryPool&) = delete;
  RawMemoryPool& operator=(const RawMemoryPool&) = delete;
  RawMemoryPool(RawMemoryPool&&) = delete;
  RawMemoryPool&& operator=(RawMemoryPool&&) = delete;

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
    metrics_.setAggregator(aggregator);
  }

  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.memory.pool");
    return logger_;
  }

 private:
  const uint32_t WAIT_TIMEOUT_MILLI = 5;
  const uint32_t chunkSize_;
  int32_t minChunksNum_ = 0;
  int32_t maxChunksNum_ = 0;
  std::shared_ptr<boost::lockfree::queue<char*, boost::lockfree::fixed_sized<true>>> pool_;
  std::atomic_int numOfAvailableChunks_{0};
  std::atomic_int numOfAllocatedChunks_{0};
  std::atomic_bool stopWorking_{false};
  std::condition_variable waitForAvailChunkCond_;
  std::mutex waitForAvailChunkLock_;
  RawMemoryPoolMetrics metrics_;
};

}  // namespace concordUtil
