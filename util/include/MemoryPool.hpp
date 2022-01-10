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

#include "Logger.hpp"

#include <mutex>
#include <condition_variable>
#include <atomic>
#include <boost/lockfree/queue.hpp>

namespace concordUtils {

class MemoryPool {
 public:
  MemoryPool(uint32_t chunkSize) : chunkSize_(chunkSize) {}
  virtual ~MemoryPool();

  void allocatePool(int32_t minChunks, int32_t maxChunks);
  char* getChunk();
  void returnChunk(char*);

  MemoryPool(const MemoryPool&) = delete;
  MemoryPool& operator=(const MemoryPool&) = delete;
  MemoryPool(MemoryPool&&) = delete;
  MemoryPool&& operator=(MemoryPool&&) = delete;

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
  std::atomic_bool stopWorking_{false};
  std::condition_variable waitForChunkCond_;
  std::mutex waitForChunkLock_;
};

}  // namespace concordUtils
