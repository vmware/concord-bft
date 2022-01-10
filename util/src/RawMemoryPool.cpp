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

#include "RawMemoryPool.hpp"

#include <thread>
#include "assertUtils.hpp"
#include "kvstream.h"

namespace concordUtil {

using namespace std;
using namespace std::chrono;

RawMemoryPool::RawMemoryPool(uint32_t chunkSize, Timers& timers) : chunkSize_(chunkSize), metrics_(timers) {}

RawMemoryPool::~RawMemoryPool() {
  stopWorking_ = true;
  unique_lock<mutex> lock(waitForAvailChunkLock_);
  this_thread::sleep_for(std::chrono::milliseconds(WAIT_TIMEOUT_MILLI * 2));
  char* chunk = nullptr;
  while (pool_->pop(chunk)) delete[] chunk;
}

void RawMemoryPool::allocatePool(int32_t minChunks, int32_t maxChunks) {
  if (minChunks > maxChunks || minChunks < 0 || maxChunks < 0) {
    stringstream err;
    err << "Wrong parameters specified: minChunks=" << minChunks << ", maxChunks=" << maxChunks;
    LOG_ERROR(logger(), KVLOG(err.str()));
    throw std::invalid_argument(__PRETTY_FUNCTION__ + err.str());
  }
  minChunksNum_ = minChunks;
  maxChunksNum_ = maxChunks;
  pool_ = make_shared<boost::lockfree::queue<char*, boost::lockfree::fixed_sized<true>>>(maxChunksNum_);
  for (int32_t i = 0; i < minChunksNum_; ++i) {
    pool_->push(new char[chunkSize_]);
    metrics_.increaseAvailableChunksNum();
    metrics_.increaseAllocatedChunksNum();
  }
  numOfAvailableChunks_ = minChunksNum_;
  numOfAllocatedChunks_ = minChunksNum_;
  LOG_INFO(logger(),
           "Memory pool allocated" << KVLOG(
               chunkSize_, minChunksNum_, maxChunksNum_, numOfAllocatedChunks_, numOfAvailableChunks_));
}

char* RawMemoryPool::getChunk() {
  ConcordAssert(pool_.get() != nullptr);
  char* chunk = nullptr;
  if (!pool_->pop(chunk)) {
    // No available chunks => allocate a new one, if permitted
    if (numOfAvailableChunks_ < maxChunksNum_) {
      chunk = new char[chunkSize_];
      numOfAllocatedChunks_++;
      metrics_.increaseAllocatedChunksNum();
      numOfAvailableChunks_++;
      metrics_.increaseAvailableChunksNum();
      LOG_INFO(logger(), "A new chunk has been allocated" << KVLOG(numOfAllocatedChunks_, numOfAvailableChunks_));
    } else {
      // Wait until some chunk gets released
      unique_lock<mutex> lock(waitForAvailChunkLock_);
      while (!stopWorking_ && !numOfAvailableChunks_)
        waitForAvailChunkCond_.wait_until(lock, steady_clock::now() + milliseconds(WAIT_TIMEOUT_MILLI));
    }
  }
  if (chunk) numOfAvailableChunks_--;
  metrics_.decreaseAvailableChunksNum();
  return chunk;
}

void RawMemoryPool::returnChunk(char* chunk) {
  if (numOfAvailableChunks_ >= maxChunksNum_) {
    stringstream err;
    err << "The chunk overflows the pool size: numOfAvailableChunks_=" << numOfAvailableChunks_
        << ", maxChunksNum_=" << maxChunksNum_;
    LOG_ERROR(logger(), KVLOG(err.str()));
    throw std::runtime_error(__PRETTY_FUNCTION__ + err.str());
  }
  pool_->push(chunk);
  numOfAvailableChunks_++;
  metrics_.increaseAvailableChunksNum();
  waitForAvailChunkCond_.notify_one();
}

}  // namespace concordUtil
