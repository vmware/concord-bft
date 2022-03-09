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
  if (pool_)
    while (pool_->pop(chunk)) delete[] chunk;
}

void RawMemoryPool::allocatePool(int32_t initialChunksNum, int32_t maxChunksNum) {
  if (initialChunksNum > maxChunksNum || initialChunksNum < 0 || maxChunksNum < 0) {
    stringstream err;
    err << "Wrong parameters specified: initialChunksNum=" << initialChunksNum << ", maxChunksNum=" << maxChunksNum;
    LOG_ERROR(logger(), KVLOG(err.str()));
    throw std::invalid_argument(__PRETTY_FUNCTION__ + err.str());
  }
  initialChunksNum_ = initialChunksNum;
  maxChunksNum_ = maxChunksNum;
  pool_ = make_shared<boost::lockfree::queue<char*, boost::lockfree::fixed_sized<true>>>(maxChunksNum_);
  for (int32_t i = 0; i < initialChunksNum_; ++i) {
    char* chunk = new char[chunkSize_];
    pool_->push(chunk);
    LOG_DEBUG(logger(), "Initial chunk allocation" << KVLOG((void*)chunk));
    metrics_.increaseAvailableChunksNum();
    metrics_.increaseAllocatedChunksNum();
  }
  numOfAvailableChunks_ = initialChunksNum;
  numOfAllocatedChunks_ = initialChunksNum;
  LOG_INFO(logger(),
           "Memory pool allocated" << KVLOG(chunkSize_,
                                            initialChunksNum_,
                                            maxChunksNum_,
                                            numOfAllocatedChunks_,
                                            numOfAvailableChunks_,
                                            maxAvailableChunksPercentage_));
}

void RawMemoryPool::increaseNumOfAllocatedChunks() {
  numOfAllocatedChunks_++;
  metrics_.increaseAllocatedChunksNum();
}

void RawMemoryPool::increaseNumOfAvailableChunks() {
  numOfAvailableChunks_++;
  metrics_.increaseAvailableChunksNum();
}

void RawMemoryPool::decreaseNumOfAllocatedChunks() {
  numOfAllocatedChunks_--;
  metrics_.decreaseAllocatedChunksNum();
}

void RawMemoryPool::decreaseNumOfAvailableChunks() {
  numOfAvailableChunks_--;
  metrics_.decreaseAvailableChunksNum();
}

char* RawMemoryPool::allocateChunk() {
  char* chunk = nullptr;
  if (numOfAllocatedChunks_ < maxChunksNum_) {
    chunk = new char[chunkSize_];
    increaseNumOfAllocatedChunks();
  }
  if (chunk)
    LOG_DEBUG(logger(),
              "A chunk has been allocated" << KVLOG(numOfAllocatedChunks_, numOfAvailableChunks_, (void*)chunk));
  else
    LOG_WARN(logger(),
             "The pool size has reached the maximum, wait for a chunk to become available" << KVLOG(maxChunksNum_));
  return chunk;
}

void RawMemoryPool::deleteChunk(char*& chunk) {
  LOG_DEBUG(logger(), "Going to delete a chunk" << KVLOG(numOfAllocatedChunks_, numOfAvailableChunks_, (void*)chunk));
  delete[] chunk;
  chunk = nullptr;
  decreaseNumOfAllocatedChunks();
}

void RawMemoryPool::returnChunkToThePool(char* chunk) {
  pool_->push(chunk);
  increaseNumOfAvailableChunks();
  LOG_DEBUG(
      logger(),
      "A chunk has been returned to the pool" << KVLOG(numOfAllocatedChunks_, numOfAvailableChunks_, (void*)chunk));
  waitForAvailChunkCond_.notify_one();
}

bool RawMemoryPool::isPoolPruningRequired() {
  return (((numOfAllocatedChunks_ > initialChunksNum_) &&
           (numOfAvailableChunks_ >= numOfAllocatedChunks_ * maxAvailableChunksPercentage_)));
}

char* RawMemoryPool::getChunk() {
  ConcordAssert(pool_.get() != nullptr);
  char* chunk = nullptr;
  bool chunkAllocatedFromPool = true;
  bool isChunkAvailable = pool_->pop(chunk);
  if (!isChunkAvailable) {
    // No available chunks => allocate a new one from the heap, if permitted
    chunk = allocateChunk();
    if (chunk)
      chunkAllocatedFromPool = false;
    else {
      // Pool size limit has been reached; wait until some chunk gets released
      unique_lock<mutex> lock(waitForAvailChunkLock_);
      while (!stopWorking_ && !isChunkAvailable) {
        waitForAvailChunkCond_.wait_until(lock, steady_clock::now() + milliseconds(WAIT_TIMEOUT_MILLI));
        isChunkAvailable = pool_->pop(chunk);
      }
    }
  }
  if (chunk) {
    if (chunkAllocatedFromPool) decreaseNumOfAvailableChunks();
    LOG_DEBUG(logger(),
              "A chunk has been consumed"
                  << KVLOG(numOfAllocatedChunks_, numOfAvailableChunks_, chunkAllocatedFromPool, (void*)chunk));
  }
  return chunk;
}

void RawMemoryPool::returnChunk(char* chunk) {
  if (numOfAvailableChunks_ >= maxChunksNum_) {
    stringstream err;
    err << "Returned chunk overflows the pool size limit: numOfAvailableChunks_=" << numOfAvailableChunks_
        << ", maxChunksNum_=" << maxChunksNum_ << ", chunk=" << (void*)chunk;
    LOG_ERROR(logger(), KVLOG(err.str()));
    throw std::runtime_error(__PRETTY_FUNCTION__ + err.str());
  }
  if (!isPoolPruningRequired())
    returnChunkToThePool(chunk);
  else  // Too many chunks are not in use => delete
    deleteChunk(chunk);
}

}  // namespace concordUtil
