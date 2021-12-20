// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <mutex>

#include "gtest/gtest.h"

#include "SimpleBCStateTransfer.hpp"
#include "categorization/kv_blockchain.h"

using namespace concord::kvbc;

namespace bftEngine {

namespace bcst {

#pragma pack(push, 1)
class Block {
 public:
  static uint32_t calcMaxDataSize() { return kMaxBlockSize_ - (sizeof(Block) - 1); }
  static uint32_t getMaxTotalBlockSize() { return kMaxBlockSize_; }
  static void setMaxTotalBlockSize(uint32_t size) { kMaxBlockSize_ = size; }

  static std::shared_ptr<Block> createFromData(uint32_t dataSize,
                                               const char* data,
                                               uint64_t blockId,
                                               StateTransferDigest& digestPrev) {
    auto totalBlockSize = calcTotalBlockSize(dataSize);
    ConcordAssertLE(totalBlockSize, kMaxBlockSize_);
    Block* blk = reinterpret_cast<Block*>(std::malloc(totalBlockSize));
    return std::shared_ptr<Block>(blk->initBlock(data, dataSize, totalBlockSize, blockId, digestPrev));
  }

  static std::shared_ptr<Block> createFromBlock(const char* blk, uint32_t blkSize) {
    ConcordAssertLE(blkSize, kMaxBlockSize_);
    char* buff = static_cast<char*>(std::malloc(blkSize));
    memcpy(buff, blk, blkSize);
    return std::shared_ptr<Block>(reinterpret_cast<Block*>(buff));
  }

  static void free(Block* i) { std::free(static_cast<void*>(i)); }

  StateTransferDigest digestPrev;  // For block ID N, this is the digest calculated on block ID N-1
  uint32_t actualDataSize;
  uint32_t totalBlockSize;
  uint64_t blockId;
  char data[1];

 private:
  static uint32_t calcTotalBlockSize(uint32_t dataSize) { return sizeof(Block) + dataSize - 1; }
  static uint32_t calcDataSize(uint32_t totalSize) { return totalSize - sizeof(Block) + 1; }
  static uint32_t kMaxBlockSize_;

  Block* initBlock(
      const char* data, uint32_t dataSize, uint32_t totalBlockSize, uint64_t blockId, StateTransferDigest& digestPrev) {
    this->actualDataSize = dataSize;
    this->totalBlockSize = totalBlockSize;
    this->blockId = blockId;
    memcpy(this->data, data, dataSize);
    this->digestPrev = digestPrev;
    return this;
  }
};

uint32_t Block::kMaxBlockSize_ = 0;
#pragma pack(pop)

class TestAppState : public IAppState {
  static constexpr uint32_t blockIoMinLatencyMs = 1;
  static constexpr uint32_t blockIoMaxLatencyMs = 4;

 public:
  TestAppState() : last_block_(getGenesisBlockNum()){};

  bool hasBlock(uint64_t blockId) const override {
    std::lock_guard<std::mutex> lg(mtx);
    auto it = blocks_.find(blockId);
    return it != blocks_.end();
  }

  bool getBlock(uint64_t blockId, char* outBlock, uint32_t outBlockMaxSize, uint32_t* outBlockActualSize) override {
    std::lock_guard<std::mutex> lg(mtx);
    auto it = blocks_.find(blockId);
    if (it == blocks_.end()) {
      return false;
    }
    ConcordAssert(outBlockMaxSize >= it->second->totalBlockSize);
    std::memcpy(outBlock, it->second.get(), it->second->totalBlockSize);
    *outBlockActualSize = it->second->totalBlockSize;
    return true;
  };

  const std::shared_ptr<Block> peekBlock(uint64_t blockId) const {
    std::lock_guard<std::mutex> lg(mtx);
    auto it = blocks_.find(blockId);
    if (it == blocks_.end()) {
      return nullptr;
    }
    return it->second;
  };

  std::future<bool> getBlockAsync(uint64_t blockId,
                                  char* outBlock,
                                  uint32_t outBlockMaxSize,
                                  uint32_t* outBlockActualSize) override {
    bool res = getBlock(blockId, outBlock, outBlockMaxSize, outBlockActualSize);
    std::future<bool> future = std::async(std::launch::async, [&, res]() {
      // simulate processing time
      sleepForRandomtime(blockIoMinLatencyMs, blockIoMaxLatencyMs);
      return res;
    });
    return future;
  }

  bool getPrevDigestFromBlock(uint64_t blockId, StateTransferDigest* outPrevBlockDigest) override {
    std::lock_guard<std::mutex> lg(mtx);
    ConcordAssert(blockId > 0);
    auto it = blocks_.find(blockId);
    if (it == blocks_.end()) return false;
    std::memcpy(outPrevBlockDigest, &it->second->digestPrev, sizeof(it->second->digestPrev));
    return true;
  };

  void getPrevDigestFromBlock(const char* blockData,
                              const uint32_t blockSize,
                              StateTransferDigest* outPrevBlockDigest) override {
    const Block* blk = reinterpret_cast<const Block*>(blockData);
    ConcordAssertEQ(blockSize, blk->totalBlockSize);
    std::memcpy(outPrevBlockDigest, &blk->digestPrev, sizeof(blk->digestPrev));
  }

  bool putBlock(const uint64_t blockId, const char* block, const uint32_t blockSize, bool lastBlock) override {
    std::lock_guard<std::mutex> lg(mtx);
    ConcordAssertLE(blockSize, Block::getMaxTotalBlockSize());
    const auto blk = Block::createFromBlock(block, blockSize);
    if (blockId > last_block_) last_block_ = blockId;
    blocks_.emplace(blockId, std::move(blk));
    return true;
  }

  std::future<bool> putBlockAsync(uint64_t blockId,
                                  const char* block,
                                  const uint32_t blockSize,
                                  bool lastBlock) override {
    // TODO(GL) - At this stage we put the blocks in the main thread context. Doing so in child thread context
    // complicates the main test logic, since we have to trigger ST for multiple checks.
    // Try to do this in the future to simulate un-ordered completions.
    ConcordAssertLE(blockSize, Block::getMaxTotalBlockSize());
    putBlock(blockId, block, blockSize, lastBlock);
    std::future<bool> future = std::async(std::launch::async, []() {
      // simulate processing time
      sleepForRandomtime(blockIoMinLatencyMs, blockIoMaxLatencyMs);
      return true;
    });
    return future;
  }

  uint64_t getLastReachableBlockNum() const override { return last_block_; }
  uint64_t getGenesisBlockNum() const override { return concord::kvbc::INITIAL_GENESIS_BLOCK_ID; }
  uint64_t getLastBlockNum() const override { return last_block_; };

  void postProcessUntilBlockId(uint64_t maxBlockId) override {
    sleepForRandomtime(1, 2);
    return;
  }

 private:
  uint64_t last_block_;
  std::unordered_map<uint64_t, std::shared_ptr<Block>> blocks_;
  mutable std::mutex mtx;

  static void sleepForRandomtime(uint32_t fromMs, uint32_t toMs) {
    ASSERT_LT(fromMs, toMs);
    std::mt19937_64 eng{std::random_device{}()};
    std::uniform_int_distribution<uint32_t> dist{fromMs, toMs};
    std::this_thread::sleep_for(std::chrono::milliseconds{dist(eng)});
  }
};

}  // namespace bcst

}  // namespace bftEngine
