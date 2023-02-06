// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <cstring>
#include <memory>
#include <vector>
#include <future>
#include <thread>
#include <chrono>

#include "gtest/gtest.h"

#include "MultiSizeBufferPool.hpp"
namespace concordUtil::test {

using namespace std;
using namespace concordUtil;
using namespace std::chrono_literals;

#define ASSERT_NFF ASSERT_NO_FATAL_FAILURE

// valid subpool configurations
static const MultiSizeBufferPool::SubpoolConfig subpool_config0{300, 4, 6};
static const MultiSizeBufferPool::SubpoolConfig subpool_config1{800, 3, 5};
static const MultiSizeBufferPool::SubpoolConfig subpool_config2{1034, 2, 4};
static const MultiSizeBufferPool::SubpoolConfig subpool_config3{2048, 1, 5};

// invalid subpool configurations
static const MultiSizeBufferPool::SubpoolConfig subpool_config100{0, 4, 6};    // bufferSize == 0
static const MultiSizeBufferPool::SubpoolConfig subpool_config101{300, 6, 4};  // numInitialBuffers > numMaxBuffers
static const MultiSizeBufferPool::SubpoolConfig subpool_config102{
    300, 0, 0};  // numInitialBuffers == numMaxBuffers == 0

// pool config
static const concordUtil::MultiSizeBufferPool::Config pool_config0{// maxAllocatedBytes
                                                                   10ULL * (1 << 30),
                                                                   // purgeEvaluationFrequency
                                                                   0,
                                                                   // statusReportFrequency
                                                                   60,
                                                                   // histogramsReportFrequency
                                                                   300,
                                                                   // metricsReportFrequency
                                                                   5,
                                                                   // enableStatusReportChangesOnly
                                                                   false,
                                                                   // subpoolSelectorEvaluationNumRetriesMinThreshold
                                                                   3,
                                                                   // subpoolSelectorEvaluationNumRetriesMaxThreshold
                                                                   10};

class MultiSizeBufferPoolTestFixture : public ::testing::Test {
 public:
 protected:
  void SetUp() override{};
  void TearDown() override {
    if (release_all_buffers_on_teardown && pool_) pushAllBuffers();
  };
  void init(const MultiSizeBufferPool::SubpoolsConfig& subpools_config,
            MultiSizeBufferPool::Config pool_config,
            std::unique_ptr<MultiSizeBufferPool::ISubpoolSelector>&& subpool_selector = nullptr,
            std::unique_ptr<MultiSizeBufferPool::IPurger>&& purger = nullptr) {
    logging::Logger::getInstance("concord.memory.pool").setLogLevel(log4cplus::ERROR_LOG_LEVEL);
    subpools_config_ = std::make_unique<MultiSizeBufferPool::SubpoolsConfig>(subpools_config);
    pool_config_ = std::make_unique<MultiSizeBufferPool::Config>(pool_config);

    std::string pool_name = std::string(::testing::UnitTest::GetInstance()->current_test_info()->name()) + "_pool";
    pool_ = std::make_unique<MultiSizeBufferPool>(
        pool_name, timers_, *subpools_config_, *pool_config_, std::move(subpool_selector), std::move(purger));
    resetAllocatedBuffers();
  }

  void doPeriodic() { pool_->doPeriodic(); }
  // requested_buffer_size == 0 then getBufferBySubpoolSelector, else call getBufferByMinBufferSize
  void popBufferAndValidate(uint32_t requested_buffer_size = 0,
                            uint32_t expected_buffer_size = 0,
                            bool expectSuccess = true) {
    // we rely on the fact that pool will throw if bufferSize is invalid
    auto [buffer, actual_buffer_size] = (requested_buffer_size == 0)
                                            ? pool_->getBufferBySubpoolSelector()
                                            : pool_->getBufferByMinBufferSize(requested_buffer_size);
    if (expectSuccess) {
      ASSERT_TRUE(buffer);
      ASSERT_TRUE(MultiSizeBufferPool::validateBuffer(buffer).first);
      ASSERT_EQ(actual_buffer_size, MultiSizeBufferPool::getBufferSize(buffer));
      if (expected_buffer_size > 0) {
        ASSERT_EQ(actual_buffer_size, expected_buffer_size);
      }
      {
        std::lock_guard g(allocated_buffers_mutex_);
        auto iter = allocated_buffers_.find(actual_buffer_size);
        ASSERT_TRUE(iter != allocated_buffers_.end());
        auto set_iter = iter->second;
        ASSERT_TRUE(set_iter.find(buffer) == set_iter.end());
        auto result = allocated_buffers_[actual_buffer_size].insert(buffer);
        ASSERT_TRUE(result.second);
      }
    } else {
      ASSERT_TRUE(!buffer);
    }
  }

  void pushAllBuffers() {
    std::lock_guard g(allocated_buffers_mutex_);
    for (auto const& [_, sub_buffers] : allocated_buffers_) {
      UNUSED(_);
      for (const auto& buffer : sub_buffers) {
        pool_->returnBuffer(buffer);
      }
    }
    allocated_buffers_.clear();
  }

  // if bufferSizeHint != 0, delete from this subpool.
  void pushArbitraryBuffer(uint32_t bufferSizeHint = 0) {
    std::lock_guard g(allocated_buffers_mutex_);
    char* buffer_to_erase{};
    ASSERT_TRUE(!allocated_buffers_.empty());
    auto erase_first_buffer_in_buffers = [this, &buffer_to_erase](Buffers& buffers) {
      buffer_to_erase = *buffers.begin();
      pool_->returnBuffer(buffer_to_erase);
      buffers.erase(buffer_to_erase);
    };
    if (bufferSizeHint != 0) {
      auto iter = allocated_buffers_.find(bufferSizeHint);
      ASSERT_TRUE(iter != allocated_buffers_.end());
      ASSERT_FALSE(iter->second.empty());
      erase_first_buffer_in_buffers(iter->second);
    } else {
      // bufferSizeHint==0: Just choose 1st non-empty pool
      for (auto& [_, sub_buffers] : allocated_buffers_) {
        UNUSED(_);
        if (!sub_buffers.empty()) {
          erase_first_buffer_in_buffers(sub_buffers);
          break;
        }
      }
    }
    ASSERT_TRUE(buffer_to_erase);
  }

  void pushSpecificBuffer(char* buffer_to_erase) {
    std::lock_guard g(allocated_buffers_mutex_);
    ASSERT_TRUE(buffer_to_erase);
    auto bufferSize = MultiSizeBufferPool::getBufferSize(buffer_to_erase);
    pool_->returnBuffer(buffer_to_erase);
    auto iter = allocated_buffers_.find(bufferSize);
    ASSERT_TRUE(iter != allocated_buffers_.end());
    ASSERT_FALSE(iter->second.empty());
    ASSERT_EQ(iter->second.erase(buffer_to_erase), 1);
  }

  void stopPool() {
    for (auto& spIter : pool_->subpools_) {
      auto& sp = spIter.second;
      sp->stopFlag_ = true;
      sp->waitForAvailChunkCond_.notify_all();
    }
  }

  MultiSizeBufferPool::ChunkHeader* getBufferHeader(char* buffer) {
    char* chunk = MultiSizeBufferPool::bufferToChunk(buffer);
    return reinterpret_cast<MultiSizeBufferPool::ChunkHeader*>(chunk);
  }

  MultiSizeBufferPool::ChunkTrailer* getBufferTrailer(char* buffer) {
    char* chunk = MultiSizeBufferPool::bufferToChunk(buffer);
    auto chunkHeader = reinterpret_cast<MultiSizeBufferPool::ChunkHeader*>(chunk);
    return MultiSizeBufferPool::getChunkTrailer(chunk,
                                                chunkHeader->bufferSize_ + MultiSizeBufferPool::chunkMetadataSize());
  }

  void resetAllocatedBuffers() {
    std::lock_guard g(allocated_buffers_mutex_);
    allocated_buffers_.clear();
    for (const auto& subpool_config : *subpools_config_) {
      allocated_buffers_.emplace(subpool_config.bufferSize, std::set<char*>());
    }
  }

  static uint32_t getChunkMetadataSize() { return MultiSizeBufferPool::chunkMetadataSize(); }
  const MultiSizeBufferPool::SubPool& getSubPool(uint32_t bufferSize) const { return pool_->getSubPool(bufferSize); }
  const MultiSizeBufferPool::Statistics& getPoolStats() const { return pool_->stats_; }
  const MultiSizeBufferPool::Statistics& getSubPoolStats(const MultiSizeBufferPool::SubPool& sp) const {
    return sp.stats_;
  }

  // True when all chunks are allocated from heap up to maximum allowed in subpool config, and none are available
  bool isPoolEmpty() const {
    for (const auto& [_, subpool] : pool_->subpools_) {
      UNUSED(_);
      if (!isSubPoolEmpty(*subpool)) return false;
    }
    return true;
  }
  bool isSubPoolEmpty(uint32_t bufferSize) const {
    const auto& sp = pool_->getSubPool(bufferSize);
    return isSubPoolEmpty(sp);
  }
  bool isSubPoolEmpty(const MultiSizeBufferPool::SubPool& sp) const {
    return (sp.stats_.current_.numUnusedChunks_ == 0);
  }

  std::unique_ptr<MultiSizeBufferPool::SubpoolsConfig> subpools_config_;
  std::unique_ptr<MultiSizeBufferPool::Config> pool_config_;
  std::unique_ptr<MultiSizeBufferPool> pool_;
  Timers timers_;
  using Buffers = std::set<char*>;
  std::map<uint32_t, Buffers> allocated_buffers_;
  std::mutex allocated_buffers_mutex_;
  bool release_all_buffers_on_teardown{true};
  MultiSizeBufferPool::Statistics stats_{true};
};

TEST_F(MultiSizeBufferPoolTestFixture, Test_Invalid_Input) {
  // Throw on empty subpools config
  ASSERT_THROW(init({}, pool_config0), std::invalid_argument);

  // Throw on non ascending subpools config buffer size
  ASSERT_THROW(init({{subpool_config0, subpool_config1, subpool_config1}}, pool_config0), std::invalid_argument);
  ASSERT_THROW(init({{subpool_config0, subpool_config2, subpool_config1}}, pool_config0), std::invalid_argument);

  // Throw when bufferSize == 0
  ASSERT_THROW(init({{subpool_config100}}, pool_config0), std::invalid_argument);

  // Throw when numInitialBuffers > numMaxBuffers
  ASSERT_THROW(init({{subpool_config101}}, pool_config0), std::invalid_argument);

  // Throw when numInitialBuffers == numMaxBuffers == 0
  ASSERT_THROW(init({{subpool_config102}}, pool_config0), std::invalid_argument);
}

// Test enforcement of maxAllocatedBytes
TEST_F(MultiSizeBufferPoolTestFixture, Test_maxAllocatedBytes) {
  // This is dirty, but done in a test
  auto modifiedConfig = pool_config0;
  auto chunkMetadataSize = getChunkMetadataSize();
  decltype(modifiedConfig.maxAllocatedBytes) maxAllocatedBytes =
      (subpool_config0.bufferSize + chunkMetadataSize) + (subpool_config1.bufferSize + chunkMetadataSize);
  memcpy((void*)&modifiedConfig.maxAllocatedBytes, (void*)&maxAllocatedBytes, sizeof(maxAllocatedBytes));
  // Make sure that when creating the pool, if initial subpools allocation exceeds maxAllocatedBytes. pool throws.
  ASSERT_THROW(init({{subpool_config0, subpool_config1}}, modifiedConfig), std::invalid_argument);

  // Now we start with valid initial allocation. Each subpool allows to allocate more chunks than we need, but the
  // maxAllocatedBytes should block us from allocating more

  // allow no more than what is allowed initially
  decltype(modifiedConfig.maxAllocatedBytes) maxAllocatedBytes1 =
      (subpool_config2.bufferSize + chunkMetadataSize) * subpool_config2.numInitialBuffers +
      (subpool_config3.bufferSize + chunkMetadataSize) * subpool_config3.numInitialBuffers;
  memcpy((void*)&modifiedConfig.maxAllocatedBytes, (void*)&maxAllocatedBytes1, sizeof(maxAllocatedBytes1));
  ASSERT_NFF(init({{subpool_config2, subpool_config3}}, modifiedConfig));

  // Get all chunks (use the smaller buffer)
  const auto& stats = getPoolStats();
  ASSERT_EQ(stats.current_.numUsedChunks_, 0);
  for (size_t i{}; i < (subpool_config3.numInitialBuffers + subpool_config2.numInitialBuffers); ++i) {
    ASSERT_NFF(popBufferAndValidate());
  }

  // should block - run 2 workers on both pools
  ASSERT_EQ(stats.current_.numUsedChunks_, 3);
  auto worker_allocator_future3 =
      std::async(std::launch::async, [this]() { ASSERT_NFF(popBufferAndValidate(subpool_config3.bufferSize)); });
  auto worker_allocator_future2 =
      std::async(std::launch::async, [this]() { ASSERT_NFF(popBufferAndValidate(subpool_config2.bufferSize)); });
  std::this_thread::sleep_for(100ms);
  ASSERT_TRUE(worker_allocator_future3.valid());
  ASSERT_TRUE(worker_allocator_future2.valid());
  ASSERT_EQ(stats.current_.numUsedChunks_, 3);

  // push one buffer
  ASSERT_NFF(pushArbitraryBuffer(subpool_config3.bufferSize));
  worker_allocator_future3.wait();
  ASSERT_EQ(stats.current_.numUsedChunks_, 3);

  // push another buffer
  ASSERT_NFF(pushArbitraryBuffer(subpool_config2.bufferSize));
  worker_allocator_future2.wait();
  ASSERT_EQ(stats.current_.numUsedChunks_, 3);
  worker_allocator_future2.get();
  worker_allocator_future3.get();
  ASSERT_FALSE(worker_allocator_future3.valid());
  ASSERT_FALSE(worker_allocator_future2.valid());
}

// Create and destroy pool
TEST_F(MultiSizeBufferPoolTestFixture, Subpool_Create_Destroy) {
  ASSERT_NFF(init({{subpool_config0, subpool_config1, subpool_config2}}, pool_config0));
}

// Test MultiSizeBufferPool::getBufferBySubpoolSelector by allocating a single allocation from each subpool_config
TEST_F(MultiSizeBufferPoolTestFixture, Subpool_getBufferBySubpoolSelector_single_buffer_per_subpool) {
  ASSERT_NFF(init({{subpool_config0, subpool_config1, subpool_config2}}, pool_config0));

  // Allocate
  for (const auto& subpool_config : *subpools_config_) {
    ASSERT_NFF(popBufferAndValidate(subpool_config.bufferSize));
  }
}

// Validate buffer magic protection on header and trailer
TEST_F(MultiSizeBufferPoolTestFixture, validate_magic_protection) {
  // initialize, allocate single buffer
  ASSERT_NFF(init({{subpool_config0, subpool_config1, subpool_config2}}, pool_config0));
  ASSERT_NFF(popBufferAndValidate((*subpools_config_)[0].bufferSize));
  ASSERT_EQ(allocated_buffers_[(*subpools_config_)[0].bufferSize].size(), 1);  // one buffer is allocated

  // Get the header and corrupt it
  auto* header{getBufferHeader(*allocated_buffers_.begin()->second.begin())};
  auto magicField = header->magicField_;
  memset(header, 0, sizeof(header->magicField_));

  // push it back - we expect an exception
  ASSERT_THROW(pushArbitraryBuffer(), std::invalid_argument);

  // copy back so that test can end without more exceptions
  memcpy(header, &magicField, sizeof(magicField));

  // now get the trailer and corrupt it
  auto* trailer{getBufferTrailer(*allocated_buffers_.begin()->second.begin())};
  magicField = trailer->magicField_;
  memset(trailer, 0, sizeof(trailer->magicField_));

  // push it back - we expect an exception
  ASSERT_THROW(pushArbitraryBuffer(), std::invalid_argument);

  // copy back so that test can end without more exceptions
  memcpy(trailer, &magicField, sizeof(trailer->magicField_));
}

// Test MultiSizeBufferPool::getBufferBySubpoolSelector by allocating numInitialBuffers from each subpool_config
TEST_F(MultiSizeBufferPoolTestFixture, Subpool_getBufferBySubpoolSelector_numInitialBuffers_buffers_per_subpool) {
  ASSERT_NFF(init({{subpool_config0, subpool_config1, subpool_config2}}, pool_config0));

  // Allocate
  for (const auto& subpool_config : *subpools_config_) {
    for (size_t i{}; i < subpool_config.numInitialBuffers; ++i) {
      ASSERT_NFF(popBufferAndValidate(subpool_config.bufferSize));
    }
  }
}

// Test MultiSizeBufferPool::getBufferBySubpoolSelector by allocating numMaxBuffers from each subpool_config
TEST_F(MultiSizeBufferPoolTestFixture, Subpool_getBufferBySubpoolSelector_numMaxBuffers_buffers_per_subpool) {
  ASSERT_NFF(init({{subpool_config0, subpool_config1, subpool_config2}}, pool_config0));

  // Allocate
  for (const auto& subpool_config : *subpools_config_) {
    for (size_t i{}; i < subpool_config.numMaxBuffers; ++i) {
      ASSERT_NFF(popBufferAndValidate(subpool_config.bufferSize));
    }
  }
}

// Test MultiSizeBufferPool::getBufferBySubpoolSelector by allocating numMaxBuffers from a single subpool_config, then
// trying to allocate one more to trigger a wait
TEST_F(MultiSizeBufferPoolTestFixture, Subpool_getBufferBySubpoolSelector_above_numMaxBuffers_buffers_single_subpool) {
  ASSERT_NFF(init({{subpool_config0}}, pool_config0));

  // Allocate all buffers from pool
  for (const auto& subpool_config : *subpools_config_) {
    for (size_t i{}; i < subpool_config.numMaxBuffers; ++i) {
      ASSERT_NFF(popBufferAndValidate(subpool_config.bufferSize));
    }
  }

  // Pool has no more resources, allocating one more time triggers a wait. To do it on a unittest we must
  // run in a worker thread
  auto worker_allocator_future =
      std::async(std::launch::async, [this]() { ASSERT_NFF(popBufferAndValidate((*subpools_config_)[0].bufferSize)); });
  ASSERT_TRUE(worker_allocator_future.valid());
  ASSERT_TRUE(isSubPoolEmpty((*subpools_config_)[0].bufferSize));
  ASSERT_TRUE(isPoolEmpty());
  ASSERT_EQ(allocated_buffers_[(*subpools_config_)[0].bufferSize].size(), (*subpools_config_)[0].numMaxBuffers);

  // release one buffer and wait for worker thread to allocate successfully
  ASSERT_NFF(pushArbitraryBuffer());
  worker_allocator_future.wait();
}

// Test MultiSizeBufferPool::getBufferBySubpoolSelector by allocating numMaxBuffers from a first subpool_config, then
// trying to allocate one more to get from the larger subpool
TEST_F(MultiSizeBufferPoolTestFixture, Subpool_getBufferBySubpoolSelector_get_from_next_subpool) {
  ASSERT_NFF(init({{subpool_config0, subpool_config1, subpool_config2}}, pool_config0));

  // Allocate  buffers from sub pool 0
  for (size_t i{}; i < (*subpools_config_)[0].numMaxBuffers; ++i) {
    ASSERT_NFF(popBufferAndValidate((*subpools_config_)[0].bufferSize));
  }

  // subpool has no more resources, so it should allocate one more from the next subpool
  ASSERT_NFF(popBufferAndValidate((*subpools_config_)[0].bufferSize, (*subpools_config_)[1].bufferSize));
}

// Test MultiSizeBufferPool::getBufferBySubpoolSelector by allocating numMaxBuffers on all subpools, then trying to
// allocate a buffer on the smallest sized pool. Thead will wait for a buffer, then we release a buffer on that subpool
// and see that allocation from the smallest pool succeed.
TEST_F(MultiSizeBufferPoolTestFixture, Subpool_getBufferBySubpoolSelector_Check_all_subpools_and_wait_for_original) {
  ASSERT_NFF(init({{subpool_config0, subpool_config1, subpool_config2, subpool_config3}}, pool_config0));

  // Allocate all buffers on all subpools
  for (const auto& subpool_config : (*subpools_config_)) {
    for (size_t i{}; i < subpool_config.numMaxBuffers; ++i) {
      ASSERT_NFF(popBufferAndValidate(subpool_config.bufferSize));
    }
  }
  // pool should be empty
  ASSERT_TRUE(isPoolEmpty());

  // Pool has no more resources, allocating one more time triggers a wait. To do it on a unittest we must
  // run in a worker thread
  auto worker_allocator_future =
      std::async(std::launch::async, [this]() { ASSERT_NFF(popBufferAndValidate((*subpools_config_)[0].bufferSize)); });
  ASSERT_TRUE(worker_allocator_future.valid());
  ASSERT_TRUE(isSubPoolEmpty((*subpools_config_)[0].bufferSize));

  // release one buffer from the minimal pool and wait for worker thread to allocate successfully
  ASSERT_TRUE(isPoolEmpty());
  ASSERT_NFF(pushArbitraryBuffer((*subpools_config_)[0].bufferSize));
  worker_allocator_future.wait();

  // Pool should be still empty
  ASSERT_TRUE(isPoolEmpty());
}

// Test pool when creating a pool an external subpool_config selector. Here we will implement a maximum buffer size
// selector.
TEST_F(MultiSizeBufferPoolTestFixture, test_external_subpool_selector) {
  class MaximalSizeSubpoolSelector : public MultiSizeBufferPool::ISubpoolSelector {
   public:
    MaximalSizeSubpoolSelector(const MultiSizeBufferPool::SubpoolsConfig& subpools_config)
        : MultiSizeBufferPool::ISubpoolSelector(subpools_config) {}
    const MultiSizeBufferPool::SubpoolConfig& selectSubpool() const override { return config_.back(); }
    std::string_view name() const override { return "MaximalSizeSubpoolSelector"; }
    void reportBufferUsage(uint32_t subpoolSize, uint32_t actualRequiredSize) override{};
  };

  // pool with 3 subpools config, pass the custom selector
  MultiSizeBufferPool::SubpoolsConfig subpools_config{{subpool_config0, subpool_config1, subpool_config2}};
  ASSERT_NFF(init(subpools_config, pool_config0, std::make_unique<MaximalSizeSubpoolSelector>(subpools_config)));

  // allocate all buffers with max subpool_config selector
  for (size_t i{}; i < (*subpools_config_)[2].numMaxBuffers; ++i) {
    ASSERT_NFF(popBufferAndValidate(0, (*subpools_config_)[2].bufferSize, true));
  }

  // allocate 1 buffer by minimal size from other subpools config
  for (const auto& subpool_config : (*subpools_config_)) {
    if ((*subpools_config_)[2].bufferSize == subpool_config.bufferSize) continue;
    ASSERT_NFF(popBufferAndValidate(subpool_config.bufferSize, subpool_config.bufferSize, true));
  }

  // Try to allocate one more buffer by subpool_config selector. This should cause wait on worker thread.
  auto worker_allocator_future = std::async(
      std::launch::async, [this]() { ASSERT_NFF(popBufferAndValidate(0, (*subpools_config_)[2].bufferSize)); });
  ASSERT_TRUE(worker_allocator_future.valid());
  ASSERT_TRUE(isSubPoolEmpty((*subpools_config_)[2].bufferSize));

  // release the buffer
  ASSERT_NFF(pushArbitraryBuffer((*subpools_config_)[2].bufferSize));
  worker_allocator_future.wait();
  ASSERT_TRUE(isSubPoolEmpty((*subpools_config_)[2].bufferSize));
}

class MultiSizeBufferPoolParametrizedTestFixture1
    : public MultiSizeBufferPoolTestFixture,
      public testing::WithParamInterface<MultiSizeBufferPool::SubpoolsConfig> {};

TEST_P(MultiSizeBufferPoolParametrizedTestFixture1, validate_statistics_parametrized) {
  auto subpools_config = GetParam();
  auto chunkMetadataSize = getChunkMetadataSize();
  ASSERT_NFF(init(subpools_config, pool_config0));

  auto& o = stats_.overall_;  // local overall stats marked as o
  auto& c = stats_.current_;  // local current stats marked as o

  // validate initial number of chunks
  for (const auto& subpool_config : subpools_config) {
    const auto& sp = getSubPool(subpool_config.bufferSize);
    const auto& spStats = getSubPoolStats(sp);
    const auto& current_stats = spStats.current_;
    ASSERT_EQ(current_stats.numAllocatedChunks_, subpool_config.numInitialBuffers);
    ASSERT_EQ(current_stats.numNonAllocatedChunks_, subpool_config.numMaxBuffers - subpool_config.numInitialBuffers);
    ASSERT_EQ(current_stats.numUsedChunks_, 0);
    ASSERT_EQ(current_stats.numUnusedChunks_, current_stats.numAllocatedChunks_);
    c.numAllocatedChunks_ += current_stats.numAllocatedChunks_;
    c.numNonAllocatedChunks_ += current_stats.numNonAllocatedChunks_;
    c.numUsedChunks_ += current_stats.numUsedChunks_;
    c.numUnusedChunks_ += current_stats.numUnusedChunks_;
    c.numAllocatedBytes_ += current_stats.numAllocatedChunks_ * (chunkMetadataSize + subpool_config.bufferSize);
  }
  o.numAllocatedBytes_.store(c.numAllocatedBytes_);
  o.numAllocatedChunks_.store(c.numAllocatedChunks_);

  // validate initial pool statistics vs subpools
  const auto& target_pool_stats = getPoolStats();
  ASSERT_EQ(target_pool_stats.current_.numAllocatedChunks_, c.numAllocatedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numNonAllocatedChunks_, c.numNonAllocatedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numUsedChunks_, c.numUsedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numUnusedChunks_, c.numUnusedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numAllocatedBytes_, c.numAllocatedBytes_);

  // validate overalls
  ASSERT_EQ(o.numAllocatedBytes_, target_pool_stats.overall_.numAllocatedBytes_);
  ASSERT_EQ(o.numAllocatedChunks_, target_pool_stats.overall_.numAllocatedChunks_);
  ASSERT_EQ(o.numDeletedBytes_, target_pool_stats.overall_.numDeletedBytes_);
  ASSERT_EQ(o.numUsedBytes_, target_pool_stats.overall_.numUsedBytes_);
  ASSERT_EQ(o.numDeletedChunks_, target_pool_stats.overall_.numDeletedChunks_);
  ASSERT_EQ(o.numUsedChunks_, target_pool_stats.overall_.numUsedChunks_);

  // Allocated maximum chunks from each subpool and validate again.
  // None of the single allocations should trigger an allocation from heap
  c.numAllocatedChunks_ = 0;
  c.numNonAllocatedChunks_ = 0;
  c.numUnusedChunks_ = 0;
  c.numAllocatedBytes_ = 0;
  c.numAllocatedChunks_ = 0;
  for (const auto& subpool_config : subpools_config) {
    const auto& sp = getSubPool(subpool_config.bufferSize);
    const auto& spStats = getSubPoolStats(sp);
    const auto& current_stats = spStats.current_;
    for (size_t i{}; i < subpool_config.numMaxBuffers; ++i) {
      ASSERT_NFF(popBufferAndValidate(subpool_config.bufferSize));
    }
    ASSERT_EQ(current_stats.numAllocatedChunks_, subpool_config.numMaxBuffers);
    ASSERT_EQ(current_stats.numNonAllocatedChunks_, 0);
    ASSERT_EQ(current_stats.numUsedChunks_, subpool_config.numMaxBuffers);
    ASSERT_EQ(current_stats.numUnusedChunks_, 0);
    c.numAllocatedChunks_ += subpool_config.numMaxBuffers;
    c.numUsedChunks_ += subpool_config.numMaxBuffers;
    c.numAllocatedBytes_ += subpool_config.numMaxBuffers * (subpool_config.bufferSize + chunkMetadataSize);
  }
  o.numAllocatedBytes_.store(target_pool_stats.current_.numAllocatedBytes_);
  o.numAllocatedChunks_.store(target_pool_stats.current_.numAllocatedChunks_);
  o.numUsedBytes_.store(target_pool_stats.current_.numUsedBytes_);
  o.numUsedChunks_.store(target_pool_stats.current_.numUsedChunks_);

  // validate initial pool statistics vs subpools
  ASSERT_EQ(target_pool_stats.current_.numAllocatedChunks_, c.numAllocatedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numNonAllocatedChunks_, c.numNonAllocatedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numUsedChunks_, c.numUsedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numUnusedChunks_, c.numUnusedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numAllocatedBytes_, c.numAllocatedBytes_);

  // validate numUsedBytes
  for (const auto& [bufferSize, buffers] : allocated_buffers_) {
    c.numUsedBytes_ += buffers.size() * (bufferSize + chunkMetadataSize);
  }
  ASSERT_EQ(target_pool_stats.current_.numUsedBytes_, c.numUsedBytes_);

  // return all buffers and validate again
  for (auto const& [_, sub_buffers] : allocated_buffers_) {
    UNUSED(_);
    for (const auto& buffer : sub_buffers) {
      pool_->returnBuffer(buffer);
      --c.numUsedChunks_;
      ++c.numUnusedChunks_;
    }
  }
  resetAllocatedBuffers();
  ASSERT_EQ(target_pool_stats.current_.numAllocatedChunks_, c.numAllocatedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numNonAllocatedChunks_, c.numNonAllocatedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numUsedChunks_, c.numUsedChunks_);
  ASSERT_EQ(target_pool_stats.current_.numUnusedChunks_, c.numUnusedChunks_);

  // allocate 1 buffer from each subpool, then check overall statistics
  for (const auto& subpool_config : subpools_config) {
    ASSERT_NFF(popBufferAndValidate(subpool_config.bufferSize));
    o.numUsedBytes_ += subpool_config.bufferSize + chunkMetadataSize;
    ++o.numUsedChunks_;
  }
  ASSERT_EQ(target_pool_stats.overall_.numAllocatedBytes_, o.numAllocatedBytes_);
  ASSERT_EQ(target_pool_stats.overall_.numDeletedBytes_, o.numDeletedBytes_);
  ASSERT_EQ(target_pool_stats.overall_.numUsedBytes_, o.numUsedBytes_);
  ASSERT_EQ(target_pool_stats.overall_.numAllocatedChunks_, o.numAllocatedChunks_);
  ASSERT_EQ(target_pool_stats.overall_.numDeletedChunks_, o.numDeletedChunks_);
  ASSERT_EQ(target_pool_stats.overall_.numUsedChunks_, o.numUsedChunks_);

  // check report on failure: report that client actually needed x2 of buffer size and validate counter goes up
  ASSERT_EQ(target_pool_stats.overall_.numBufferUsageFailed_, 0);
  for (size_t i{}; i < 3; ++i) {
    auto buffer = *allocated_buffers_.begin()->second.begin();
    auto header = getBufferHeader(buffer);
    ASSERT_NFF(pool_->reportBufferUsage(*allocated_buffers_.begin()->second.begin(), header->bufferSize_ * 2));
  }
  ASSERT_EQ(target_pool_stats.overall_.numBufferUsageFailed_, 3);
}

INSTANTIATE_TEST_CASE_P(validate_statistics_parametrized,
                        MultiSizeBufferPoolParametrizedTestFixture1,
                        ::testing::Values(MultiSizeBufferPool::SubpoolsConfig{{300, 3, 3}},
                                          MultiSizeBufferPool::SubpoolsConfig{{300, 1, 3}},
                                          MultiSizeBufferPool::SubpoolsConfig{{300, 2, 6}, {400, 4, 6}}), );

// with a single subpool, take all buffers and then wait with numMaxBuffers threads. See that every time a single
// buffer is returned, 1 waiting thread is freed and taking the returned buffer.
TEST_F(MultiSizeBufferPoolTestFixture, test_multithreading1) {
  ASSERT_NFF(init({{subpool_config0}}, pool_config0));

  // allocate all buffers
  for (size_t i{}; i < (*subpools_config_)[0].numMaxBuffers; ++i) {
    ASSERT_NFF(popBufferAndValidate(0, (*subpools_config_)[0].bufferSize, true));
  }
  ASSERT_TRUE(isSubPoolEmpty((*subpools_config_)[0].bufferSize));
  ASSERT_TRUE(isPoolEmpty());

  std::vector<std::future<void>> workers_futures;
  std::atomic_size_t counter{};
  // Try to pop, worker threads should get "stuck" on an infinite wait
  for (size_t i{}; i < (*subpools_config_)[0].numMaxBuffers; ++i) {
    auto f = std::async(std::launch::async, [&, this]() {
      ASSERT_NFF(popBufferAndValidate(0, (*subpools_config_)[0].bufferSize, true));
      ++counter;
    });
    ASSERT_TRUE(f.valid());
    workers_futures.push_back(std::move(f));
  }

  // Wait some for worker threads to reach infinite wait, then start releasing buffers
  std::this_thread::sleep_for(100ms);
  ASSERT_EQ(counter, 0);
  for (size_t i{}; i < (*subpools_config_)[0].numMaxBuffers; ++i) {
    ASSERT_NFF(pushArbitraryBuffer());
  }
  // wait for all to finish
  for (const auto& f : workers_futures) {
    f.wait();
  }
  ASSERT_EQ(counter, (*subpools_config_)[0].numMaxBuffers);
  ASSERT_TRUE(isSubPoolEmpty((*subpools_config_)[0].bufferSize));
  ASSERT_TRUE(isPoolEmpty());
}

// Similar to test_multithreading1,but check that all threads go out when pool is destroyed
TEST_F(MultiSizeBufferPoolTestFixture, test_multithreading2) {
  ASSERT_NFF(init({{subpool_config0}}, pool_config0));

  // allocate all buffers
  for (size_t i{}; i < (*subpools_config_)[0].numMaxBuffers; ++i) {
    ASSERT_NFF(popBufferAndValidate(0, (*subpools_config_)[0].bufferSize, true));
  }
  ASSERT_TRUE(isSubPoolEmpty((*subpools_config_)[0].bufferSize));
  ASSERT_TRUE(isPoolEmpty());

  std::vector<std::future<void>> workers_futures;
  std::atomic_size_t counter{};
  // Try to pop, worker threads should get blocked, but eventually get a buffer when another thread frees one
  for (size_t i{}; i < (*subpools_config_)[0].numMaxBuffers; ++i) {
    auto f = std::async(std::launch::async, [&, this]() {
      auto [buffer, actual_buffer_size] = pool_->getBufferBySubpoolSelector();
      (void)actual_buffer_size;
      ASSERT_TRUE(buffer);
      ++counter;
    });
    ASSERT_TRUE(f.valid());
    workers_futures.push_back(std::move(f));
  }

  // Wait some for worker threads to reach infinite wait, then push back all chunks to see that they are all out and
  // pull is not empty
  ASSERT_TRUE(isPoolEmpty());
  ASSERT_NFF(pushAllBuffers());
  for (const auto& f : workers_futures) {
    f.wait();
  }
  std::this_thread::sleep_for(100ms);
  ASSERT_EQ(counter, (*subpools_config_)[0].numMaxBuffers);
  ASSERT_TRUE(isPoolEmpty());
}

// Test external purger passed on constructor
TEST_F(MultiSizeBufferPoolTestFixture, Subpool_test_external_purger) {
  class BrutalPoolPurger : public MultiSizeBufferPool::IPurger {
   public:
    std::string_view name() const override { return "BrutalPoolPurger"; }
    virtual void evaluate(MultiSizeBufferPool& pool) override {
      const auto& subpoolsBufferSizes = pool.getSubpoolsBufferSizes();
      for (const auto& buffersize : subpoolsBufferSizes) pool.setPurgeState(buffersize, true, 0);
    }
  };

  auto modifiedConfig = pool_config0;
  decltype(modifiedConfig.purgeEvaluationFrequency) purgeEvaluationFrequency = 1;  // every second evaluate
  memcpy((void*)&modifiedConfig.purgeEvaluationFrequency,
         (void*)&purgeEvaluationFrequency,
         sizeof(purgeEvaluationFrequency));

  // pool with 3 subpools config, pass the custom selector
  MultiSizeBufferPool::SubpoolsConfig subpools_config{{subpool_config0, subpool_config1, subpool_config2}};
  ASSERT_NFF(init(subpools_config, modifiedConfig, nullptr, std::make_unique<BrutalPoolPurger>()));

  // Allocate all buffers on all subpools
  ASSERT_FALSE(isPoolEmpty());
  for (const auto& subpool_config : (*subpools_config_)) {
    for (size_t i{}; i < subpool_config.numMaxBuffers; ++i) {
      ASSERT_NFF(popBufferAndValidate(subpool_config.bufferSize));
    }
  }

  // trigger purger, then return all buffers. Pool should deallocate and become empty.
  // Check statistics as well
  ASSERT_NFF(doPeriodic());
  ASSERT_NFF(pushAllBuffers());
  ASSERT_TRUE(isPoolEmpty());

  auto chunkMetadataSize = getChunkMetadataSize();
  auto totalBytes = subpool_config0.numMaxBuffers * (subpool_config0.bufferSize + chunkMetadataSize) +
                    subpool_config1.numMaxBuffers * (subpool_config1.bufferSize + chunkMetadataSize) +
                    subpool_config2.numMaxBuffers * (subpool_config2.bufferSize + chunkMetadataSize);
  auto totalChunks = subpool_config0.numMaxBuffers + subpool_config1.numMaxBuffers + subpool_config2.numMaxBuffers;
  const auto& target_pool_stats = getPoolStats();
  ASSERT_EQ(target_pool_stats.current_.numAllocatedBytes_, 0);
  ASSERT_EQ(target_pool_stats.current_.numNonAllocatedBytes_, totalBytes);
  ASSERT_EQ(target_pool_stats.current_.numUsedBytes_, 0);
  ASSERT_EQ(target_pool_stats.current_.numUnusedBytes_, 0);
  ASSERT_EQ(target_pool_stats.current_.numAllocatedChunks_, 0);
  ASSERT_EQ(target_pool_stats.current_.numNonAllocatedChunks_, totalChunks);
  ASSERT_EQ(target_pool_stats.current_.numUsedChunks_, 0);
  ASSERT_EQ(target_pool_stats.current_.numUnusedChunks_, 0);
  ASSERT_EQ(target_pool_stats.overall_.numDeletedBytes_, totalBytes);
  ASSERT_EQ(target_pool_stats.overall_.numDeletedChunks_, totalChunks);
}

}  // namespace concordUtil::test

int main(int argc, char** argv) {
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_death_test_style =
      "threadsafe";  // mitigate the risks of testing in a possibly multithreaded environment

  return RUN_ALL_TESTS();
}
