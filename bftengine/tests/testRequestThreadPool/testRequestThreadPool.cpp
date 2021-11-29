// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <stdint.h>
#include <vector>
#include <future>

#include "gtest/gtest.h"

#include "bftengine/ReplicaConfig.hpp"
#include "RequestThreadPool.hpp"

namespace {
using namespace bftEngine::impl;

using AnswerType = int;
constexpr auto answer2 = AnswerType{84};
auto funcLevel2() { return answer2; }

void setNumOfThreads(uint32_t level1NumThreads, uint32_t level2NumThreads) {
  bftEngine::ReplicaConfig& config = bftEngine::ReplicaConfig::instance();
  config.threadbagConcurrencyLevel1 = level1NumThreads;
  config.threadbagConcurrencyLevel2 = level2NumThreads;
}

void level2ThreadPool(uint32_t numThreads) {
  std::vector<std::future<AnswerType>> tasks;
  try {
    static auto& tp = RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::FIRSTLEVEL);
    for (uint32_t i = 0; i <= 10 * numThreads; i++) {
      tasks.push_back(tp.async([]() { return funcLevel2(); }));
    }
    for (const auto& t : tasks) {
      t.wait();
    }
  } catch (std::out_of_range& ex) {
  }
  for (auto& t : tasks) {
    ASSERT_EQ(answer2, t.get());
  }
}

TEST(testRequestThreadPool, selectNonExistentTP) {
  EXPECT_THROW(RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::MAXLEVEL), std::out_of_range);
  EXPECT_THROW(RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::MAXLEVEL + 10), std::out_of_range);
}

TEST(testRequestThreadPool, selectExistentTP) {
  EXPECT_NO_THROW(RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::STARTING));
  EXPECT_NO_THROW(RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::FIRSTLEVEL));
}

TEST(testRequestThreadPool, checkLimitOfTP) {
  setNumOfThreads(10u, 5u);
  std::vector<std::future<void>> tasks;
  try {
    static auto& tp = RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::STARTING);
    for (uint32_t i = 0; i <= 100; i++) {
      tasks.push_back(tp.async([]() { level2ThreadPool(5u); }));
    }
    for (const auto& t : tasks) {
      t.wait();
    }
  } catch (std::out_of_range& ex) {
  }
}

TEST(testRequestThreadPool, sameTP) {
  setNumOfThreads(10u, 5u);
  std::vector<size_t> thread_pool_addr;
  for (uint16_t lvl = RequestThreadPool::PoolLevel::STARTING; lvl < RequestThreadPool::PoolLevel::MAXLEVEL; lvl++) {
    static auto& tp = RequestThreadPool::getThreadPool(lvl);
    size_t addr_tp = reinterpret_cast<size_t>(&tp);
    thread_pool_addr.push_back(addr_tp);
  }

  std::vector<std::future<void>> tasks;

  try {
    auto pool = concord::util::ThreadPool{};
    for (uint32_t i = 0; i < 10000; i++) {
      tasks.push_back(pool.async([&thread_pool_addr]() {
        std::vector<size_t> thread_pool_addr_tp;
        for (uint16_t lvl = RequestThreadPool::PoolLevel::STARTING; lvl < RequestThreadPool::PoolLevel::MAXLEVEL;
             lvl++) {
          static auto& tp = RequestThreadPool::getThreadPool(lvl);
          size_t addr_tp = reinterpret_cast<size_t>(&tp);
          thread_pool_addr_tp.push_back(addr_tp);
        }
        ASSERT_EQ(thread_pool_addr.size(), thread_pool_addr_tp.size());
        for (size_t j = 0; j < thread_pool_addr_tp.size(); j++) {
          ASSERT_EQ(thread_pool_addr[j], thread_pool_addr_tp[j]);
        }
      }));
    }
    for (const auto& t : tasks) {
      t.wait();
    }
  } catch (std::out_of_range& ex) {
  }
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
