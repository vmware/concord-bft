// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//

#include "gtest/gtest.h"
#include "SimpleAutoResetEvent.hpp"
#include "SimpleThreadPool.hpp"
#include "Handoff.hpp"
#include <thread>
#include <chrono>
#include <array>
using namespace concord;
namespace test {
namespace mt {

/**
 * Fixture for testing SimpleThreadPool
 */
class SimpleThreadPoolFixture : public testing::Test {
 protected:
  class TestJob : public concord::util::SimpleThreadPool::Job {
   public:
    TestJob(SimpleThreadPoolFixture* f, uint32_t sleep_ms = 10) : fixture_(f), sleep_ms_(sleep_ms) {}
    void execute() {
      fixture_->result++;
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms_));  // don't exit immediately
    }
    void release() { delete this; }
    virtual ~TestJob() {}

    SimpleThreadPoolFixture* fixture_;
    uint32_t sleep_ms_;
  };

  SimpleThreadPoolFixture() : result(0) {}

  // Sets up the test fixture.
  virtual void SetUp() {
    ASSERT_GT(std::thread::hardware_concurrency(), 1);
    pool_.start(std::thread::hardware_concurrency());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // let the pool actually start all its threads
    EXPECT_EQ(pool_.getNumOfThreads(), std::thread::hardware_concurrency());
  }

  // Tears down the test fixture.
  virtual void TearDown() {
    ASSERT_EQ(pool_.getNumOfThreads(), 0);
    ASSERT_EQ(pool_.getNumOfJobs(), 0);
  }

  concord::util::SimpleThreadPool pool_;
  std::atomic_int result;
};

TEST_F(SimpleThreadPoolFixture, ThreadPoolStartStopNoJobs) { pool_.stop(); }

TEST_F(SimpleThreadPoolFixture, ThreadPoolStartStopWithJobsNoExecute) {
  for (int i = 0; i < 100; ++i) pool_.add(new TestJob(this));
  pool_.stop(false);
}

TEST_F(SimpleThreadPoolFixture, ThreadPoolStartStopWithJobsExecute) {
  for (int i = 0; i < 100; ++i) pool_.add(new TestJob(this));
  pool_.stop(true);
  EXPECT_EQ(result, 100);
}

TEST_F(SimpleThreadPoolFixture, ThreadPoolMultipleProducers) {
  std::array<std::thread, 10> producers;
  for (int i = 0; i < 10; ++i) {
    producers[i] = std::thread([this] {
      for (int i = 0; i < 100; ++i) pool_.add(new TestJob(this, 1));
    });
  }
  for (auto& t : producers) t.join();
  pool_.stop(true);
  EXPECT_EQ(result, 1000);
}

/**
 * Fixture for testing Handoff
 */
using namespace std::placeholders;
class HandoffFixture : public testing::Test {
 protected:
  // Sets up the test fixture.
  virtual void SetUp() {
    ASSERT_GT(std::thread::hardware_concurrency(), 1);
    handoff_ = new concord::util::Handoff(0);
  }

  concord::util::Handoff* handoff_;
  std::atomic_int result = 0;
};

TEST_F(HandoffFixture, Basic) {
  auto g = std::bind([this](int i) { this->result += i; }, _1);
  auto f = std::bind(&concord::util::Handoff::push, handoff_, _1, _2);

  // run 10000 unblocked, and then 10000  blocked
  for (size_t i{}; i < 2; ++i) {
    for (size_t j = 1; j <= 10000; ++j) {
      f(std::bind(g, j), static_cast<bool>(i));
    }
  }
  delete handoff_;
  // Sum of 2 arithmetic progression series
  EXPECT_EQ(result, 10000 * (1 + 10000));
}

}  // namespace mt
}  // namespace test
