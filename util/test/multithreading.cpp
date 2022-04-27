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
#include <future>

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
  virtual void SetUp() { ASSERT_GT(std::thread::hardware_concurrency(), 1); }

  HandoffFixture() : handoff_(0){};
  concord::util::Handoff handoff_;

  std::string str_result;
  std::atomic_int result{};
  int num_calls{};
};

TEST_F(HandoffFixture, Basic) {
  using namespace std::chrono_literals;
  static constexpr size_t count_iterations{10000};
  auto g = std::bind(
      [this](int i) {
        result += i;
        ++num_calls;
      },
      _1);
  auto f = std::bind(&concord::util::Handoff::push, &handoff_, _1, _2);

  // run count_iterations unblocked, and then count_iterations blocked
  num_calls = 0;
  for (size_t i{}; i < 2; ++i) {
    for (size_t j = 1; j <= count_iterations; ++j) {
      f(std::bind(g, j), static_cast<bool>(i));
    }
  }

  // let consumer thread finish processing
  std::this_thread::sleep_for(2000ms);
  ASSERT_TRUE(handoff_.empty());
  ASSERT_EQ(handoff_.size(), 0);
  ASSERT_EQ(num_calls, 2 * count_iterations);
  ASSERT_EQ(result, count_iterations * (1 + count_iterations));  // Sum of 2 arithmetic progression series

  // push async call to sleep 1 seconds on consumer thread, then push 100 additional async calls.
  // Since consumer thread will be still sleeping on the initial call - all 100 async calls are pending. Now, push
  // count_iterations blocking calls. We expect them to get executed 1st.
  // sync calls are represented by numeric characters '1' and '2' (rotating).
  // async calls are represented by the letter 'a';
  std::srand(std::time(nullptr));
  num_calls = 0;
  auto s = std::bind(
      [this](char c) {
        this->str_result += c;
        ++num_calls;
      },
      _1);

  // put consumer thread to sleep
  f(std::bind([]() { std::this_thread::sleep_for(2000ms); }), false);

  // push count_iterations async calls
  for (size_t j = 1; j <= count_iterations; ++j) {
    f(std::bind(s, ((j % 2) != 0) ? '1' : '2'), false);
  }

  // now push count_iterations sync calls
  std::vector<std::future<void>> v;
  for (size_t j = 1; j <= count_iterations; ++j) {
    v.emplace_back(std::async(std::launch::async, f, std::bind(s, 'a'), true));
  }

  for (const auto& e : v) {
    e.wait();
  }

  // let consumer finish process all blocked calls
  std::this_thread::sleep_for(2000ms);

  // now check that str_result has the pattern of ababab....121212: all calls are ordered, sync calls before  async
  ASSERT_EQ(handoff_.size(), 0);
  ASSERT_TRUE(handoff_.empty());
  ASSERT_EQ(num_calls, 2 * count_iterations);
  ASSERT_EQ(str_result.size(), count_iterations * 2);

  for (size_t j{0}, i{0}; j < str_result.size(); ++j, ++i) {
    if (j < count_iterations) {
      ASSERT_EQ(str_result[j], 'a');
    } else {
      if (j == count_iterations) {
        i = 0;
      }
      if ((i % 2) == 0) {
        ASSERT_EQ(str_result[j], '1');
      } else {
        ASSERT_EQ(str_result[j], '2');
      }
    }
  }

  // Now, check that when we define a function with an out value, we get the required return value
  // 1) multiply 2 values and get the result into result
  // 2) append a string "_hello" into msg="123"
  auto multiply = std::bind(
      [this](size_t n, size_t m, size_t* result) {
        *result = m * n;
        ++num_calls;
      },
      _1,
      _2,
      _3);

  auto appendStr = std::bind(
      [this](std::string& str, std::string&& str_append) {
        str.append(str_append);
        ++num_calls;
      },
      _1,
      _2);

  // do the same thing once blocked, and once non-blocked
  for (size_t i{}; i < 2; ++i) {
    num_calls = 0;
    size_t result{};
    std::string msg{"123"};
    f(std::bind(multiply, 6, 8, &result), (i == 0));
    f(std::bind(appendStr, std::ref(msg), "_hello"), (i == 0));
    if (i != 0) {
      // async call, wait for short time
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_EQ(handoff_.size(), 0) << KVLOG(i);
    ASSERT_TRUE(handoff_.empty()) << KVLOG(i);
    ASSERT_EQ(num_calls, 2) << KVLOG(i);
    ASSERT_EQ(result, 6 * 8) << KVLOG(i);
    ASSERT_EQ(msg, "123_hello") << KVLOG(i);
  }
}

}  // namespace mt
}  // namespace test
