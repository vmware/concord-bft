// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "gtest/gtest.h"

#include "thread_pool.hpp"

#include <functional>
#include <future>
#include <thread>
#include <vector>

using namespace concord::util;

namespace {

constexpr auto answer = 42;

auto func() { return answer; }
auto (*func_ptr)() = func;
auto (&func_ref)() = func;

// Make sure the pool can execute lambdas.
TEST(thread_pool, lambda) {
  auto pool = ThreadPool{};
  auto future = pool.async([]() { return answer; });
  ASSERT_EQ(answer, future.get());
}

// Make sure the pool can execute functions.
TEST(thread_pool, functions) {
  auto pool = ThreadPool{};
  auto future1 = pool.async(func);
  auto future2 = pool.async(&func);
  auto future3 = pool.async(func_ptr);
  auto future4 = pool.async(func_ref);
  ASSERT_EQ(answer, future1.get());
  ASSERT_EQ(answer, future2.get());
  ASSERT_EQ(answer, future3.get());
  ASSERT_EQ(answer, future4.get());
}

// Make sure the pool can execute std::function objects.
TEST(thread_pool, std_func) {
  auto pool = ThreadPool{};
  auto std_func = std::function{func};
  auto future = pool.async(std_func);
  ASSERT_EQ(answer, future.get());
}

// Make sure exceptions from the user-passed function are correctly propagated.
TEST(thread_pool, exception) {
  auto pool = ThreadPool{};
  auto future = pool.async([]() { throw answer; });
  ASSERT_THROW(future.get(), decltype(answer));
}

// Make sure that the returned futures' desctructors don't block.
TEST(thread_pool, non_blocking_future_dtors) {
  auto pool = ThreadPool{};
  auto future1 = pool.async(func);
  ASSERT_TRUE(future1.valid());
  auto future2 = pool.async(func);
  ASSERT_EQ(answer, future2.get());
  // future1's dtor doesn't block if get()/wait() hasn't been called.
}

// Make sure that adding more tasks than the concurrency supported by the system works.
TEST(thread_pool, more_tasks_than_concurrency) {
  auto pool = ThreadPool{};
  const auto conc = std::thread::hardware_concurrency() * 10;
  std::vector<std::future<int>> futures;
  for (auto i = 0u; i < conc; ++i) {
    futures.push_back(pool.async(func));
  }
  for (auto& future : futures) {
    ASSERT_EQ(answer, future.get());
  }
}

// Make sure that adding tasks from different threads works properly.
TEST(thread_pool, add_tasks_from_different_threads) {
  auto pool = ThreadPool{};
  auto async_future = std::async(std::launch::async, [&pool]() { pool.async(func); });
  auto pool_future = pool.async(func);
  ASSERT_EQ(answer, pool_future.get());
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
