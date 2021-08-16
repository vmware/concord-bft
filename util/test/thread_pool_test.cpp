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

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace concord::util;

using AnswerType = int;
constexpr auto answer = AnswerType{42};
const auto concurrency = std::thread::hardware_concurrency() > 0 ? std::thread::hardware_concurrency() : 1;

auto func() { return answer; }
auto (*func_ptr)() = func;
auto (&func_ref)() = func;

auto identity(AnswerType v) { return v; }

void void_func(AnswerType) {}

struct MoveOnly {
  MoveOnly() = default;
  MoveOnly(const MoveOnly&) = delete;
  MoveOnly(MoveOnly&&) = default;
  MoveOnly& operator=(const MoveOnly&) = delete;
  MoveOnly& operator=(MoveOnly&&) = default;
};

// Deleting the move ctor is omitted intentionally as we don't want it to take part in overload resolution.
// Based on https://stackoverflow.com/questions/40536060/c-perfect-forward-copy-only-types-to-make-tuple
struct CopyOnly {
  CopyOnly() = default;
  CopyOnly(const CopyOnly&) = default;
  CopyOnly& operator=(const CopyOnly&) = default;
  CopyOnly& operator=(CopyOnly&&) = delete;
};

void own(MoveOnly&&) {}
void copy(CopyOnly) {}

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

// Make sure we can execute a void function.
TEST(thread_pool, void_func) {
  auto pool = ThreadPool{};
  auto future = pool.async(void_func, answer);
  ASSERT_NO_THROW(future.wait());
}

// Make sure async supports arguments.
TEST(thread_pool, arguments_with_return) {
  auto pool = ThreadPool{};
  auto future = pool.async(identity, answer + 1);
  ASSERT_EQ(answer + 1, future.get());
}

// Make sure we can execute tasks that have different return types.
TEST(thread_pool, different_task_return_types) {
  auto pool = ThreadPool{};
  auto future1 = pool.async(func);
  auto future2 = pool.async([]() {});
  auto future3 = pool.async([]() { return std::string{"s"}; });
  ASSERT_EQ(answer, future1.get());
  ASSERT_NO_THROW(future2.wait());
  ASSERT_EQ("s", future3.get());
}

// Make sure we can move arguments inside async.
TEST(thread_pool, move_only_arguments) {
  auto pool = ThreadPool{};
  auto future = pool.async(own, MoveOnly{});
  ASSERT_NO_THROW(future.wait());
}

// Make sure we can pass copy-only arguments to async.
TEST(thread_pool, copy_only_arguments) {
  auto pool = ThreadPool{};
  auto future = pool.async(copy, CopyOnly{});
  ASSERT_NO_THROW(future.wait());
}

TEST(thread_pool, pointer_arguments_are_copied) {
  auto pool = ThreadPool{};
  auto i = std::atomic_int{5};
  auto p = &i;
  auto future = pool.async(
      [](auto* v) {
        *v = 13;
        v = nullptr;
      },
      p);
  ASSERT_NO_THROW(future.wait());
  ASSERT_EQ(i, 13);
  ASSERT_EQ(p, &i);
}

TEST(thread_pool, non_const_lvalues_are_copied) {
  auto pool = ThreadPool{};
  auto i = 5;
  auto future = pool.async([](auto v) { v = 13; }, i);
  ASSERT_NO_THROW(future.wait());
  ASSERT_EQ(i, 5);
}

TEST(thread_pool, std_ref) {
  auto pool = ThreadPool{};
  auto i = std::atomic_int{5};
  auto future = pool.async([](auto v) { v.get() = 13; }, std::ref(i));
  ASSERT_NO_THROW(future.wait());
  ASSERT_EQ(i, 13);
}

// Multiple arguments of different types.
TEST(thread_pool, multiple_arguments) {
  auto pool = ThreadPool{};
  auto future = pool.async([](auto&& answer, auto&&, auto&&) { return answer; }, answer, std::string{"s"}, 3.14);
  ASSERT_EQ(answer, future.get());
}

// Make sure exceptions from the user-passed function are correctly propagated.
TEST(thread_pool, exception) {
  auto pool = ThreadPool{};
  // NOLINTNEXTLINE(misc-throw-by-value-catch-by-reference)
  auto future = pool.async([]() { throw answer; });
  ASSERT_THROW(future.get(), decltype(answer));
}

// Make sure that the returned futures' desctructors don't block.
TEST(thread_pool, non_blocking_future_dtors) {
  // Keep these variables before the pool as we capture them by reference and we'd like them to be valid (not
  // destructed) in all pool threads.
  auto future1_destroyed = false;
  auto mtx = std::mutex{};
  auto cv = std::condition_variable{};

  auto pool = ThreadPool{};

  {
    auto future1 = pool.async([&]() {
      auto lock = std::unique_lock{mtx};
      // NOLINTNEXTLINE(bugprone-infinite-loop)
      while (!future1_destroyed) {
        cv.wait(lock);
      }
    });
    ASSERT_TRUE(future1.valid());
    // future1's dtor doesn't block if get()/wait() hasn't been called.
  }

  // Signal the thread so that the lambda will end and the pool dtor will not block forever.
  {
    auto lock = std::unique_lock{mtx};
    future1_destroyed = true;
  }
  cv.notify_one();

  auto future2 = pool.async(func);
  ASSERT_EQ(answer, future2.get());
}

// Make sure that adding more tasks than the concurrency supported by the system works.
TEST(thread_pool, more_tasks_than_concurrency) {
  auto pool = ThreadPool{};
  const auto tasks = concurrency * 10;
  auto futures = std::vector<std::future<AnswerType>>{};
  for (auto i = 0u; i < tasks; ++i) {
    futures.push_back(pool.async(func));
  }
  for (auto& future : futures) {
    ASSERT_EQ(answer, future.get());
  }
}

// Make sure that the pool works correctly with a single thread.
TEST(thread_pool, one_thread) {
  auto pool = ThreadPool{1};
  const auto tasks = 16u;
  auto futures = std::vector<std::future<AnswerType>>{};
  for (auto i = 0u; i < tasks; ++i) {
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
