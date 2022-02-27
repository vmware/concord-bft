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

// Based on Mastering the C++17 STL, page 204:
// https://books.google.bg/books?id=zJlGDwAAQBAJ&pg=PA205&lpg=PA205#

#pragma once

#include <assertUtils.hpp>

#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace concord::util {

// A thread pool that supports any callable object with any return type. Returns std::future objects to users.
class ThreadPool {
 public:
  // Starts the thread pool with thread_count > 0 threads.
  ThreadPool(unsigned int thread_count) noexcept {
    ConcordAssert(thread_count > 0);
    for (auto i = 0u; i < thread_count; ++i) {
      threads_.emplace_back([this]() { loop(); });
    }
  }

  // Starts the thread pool with the maximum number of concurrent threads supported by the implementation.
  ThreadPool() noexcept
      : ThreadPool{std::thread::hardware_concurrency() > 0 ? std::thread::hardware_concurrency() : 1} {}

  // Stops the thread pool. Waits for the currently executing tasks only (will not exhaust the queues).
  ~ThreadPool() noexcept {
    {
      auto lock = std::lock_guard{task_queue_.mutex};
      task_queue_.stop = true;
    }
    task_queue_.cv.notify_all();
    for (auto& t : threads_) {
      t.join();
    }
  }

 public:
  // Executes the passed function (or any callable) in a pool thread. Returns a future to the result.
  // Arguments are always copied or moved. Reference arguments are not supported on purpose. Main reason is safety.
  // Instead, callers need to be explicit and use std::ref() should they require references.
  // Note: The returned future's destructor doesn't block if the future value hasn't been retrieved. This is in contrast
  // to the futures returned by std::async that block.
  template <class F, class... Args>
  auto async(F&& func, Args&&... args) {
    using ResultType = std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>;
    auto ptask = std::packaged_task<ResultType(std::decay_t<Args>...)>{std::forward<F>(func)};
    auto future = ptask.get_future();
    // Use an std::tuple to capture arguments and then std::apply() to unpack them:
    // https://stackoverflow.com/questions/37511129/how-to-capture-a-parameter-pack-by-forward-or-move
    auto task = GenericTask{[ptask = std::move(ptask), tup = std::make_tuple(std::forward<Args>(args)...)]() mutable {
      std::apply(ptask, std::move(tup));
    }};
    {
      auto lock = std::lock_guard{task_queue_.mutex};
      task_queue_.tasks.push(std::move(task));
    }
    task_queue_.cv.notify_one();
    return future;
  }

 private:
  using GenericTask = std::packaged_task<void()>;

  void loop() noexcept {
    while (true) {
      auto lock = std::unique_lock{task_queue_.mutex};
      while (task_queue_.tasks.empty() && !task_queue_.stop) {
        task_queue_.cv.wait(lock);
      }
      if (task_queue_.stop) break;
      ConcordAssert(!task_queue_.tasks.empty());
      auto task = std::move(task_queue_.tasks.front());
      task_queue_.tasks.pop();
      lock.unlock();
      try {
        task();
      } catch (const std::exception& e) {
        LOG_ERROR(logging::getLogger("concord.util.thread-pool"), e.what());
      }
    }
  }

 private:
  struct TaskQueue {
    // A queue of tasks for execution.
    std::queue<GenericTask> tasks;

    // A mutex for the queue.
    std::mutex mutex;

    // A condition variable to signal the queue.
    std::condition_variable cv;

    // A task queue stop flag.
    bool stop{false};
  };

  // A task queue that is shared between pool threads.
  TaskQueue task_queue_;

  // A list of threads.
  std::vector<std::thread> threads_;
};

}  // namespace concord::util
