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

#include <assertUtils.hpp>

#include <atomic>
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
 private:
  std::size_t nextQueueIndex() { return (queue_counter_++) % task_queues_.size(); }
  auto& nextQueue() { return task_queues_[nextQueueIndex()]; }

 public:
  // Starts the thread pool with thread_count > 0 threads.
  ThreadPool(unsigned int thread_count) : task_queues_{thread_count} {
    Assert(thread_count > 0);
    for (auto thread_index = 0u; thread_index < thread_count; ++thread_index) {
      threads_.emplace_back([this, thread_index]() { loop(thread_index); });
    }
  }

  // Starts the thread pool with the maximum number of concurrent threads supported by the implementation.
  ThreadPool() : ThreadPool{std::thread::hardware_concurrency() ? std::thread::hardware_concurrency() : 1} {}

  // Stops the thread pool. Waits for the currently executing tasks only (will not exhaust the queues).
  ~ThreadPool() noexcept {
    stop_ = true;
    for (auto& q : task_queues_) {
      q.cv.notify_one();
    }
    for (auto& t : threads_) {
      t.join();
    }
  }

 public:
  // Executes the passed function (or any callable) in a pool thread. Returns a future to the result.
  template <class F>
  auto async(F&& func) {
    using ResultType = std::invoke_result_t<std::decay_t<F>>;
    auto ptask = std::packaged_task<ResultType()>{std::forward<F>(func)};
    auto future = ptask.get_future();
    auto task = GenericTask{[ptask = std::move(ptask)]() mutable { ptask(); }};
    auto& queue = nextQueue();
    {
      auto lock = std::lock_guard{queue.mutex};
      queue.tasks.push(std::move(task));
    }
    queue.cv.notify_one();
    return future;
  }

 private:
  using GenericTask = std::packaged_task<void()>;

  void loop(std::size_t thread_index) noexcept {
    auto& queue = task_queues_[thread_index];
    while (true) {
      auto lock = std::unique_lock{queue.mutex};
      while (queue.tasks.empty() && !stop_) {
        queue.cv.wait(lock);
      }
      if (stop_) break;
      Assert(!queue.tasks.empty());
      auto task = std::move(queue.tasks.front());
      queue.tasks.pop();
      lock.unlock();
      task();
    }
  }

 private:
  // Represents a per-thread task queue.
  struct TaskQueue {
    // A queue of tasks for execution.
    std::queue<GenericTask> tasks;

    // A mutex for the queue.
    std::mutex mutex;

    // A condition variable to signal the queue.
    std::condition_variable cv;
  };

  // Each thread has its own queue - rationale is that doing so will lessen the contention compared to having a single
  // queue.
  std::vector<TaskQueue> task_queues_;

  // A counter used for distributing tasks among queues. Relies on unsigned wraparound.
  std::atomic<unsigned int> queue_counter_{0};

  // A global stop flag.
  std::atomic_bool stop_{false};

  // A list of threads.
  std::vector<std::thread> threads_;
};

}  // namespace concord::util
