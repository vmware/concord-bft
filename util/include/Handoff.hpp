// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <queue>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <exception>
#include "Logger.hpp"

// uncomment to add debug prints
#define DO_DEBUG
#ifdef DO_DEBUG
#define DEBUG_PRINT(x, y) LOG_DEBUG(x, y)
#else
#define DEBUG_PRINT(x, y)
#endif

namespace concord::util {
/**
 * Simple handing off to a dedicated thread functionality.
 * Implemented by passing a function with a trivial signature void(void).
 * Parameters are bound to an arbitrary function by a caller.
 *
 * Default behavior is asynchronous: caller push the function and can continue doing other things. If caller would like
 * to block on the function it may mark blocking=true.
 */
class Handoff {
  using guard = std::lock_guard<std::mutex>;
  using func_t = std::function<void()>;
  using tuple_t = std::tuple<uint64_t, func_t, std::condition_variable*, bool&>;

 public:
  Handoff(std::uint16_t replicaId) : element_id_counter_{0} {
    thread_ = std::thread([this, replicaId] {
      try {
        MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(replicaId));
        MDC_PUT(MDC_THREAD_KEY, "handoff");

        DEBUG_PRINT(getLogger(), "Entering loop...");
        for (;;) {
          auto [id, func_to_call, completion_cond, completion_flag] = pop();
          // keep for debug
          if (completion_cond) {
            DEBUG_PRINT(getLogger(), "Before calling id=" << id);
          }
          func_to_call();
          if (completion_cond) {
            DEBUG_PRINT(getLogger(), "After calling id=" << id);
            std::unique_lock<std::mutex> ul(queue_lock_);
            completion_flag = true;
            completion_cond->notify_one();
          }
        }
      } catch (ThreadCanceledException& e) {
        LOG_INFO(getLogger(), "thread cancelled " << std::this_thread::get_id());
      } catch (const std::exception& e) {
        LOG_FATAL(getLogger(), "exception: " << e.what());
        std::terminate();
      }
    });
  }

  ~Handoff() { stop(); }

  void stop() {
    if (stopped_.exchange(true)) return;
    queue_cond_.notify_one();
    auto tid = thread_.get_id();
    thread_.join();
    LOG_INFO(getLogger(), "thread joined " << tid);
  }

  void push(func_t f, bool blocking = false) {
    std::condition_variable completion_cond;
    bool completion_flag{false};
    uint64_t id{};
    {
      guard g(queue_lock_);
      id = element_id_counter_;
      // keep for debug
      if (blocking) {
        DEBUG_PRINT(getLogger(), "Before pushing id=" << id);
      }
      task_queue_.emplace(
          std::forward_as_tuple(id, std::move(f), blocking ? &completion_cond : nullptr, completion_flag));
      // keep for debug
      if (blocking) {
        DEBUG_PRINT(getLogger(), "After pushing id=" << id);
      }
      ++element_id_counter_;
    }
    queue_cond_.notify_one();

    if (blocking) {
      std::unique_lock<std::mutex> ul(queue_lock_);
      DEBUG_PRINT(getLogger(), "Before waiting on id=" << id);
      completion_cond.wait(ul, [&completion_flag] { return completion_flag; });
      DEBUG_PRINT(getLogger(), "After waiting on id=" << id);
    }
  }

  size_t size() const {
    guard g(queue_lock_);
    return task_queue_.size();
  }

 protected:
  class ThreadCanceledException : public std::runtime_error {
   public:
    ThreadCanceledException() : std::runtime_error("thread cancelled") {}
    const char* what() const noexcept override { return std::runtime_error::what(); }
  };

  tuple_t pop() {
    while (true) {
      std::unique_lock<std::mutex> ul(queue_lock_);
      queue_cond_.wait(ul, [this] { return !task_queue_.empty() || stopped_; });
      if (!stopped_ || (stopped_ && !task_queue_.empty())) {
        auto t = task_queue_.front();
        task_queue_.pop();
        return t;
      }
      LOG_INFO(getLogger(), "Handoff thread stopped!");
      throw ThreadCanceledException();
    }
  }

  static logging::Logger getLogger() {
    static logging::Logger logger_ = logging::getLogger("concord.util.handoff");
    // keep for debugging
    // logger_.setLogLevel(log4cplus::TRACE_LOG_LEVEL);
    return logger_;
  }

 protected:
  std::queue<tuple_t> task_queue_;
  mutable std::mutex queue_lock_;
  std::condition_variable queue_cond_;
  std::atomic_bool stopped_{false};
  std::thread thread_;
  std::atomic_uint64_t element_id_counter_;
};  // class Handoff

}  // namespace concord::util
