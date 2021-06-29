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

namespace concord::util {
/**
 * Simple handing off to a dedicated thread functionality.
 * Implemented by passing a function with a trivial signature void(void).
 * Parameters are bound to an arbitrary function by a caller.
 */
class Handoff {
  typedef std::lock_guard<std::mutex> guard;
  typedef std::function<void()> func_type;

 public:
  Handoff(std::uint16_t replicaId) {
    thread_ = std::thread([this, replicaId] {
      try {
        MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(replicaId));
        MDC_PUT(MDC_THREAD_KEY, "handoff");
        for (;;) pop()();
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

  void push(func_type f) {
    {
      guard g(queue_lock_);
      task_queue_.push(std::move(f));
      task_queue_size_ = task_queue_.size();
    }
    queue_cond_.notify_one();
  }

  size_t size() { return task_queue_size_; }

 protected:
  class ThreadCanceledException : public std::runtime_error {
   public:
    ThreadCanceledException() : std::runtime_error("thread cancelled") {}
    const char* what() const noexcept override { return std::runtime_error::what(); }
  };

  func_type pop() {
    while (true) {
      std::unique_lock<std::mutex> ul(queue_lock_);
      auto pred = [this] { return !task_queue_.empty() || stopped_; };

      if (!pred()) {
        queue_cond_.wait(ul, pred);
      }
      task_queue_size_ = task_queue_.size();

      if (!stopped_ || (stopped_ && !task_queue_.empty())) {
        func_type f = task_queue_.front();
        task_queue_.pop();
        return f;
      }
      LOG_INFO(getLogger(), "Handoff thread stopped!");
      throw ThreadCanceledException();
    }
  }

  static logging::Logger getLogger() {
    static logging::Logger logger_ = logging::getLogger("concord.util.handoff");
    return logger_;
  }

 protected:
  std::queue<func_type> task_queue_;
  std::mutex queue_lock_;
  std::condition_variable queue_cond_;
  std::atomic_bool stopped_{false};
  std::thread thread_;
  std::atomic_size_t task_queue_size_;
};  // class Handoff

}  // namespace concord::util
