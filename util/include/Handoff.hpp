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
  Handoff(std::uint16_t replicaId, const std::string&& name = "")
      : logger_{logging::getLogger("concord.util.handoff")} {
    if (!name.empty()) {
      name_ = std::move(name);
    }
    log_prefix_ = name_ + ": ";
    thread_ = std::thread([this, replicaId] {
      try {
        MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(replicaId));
        MDC_PUT(MDC_THREAD_KEY, "handoff");
        for (;;) pop()();
      } catch (ThreadCanceledException& e) {
        LOG_INFO(logger_, log_prefix_ << "thread cancelled " << std::this_thread::get_id());
      } catch (const std::exception& e) {
        LOG_FATAL(logger_, log_prefix_ << "exception: " << e.what());
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
    LOG_INFO(logger_, log_prefix_ << "thread joined " << tid);
  }

  void push(func_type f) {
    {
      guard g(queue_lock_);
      task_queue_.push(std::move(f));
    }
    queue_cond_.notify_one();
  }

  size_t size() const {
    guard g(queue_lock_);
    return task_queue_.size();
  }

  size_t empty() const {
    guard g(queue_lock_);
    return task_queue_.empty();
  }

 protected:
  class ThreadCanceledException : public std::runtime_error {
   public:
    ThreadCanceledException(const std::string& log_prefix) : std::runtime_error(log_prefix + "thread cancelled") {}
    const char* what() const noexcept override { return std::runtime_error::what(); }
  };

  func_type pop() {
    while (true) {
      std::unique_lock<std::mutex> ul(queue_lock_);
      queue_cond_.wait(ul, [this] { return !task_queue_.empty() || stopped_; });

      if (!stopped_ || (stopped_ && !task_queue_.empty())) {
        func_type f = task_queue_.front();
        task_queue_.pop();
        return f;
      }
      LOG_INFO(logger_, log_prefix_ << "Handoff thread stopped!");
      throw ThreadCanceledException(log_prefix_);
    }
  }

 protected:
  std::queue<func_type> task_queue_;
  mutable std::mutex queue_lock_;
  std::condition_variable queue_cond_;
  std::atomic_bool stopped_{false};
  std::thread thread_;
  std::string name_ = "handoff";
  std::string log_prefix_;
  logging::Logger logger_;
};  // class Handoff

}  // namespace concord::util
