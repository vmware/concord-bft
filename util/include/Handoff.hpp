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
        MDC_PUT(GL, "rid", std::to_string(replicaId));
        for (;;) pop()();
      } catch (ThreadCanceledException& e) {
        LOG_DEBUG(getLogger(), "thread stopped " << std::this_thread::get_id());
      } catch (const std::exception& e) {
        LOG_ERROR(getLogger(), "exception: " << e.what());
        // TODO [TK] should we allow different behavior for exception handling?
      }
    });
  }
  ~Handoff() {
    stopped_ = true;
    queue_cond_.notify_one();
    auto tid = thread_.get_id();
    thread_.join();
    LOG_DEBUG(getLogger(), "thread joined " << tid);
  }

  void push(func_type f) {
    {
      guard g(queue_lock_);
      task_queue_.push(f);
    }
    queue_cond_.notify_one();
  }

 protected:
  class ThreadCanceledException : public std::runtime_error {
   public:
    ThreadCanceledException() : std::runtime_error("thread cancelled") {}
    const char* what() const noexcept override { return std::runtime_error::what(); }
  };

  func_type pop() {
    while (true) {
      std::unique_lock<std::mutex> ul(queue_lock_);
      queue_cond_.wait(ul, [this] { return !(task_queue_.empty() && !stopped_); });
      LOG_TRACE(getLogger(), "notified stopped_: " << stopped_ << " queue size: " << task_queue_.size());

      if (!stopped_ || (stopped_ && !task_queue_.empty())) {
        func_type f = task_queue_.front();
        task_queue_.pop();
        return f;
      }
      throw ThreadCanceledException();
    }
  }

  static concordlogger::Logger getLogger() {
    static concordlogger::Logger logger_ = concordlogger::Log::getLogger("concord.util.handoff");
    return logger_;
  }

 protected:
  std::queue<func_type> task_queue_;
  std::mutex queue_lock_;
  std::condition_variable queue_cond_;
  std::atomic_bool stopped_;
  std::thread thread_;
};

}  // namespace concord::util
