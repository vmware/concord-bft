// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <condition_variable>
#include <boost/lockfree/spsc_queue.hpp>
#include <list>
#include <memory>
#include <thread>
#include <mutex>
#include <exception>
#include <chrono>
#include <variant>

#include "assertUtils.hpp"
#include "event_update.hpp"

namespace concord::client::concordclient {

typedef std::variant<Update, EventGroup> EventVariant;

class TrcQueue {
 private:
  boost::lockfree::spsc_queue<EventVariant*> queue_data_;
  std::exception_ptr exception_;
  std::mutex mutex_;
  std::condition_variable condition_;
  static constexpr size_t kQueueDataSize{10000u};

 public:
  // Construct a TrcQueue.
  TrcQueue() : queue_data_(kQueueDataSize) {}

  // Copying or moving a TrcQueue is explicitly disallowed, as we do not know of a compelling use case
  // requiring copying or moving TrcQueue, we believe semantics for these operations are likely to be
  // messy in some cases, and we believe implementation may be non-trivial. We may revisit the decision to disallow
  // these operations should compelling use cases for them be found in the future.
  TrcQueue(const TrcQueue& other) = delete;
  TrcQueue(const TrcQueue&& other) = delete;
  TrcQueue& operator=(const TrcQueue& other) = delete;
  TrcQueue& operator=(const TrcQueue&& other) = delete;

  void clear() { queue_data_.reset(); }

  void push(EventVariant* update) {
    queue_data_.push(std::move(update));
    condition_.notify_one();
  }

  std::unique_ptr<EventVariant> popTill(std::chrono::milliseconds timeout) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      auto is_timeout = !condition_.wait_for(
          lock, timeout, [this]() { return this->exception_ || (queue_data_.read_available() > 0); });
      if (is_timeout) return nullptr;
    }
    if (exception_) {
      auto e = exception_;
      exception_ = nullptr;
      std::rethrow_exception(e);
    }
    ConcordAssertGT(queue_data_.read_available(), 0);
    auto update = std::move(queue_data_.front());
    queue_data_.pop();
    return std::unique_ptr<EventVariant>(update);
  }

  std::unique_ptr<EventVariant> pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!exception_ && !(queue_data_.read_available() > 0)) {
      condition_.wait(lock);
    }
    if (exception_) {
      auto e = exception_;
      exception_ = nullptr;
      std::rethrow_exception(e);
    }
    ConcordAssertGT(queue_data_.read_available(), 0);
    auto update = std::move(queue_data_.front());
    queue_data_.pop();
    return std::unique_ptr<EventVariant>(update);
  }

  std::unique_ptr<EventVariant> tryPop() {
    if (exception_) {
      auto e = exception_;
      exception_ = nullptr;
      std::rethrow_exception(e);
    }
    if (queue_data_.read_available() > 0) {
      auto update = std::move(queue_data_.front());
      queue_data_.pop();
      return std::unique_ptr<EventVariant>(update);
    } else {
      return nullptr;
    }
  }

  uint64_t size() { return queue_data_.read_available(); }

  void setException(std::exception_ptr e) { exception_ = e; }
};

}  // namespace concord::client::concordclient
