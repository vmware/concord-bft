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
  logging::Logger logger_;

 public:
  // Construct a TrcQueue.
  TrcQueue(uint32_t queueSize) : queue_data_(queueSize), logger_(logging::getLogger("concord.client.trc_queue")) {}

  // Copying or moving a TrcQueue is explicitly disallowed, as we do not know of a compelling use case
  // requiring copying or moving TrcQueue, we believe semantics for these operations are likely to be
  // messy in some cases, and we believe implementation may be non-trivial. We may revisit the decision to disallow
  // these operations should compel use cases for them be found in the future.
  TrcQueue(const TrcQueue& other) = delete;
  TrcQueue(const TrcQueue&& other) = delete;
  TrcQueue& operator=(const TrcQueue& other) = delete;
  TrcQueue& operator=(const TrcQueue&& other) = delete;

  void clear() { queue_data_.reset(); }

  void push(EventVariant* update) {
    queue_data_.push(std::move(update));
    condition_.notify_one();
  }

  std::unique_ptr<EventVariant> getElementFromTheQueue() {
    if (queue_data_.read_available()) {
      auto update = std::move(queue_data_.front());
      queue_data_.pop();
      return std::make_unique<EventVariant>(std::move(*update));
    }
    if (exception_) {
      auto e = exception_;
      exception_ = nullptr;
      std::rethrow_exception(e);
    }
    return nullptr;
  }

  std::unique_ptr<EventVariant> popTill(std::chrono::milliseconds timeout) {
    auto update = getElementFromTheQueue();
    if (update) return update;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      auto is_timeout =
          !condition_.wait_for(lock, timeout, [this]() { return this->exception_ || queue_data_.read_available(); });
      if (is_timeout) {
        LOG_WARN(logger_, "Timeout getting element from the queue");
        return nullptr;
      }
    }
    return getElementFromTheQueue();
  }

  std::unique_ptr<EventVariant> pop() {
    auto update = getElementFromTheQueue();
    if (update) return update;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      while (!exception_ && !queue_data_.read_available()) {
        condition_.wait(lock);
      }
    }
    return getElementFromTheQueue();
  }

  // For the testing purpose only
  std::unique_ptr<EventVariant> tryPop() { return getElementFromTheQueue(); }

  uint64_t size() { return queue_data_.read_available(); }

  void setException(std::exception_ptr e) { exception_ = std::move(e); }
};

}  // namespace concord::client::concordclient
