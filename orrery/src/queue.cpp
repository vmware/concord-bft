// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "orrery/queue.h"
#include <mutex>

namespace concord::orrery::detail {

void Queue::push(Envelope&& envelope) {
  std::lock_guard<std::mutex> lock(mutex_);
  queue_.push(std::move(envelope));
}

Envelope Queue::pop() {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this]() { return !queue_.empty(); });
  auto envelope = std::move(queue_.front());
  queue_.pop();
  return envelope;
}

std::optional<Envelope> Queue::pop(std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!cv_.wait_for(lock, timeout, [this]() { return !queue_.empty(); })) {
    // Timeout
    return std::nullopt;
  }
  auto envelope = std::move(queue_.front());
  queue_.pop();
  return envelope;
}

}  // namespace concord::orrery::detail
