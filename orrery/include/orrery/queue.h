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

#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>

#include "orrery_msgs.cmf.hpp"

namespace concord::orrery::detail {

// This is the shared queue implementation for all mailboxes.
//
// We need the queue to be unbounded to easily prevent distributed deadlocks.
// We don't need to worry about bounds because we limit concurrency at the network input layer.
//
// Locks are fine performance wise unless measuremnt shows otherwise.
class Queue {
 public:
  void push(Envelope&&);

  // Blocks indefinitely until an Envelope is available.
  //
  // This is exception unsafe if we change the underlying container to one where `pop` can throw.
  // When using the default deque, this is not possible, since the underlying pop method is only
  // called on non-empty queues, and pop does not throw in that case.
  Envelope pop();

  // Blocks until an Envelope is available or a timeout.
  //
  // Returns std::nullopt on timeout.
  //
  // This is exception unsafe if we change the underlying container to one where `pop` can throw.
  // When using the default deque, this is not possible, since the underlying pop method is only
  // called on non-empty queues, and pop does not throw in that case.
  std::optional<Envelope> pop(std::chrono::milliseconds timeout);

 private:
  std::queue<Envelope> queue_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
};

}  // namespace concord::orrery::detail
