// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "client/concordclient/event_update_queue.hpp"

#include "assertUtils.hpp"

using std::lock_guard;
using std::mutex;
using std::unique_ptr;
using std::unique_lock;

namespace concord::client::concordclient {

BasicUpdateQueue::BasicUpdateQueue() : queue_data_(), mutex_(), condition_(), release_consumers_(false) {}

BasicUpdateQueue::~BasicUpdateQueue() {}

void BasicUpdateQueue::releaseConsumers() {
  {
    lock_guard<mutex> lock(mutex_);
    release_consumers_ = true;
  }
  condition_.notify_all();
}

void BasicUpdateQueue::clear() {
  lock_guard<mutex> lock(mutex_);
  queue_data_.clear();
}

void BasicUpdateQueue::push(unique_ptr<EventVariant> update) {
  {
    lock_guard<mutex> lock(mutex_);
    queue_data_.push_back(move(update));
  }
  condition_.notify_one();
}

unique_ptr<EventVariant> BasicUpdateQueue::pop() {
  unique_lock<mutex> lock(mutex_);
  while (!(release_consumers_ || (queue_data_.size() > 0))) {
    condition_.wait(lock);
  }
  if (release_consumers_) {
    return unique_ptr<EventVariant>(nullptr);
  }
  ConcordAssert(queue_data_.size() > 0);
  unique_ptr<EventVariant> ret = move(queue_data_.front());
  queue_data_.pop_front();
  return ret;
}

unique_ptr<EventVariant> BasicUpdateQueue::tryPop() {
  lock_guard<mutex> lock(mutex_);
  if (queue_data_.size() > 0) {
    unique_ptr<EventVariant> ret = move(queue_data_.front());
    queue_data_.pop_front();
    return ret;
  } else {
    return unique_ptr<EventVariant>(nullptr);
  }
}

uint64_t BasicUpdateQueue::size() {
  std::scoped_lock sl(mutex_);
  return queue_data_.size();
}

}  // namespace concord::client::concordclient
