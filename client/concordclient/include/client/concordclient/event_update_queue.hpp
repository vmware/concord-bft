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

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

#include "client/concordclient/event_update.hpp"

namespace concord::client::concordclient {

// An implementation of UpdateQueue should guarantee that ReleaseConsumers, Clear, Push, Pop, and TryPop are all
// mutually thread safe.
class UpdateQueue {
 public:
  // Destructor for UpdateQueue (which UpdateQueue implementations should override). Note the UpdateQueue interface does
  // NOT require that implementations guarantee the destructor be thread safe. the UpdateQueue functions' behavior may
  // be undefined if any other function of an UpdateQueue executes at all concurrently with that queue's destructor
  // after that destructor has begun executing, or at any point after the destructor has begun running (including after
  // the destructor has completed). Furthermore, behavior is undefined if any thread is still waiting on the blocking
  // UpdateQueue::Pop call when the destructor begins running. Code owning UpdateQueue instances should guarantee that
  // there are no outstanding calls to the UpdateQueue's functions and that no thread will start new ones before
  // destroying an instance. Note the ReleaseConsumers function can be used to have the queue unblock and release any
  // threads still waiting on UpdateQueue::Pop calls.
  virtual ~UpdateQueue() {}

  // Release any threads currently waiting on blocking UpdateQueue::Pop calls made to this UpdateQueue, making those
  // calls return pointers to null; making this call also puts the UpdateQueue into a state where any new calls to the
  // UpdateQueue::Pop function will return a pointer to null instead of waiting on new updates to become available. This
  // function may block the caller to obtain locks if that is necessary for this operation under the UpdateQueue's
  // implementation.
  virtual void releaseConsumers() = 0;

  // Synchronously clear all current updates in the queue. May block the calling thread as necessary to obtain any
  // lock(s) needed for this operation.
  virtual void clear() = 0;

  // Synchronously push a new update to the back of the queue. May block the calling thread as necessary to obtain any
  // lock(s) needed for this operation. May throw an exception if space cannot be found or allocated in the queue for
  // the update being pushed. Note the update is passed by unique_ptr, as this operation gives ownership of the
  // allocated update to the queue. UpdateQueue implementations may choose whether they keep this allocated Update or
  // free it after storing the data from the update by some other means.
  virtual void push(std::unique_ptr<EventVariant> update) = 0;

  // Synchronously pop and return the update at the front of the queue. Normally, if there are no updates available in
  // the queue, this function should block the calling thread and wait until an update that can be popped is available.
  // This function may also block the calling thread for purposes of obtaining any lock(s) needed for these operations.
  // This function gives ownership of an allocated Update to the caller; implementations of UpdateQueue may choose
  // whether they give ownership of an allocated Update they own to the caller or whether they dynamically allocate
  // memory (via malloc/new/std::allocator(s)) to construct the Update object from data the Queue has stored by some
  // other means. If ReleaseConsumers is called, any currently waiting Pop calls will be unblocked, and will return a
  // unique_ptr to null rather than continuing to wait for new updates. Furthermore, a call to ReleaseConsumers will
  // cause any subsequent calls to Pop to return nullptr and will prevent them from blocking their caller.
  virtual std::unique_ptr<EventVariant> pop() = 0;

  // Synchronously pop an update from the front of the queue if one is available, but do not block the calling thread to
  // wait on one if one is not immediately found. Returns a unique_ptr to nullptr if no update is immediately found, and
  // a unique_ptr giving ownership of an allocated Update otherwise (this may involve dynamic memory allocation at the
  // discretion of the UpdateQueue implementation).
  virtual std::unique_ptr<EventVariant> tryPop() = 0;

  virtual uint64_t size() = 0;
};

// Basic UpdateQueue implementation provided by this library. This class can be expected to adhere to the UpdateQueue
// interface, but details of this class's implementation should be considered subject to change as we may revise it as
// we implement, harden, and test the Thin Replica Client Library and develop a more complete understanding of the needs
// of this library.
class BasicUpdateQueue : public UpdateQueue {
 private:
  std::list<std::unique_ptr<EventVariant>> queue_data_;
  std::mutex mutex_;
  std::condition_variable condition_;
  bool release_consumers_;

 public:
  // Construct a BasicUpdateQueue.
  BasicUpdateQueue();

  // Copying or moving a BasicUpdateQueue is explicitly disallowed, as we do not know of a compelling use case requiring
  // copying or moving BasicUpdateQueues, we believe semantics for these operations are likely to be messy in some
  // cases, and we believe implementation may be non-trivial. We may revisit the decision to disallow these operations
  // should compelling use cases for them be found in the future.
  BasicUpdateQueue(const BasicUpdateQueue& other) = delete;
  BasicUpdateQueue(const BasicUpdateQueue&& other) = delete;
  BasicUpdateQueue& operator=(const BasicUpdateQueue& other) = delete;
  BasicUpdateQueue& operator=(const BasicUpdateQueue&& other) = delete;

  // Implementation of UpdateQueue interface
  virtual ~BasicUpdateQueue() override;
  virtual void releaseConsumers() override;
  virtual void clear() override;
  virtual void push(std::unique_ptr<EventVariant> update) override;
  virtual std::unique_ptr<EventVariant> pop() override;
  virtual std::unique_ptr<EventVariant> tryPop() override;
  virtual uint64_t size() override;
};

}  // namespace concord::client::concordclient
