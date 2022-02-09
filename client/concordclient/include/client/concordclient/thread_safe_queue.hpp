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
#include <list>
#include <memory>
#include <mutex>
#include <exception>

#include "assertUtils.hpp"

namespace concord::client::concordclient {

// An implementation of IQueue should guarantee that ReleaseConsumers, Clear, Push, Pop, and TryPop are all
// mutually thread safe.
template <typename T>
class IQueue {
 public:
  // Destructor for IQueue (which IQueue implementations should override). Note the
  // IQueue interface does NOT require that implementations guarantee the destructor be thread safe. the
  // IQueue functions' behavior may be undefined if any other function of an IQueue executes at
  // all concurrently with that queue's destructor after that destructor has begun executing, or at any point after the
  // destructor has begun running (including after the destructor has completed). Furthermore, behavior is undefined if
  // any thread is still waiting on the blocking IQueue::Pop call when the destructor begins running. Code
  // owning IQueue instances should guarantee that there are no outstanding calls to the IQueue's
  // functions and that no thread will start new ones before destroying an instance. Note the ReleaseConsumers function
  // can be used to have the queue unblock and release any threads still waiting on IQueue::Pop calls.
  virtual ~IQueue() {}

  // Release any threads currently waiting on blocking IQueue::Pop calls made to this IQueue,
  // making those calls return pointers to null; making this call also puts the IQueue into a state where any
  // new calls to the IQueue::Pop function will return a pointer to null instead of waiting on new updates to
  // become available. This function may block the caller to obtain locks if that is necessary for this operation under
  // the IQueue's implementation.
  virtual void releaseConsumers() = 0;

  // Synchronously clear all current updates in the queue. May block the calling thread as necessary to obtain any
  // lock(s) needed for this operation.
  virtual void clear() = 0;

  // Synchronously push a new update to the back of the queue. May block the calling thread as necessary to obtain any
  // lock(s) needed for this operation. May throw an exception if space cannot be found or allocated in the queue for
  // the update being pushed. Note the update is passed by unique_ptr, as this operation gives ownership of the
  // allocated update to the queue. IQueue implementations may choose whether they keep this allocated Update
  // or free it after storing the data from the update by some other means.
  virtual void push(std::unique_ptr<T> update) = 0;

  // Synchronously pop and return the update at the front of the queue. Normally, if there are no updates available in
  // the queue, this function should block the calling thread and wait until an update that can be popped is available.
  // This function may also block the calling thread for purposes of obtaining any lock(s) needed for these operations.
  // This function gives ownership of an allocated Update to the caller; implementations of IQueue may choose
  // whether they give ownership of an allocated Update they own to the caller or whether they dynamically allocate
  // memory (via malloc/new/std::allocator(s)) to construct the Update object from data the Queue has stored by some
  // other means. If ReleaseConsumers is called, any currently waiting Pop calls will be unblocked, and will return a
  // unique_ptr to null rather than continuing to wait for new updates. Furthermore, a call to ReleaseConsumers will
  // cause any subsequent calls to Pop to return nullptr and will prevent them from blocking their caller.
  virtual std::unique_ptr<T> pop() = 0;

  // Synchronously pop an update from the front of the queue if one is available, but do not block the calling thread to
  // wait on one if one is not immediately found. Returns a unique_ptr to nullptr if no update is immediately found, and
  // a unique_ptr giving ownership of an allocated Update otherwise (this may involve dynamic memory allocation at the
  // discretion of the IQueue implementation).
  virtual std::unique_ptr<T> tryPop() = 0;

  virtual uint64_t size() = 0;

  virtual void setException(std::exception_ptr e) = 0;
};

// Basic IQueue implementation provided by this library. This class can be expected to adhere to the
// IQueue interface, but details of this class's implementation should be considered subject to change as we
// may revise it as we implement, harden, and test the Thin Replica Client Library and develop a more complete
// understanding of the needs of this library.
template <typename T>
class BasicThreadSafeQueue : public IQueue<T> {
 private:
  std::list<std::unique_ptr<T>> queue_data_;
  std::mutex mutex_;
  std::condition_variable condition_;
  bool release_consumers_;
  std::exception_ptr exception_;

 public:
  // Construct a BasicThreadSafeQueue.
  BasicThreadSafeQueue();

  // Copying or moving a BasicThreadSafeQueue is explicitly disallowed, as we do not know of a compelling use case
  // requiring copying or moving BasicThreadSafeQueue, we believe semantics for these operations are likely to be
  // messy in some cases, and we believe implementation may be non-trivial. We may revisit the decision to disallow
  // these operations should compelling use cases for them be found in the future.
  BasicThreadSafeQueue(const BasicThreadSafeQueue& other) = delete;
  BasicThreadSafeQueue(const BasicThreadSafeQueue&& other) = delete;
  BasicThreadSafeQueue& operator=(const BasicThreadSafeQueue& other) = delete;
  BasicThreadSafeQueue& operator=(const BasicThreadSafeQueue&& other) = delete;

  // Implementation of ThreadSafeQueue interface
  virtual ~BasicThreadSafeQueue() override;
  virtual void releaseConsumers() override;
  virtual void clear() override;
  virtual void push(std::unique_ptr<T> update) override;
  virtual std::unique_ptr<T> pop() override;
  virtual std::unique_ptr<T> tryPop() override;
  virtual uint64_t size() override;
  virtual void setException(std::exception_ptr e) override;
};

}  // namespace concord::client::concordclient

#include "thread_safe_queue.ipp"
