// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <utility>

#include "channel.h"

namespace concord::channel {

// Forward declarations
template <typename Msg>
class BoundedMpscSender;
template <typename Msg>
class BoundedMpscReceiver;

namespace impl {

// A simple, non-optimized, lock based, bounded MPSC channel.
//
// This is an internal type used by the BoundedMpscSender and BoundedMpscReceiver and should not be exposed publicly.
template <typename Msg>
class BoundedMpscQueue {
 public:
  BoundedMpscQueue(size_t capacity) : capacity_(capacity) {}

 private:
  template <typename T>
  friend class ::concord::channel::BoundedMpscSender;
  template <typename T>
  friend class ::concord::channel::BoundedMpscReceiver;

  std::optional<size_t> size() {
    const std::lock_guard<std::mutex> lock(mutex_);
    return buf_.size();
  }

  std::optional<size_t> capacity() {
    // No need for a lock, as capacity_ is immutable
    return capacity_;
  }

  void addSender() {
    const std::lock_guard<std::mutex> lock(mutex_);
    num_senders_++;
  }

  void removeSender() {
    bool num_senders = 0;
    {
      const std::lock_guard<std::mutex> lock(mutex_);
      num_senders_--;
      num_senders = num_senders_;
    }
    if (num_senders == 0) {
      // Signal the receiver to throw a NoSendersError.
      cv_.notify_one();
    }
  }

  void invalidateReceiver() {
    const std::lock_guard<std::mutex> lock(mutex_);
    valid_receiver_ = false;
  }

  const size_t capacity_;
  std::queue<Msg> buf_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  size_t num_senders_ = 0;
  bool valid_receiver_ = true;
};

}  // namespace impl

template <typename Msg>
class BoundedMpscSender {
 public:
  // Try to send a message over a bounded channel.
  //
  // Return std::nullopt on sucess.
  // Return the Msg if the buffer is full
  //
  // Throws NoReceiversError if the receiver has been destructed.
  //
  // It's important to note that there is a race condition, whereby the receiver can be destructed
  // immediately after a successful send. There is no absolute guarantee of delivery. The
  // purpose of returning an error is to prevent endlessly retrying and never noticing that the
  // channel is effectively destroyed.
  std::optional<Msg> send(Msg&& msg) {
    {
      std::lock_guard<std::mutex> lock(queue_->mutex_);
      if (!queue_->valid_receiver_) {
        throw NoReceiversError();
      }
      if (queue_->buf_.size() == queue_->capacity_) {
        return msg;
      }
      queue_->buf_.push(std::move(msg));
    }
    queue_->cv_.notify_one();

    return std::nullopt;
  }

  std::optional<size_t> size() { return queue_->size(); }
  std::optional<size_t> capacity() { return queue_->capacity(); }

  ~BoundedMpscSender() {
    if (!queue_) return;
    queue_->removeSender();
  }

  BoundedMpscSender(const BoundedMpscSender& other) : BoundedMpscSender(other.queue_) {}
  BoundedMpscSender& operator=(const BoundedMpscSender& other) {
    *this = BoundedMpscSender(other);
    return *this;
  }
  BoundedMpscSender(BoundedMpscSender&&) noexcept = default;
  BoundedMpscSender& operator=(BoundedMpscSender&&) noexcept = default;

 private:
  BoundedMpscSender(const std::shared_ptr<impl::BoundedMpscQueue<Msg>>& queue) : queue_(queue) { queue_->addSender(); }

  std::shared_ptr<impl::BoundedMpscQueue<Msg>> queue_;

  template <typename T>
  friend std::pair<BoundedMpscSender<T>, BoundedMpscReceiver<T>> makeBoundedMpscChannel(size_t capacity);
};  // namespace concord::channel

template <typename Msg>
class BoundedMpscReceiver {
 public:
  // Receive a message over a channel.
  // Block waiting indefinitely.
  //
  // Throws NoSendersError if all senders have been destructed and there are no more messages left
  // in the communication buffer.
  Msg recv() {
    std::unique_lock<std::mutex> lock(queue_->mutex_);
    queue_->cv_.wait(lock, [this]() { return waitPred(); });

    // Always return a value if it's available, even if there are no senders.
    if (!queue_->buf_.empty()) {
      return pop();
    }
    throw NoSendersError();
  }

  // Receive a message over a channel and return it.
  // Return std::nullopt on timeout.
  //
  // Throws NoSendersError if all senders have been destructed and there are no more messages left
  // in the communication buffer.
  std::optional<Msg> recv(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(queue_->mutex_);
    if (!queue_->cv_.wait_for(lock, timeout, [this]() { return waitPred(); })) {
      // Timeout
      return std::nullopt;
    }

    // Always return a value if it's available, even if there are no senders.
    if (!queue_->buf_.empty()) {
      return pop();
    }
    throw NoSendersError();
  }

  std::optional<size_t> size() { return queue_->size(); }
  std::optional<size_t> capacity() { return queue_->capacity(); }

  ~BoundedMpscReceiver() {
    if (queue_) {
      queue_->invalidateReceiver();
    }
  }

  // You can't copy an MPSC receiver
  BoundedMpscReceiver(const BoundedMpscReceiver&) = delete;
  BoundedMpscReceiver& operator=(const BoundedMpscReceiver&) = delete;

  // We do allow moving a receiver though
  BoundedMpscReceiver(BoundedMpscReceiver&&) noexcept = default;
  BoundedMpscReceiver& operator=(BoundedMpscReceiver&&) noexcept = default;

 private:
  BoundedMpscReceiver(const std::shared_ptr<impl::BoundedMpscQueue<Msg>>& queue) : queue_(queue) {}

  Msg pop() {
    auto msg = std::move(queue_->buf_.front());
    queue_->buf_.pop();
    return msg;
  }

  bool waitPred() { return queue_->num_senders_ == 0 || !queue_->buf_.empty(); }

  std::shared_ptr<impl::BoundedMpscQueue<Msg>> queue_;

  template <typename T>
  friend std::pair<BoundedMpscSender<T>, BoundedMpscReceiver<T>> makeBoundedMpscChannel(size_t capacity);
};

template <typename Msg>
std::pair<BoundedMpscSender<Msg>, BoundedMpscReceiver<Msg>> makeBoundedMpscChannel(size_t capacity) {
  auto queue = std::make_shared<impl::BoundedMpscQueue<Msg>>(capacity);
  auto pair = std::make_pair(BoundedMpscSender<Msg>(queue), BoundedMpscReceiver<Msg>(queue));
  return pair;
}

}  // namespace concord::channel
