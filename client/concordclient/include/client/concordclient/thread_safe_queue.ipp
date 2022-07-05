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

namespace concord::client::concordclient {

template <typename T>
BasicThreadSafeQueue<T>::BasicThreadSafeQueue() : queue_data_(), mutex_(), condition_(), release_consumers_(false) {}

template <typename T>
BasicThreadSafeQueue<T>::~BasicThreadSafeQueue() {}

template <typename T>
void BasicThreadSafeQueue<T>::releaseConsumers() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    release_consumers_ = true;
  }
  condition_.notify_all();
}

template <typename T>
void BasicThreadSafeQueue<T>::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  queue_data_.clear();
}

template <typename T>
void BasicThreadSafeQueue<T>::push(std::unique_ptr<T> update) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_data_.push_back(move(update));
  }
  condition_.notify_one();
}

template <typename T>
std::unique_ptr<T> BasicThreadSafeQueue<T>::pop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!(exception_ || release_consumers_ || (queue_data_.size() > 0))) {
    condition_.wait(lock);
  }
  if (exception_) {
    auto e = exception_;
    exception_ = nullptr;
    std::rethrow_exception(e);
  }
  if (release_consumers_) {
    return std::unique_ptr<T>(nullptr);
  }
  ConcordAssertGT(queue_data_.size(), 0);
  std::unique_ptr<T> ret = move(queue_data_.front());
  queue_data_.pop_front();
  return ret;
}

template <typename T>
std::unique_ptr<T> BasicThreadSafeQueue<T>::popTill(std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mutex_);

  auto is_timeout = !condition_.wait_for(lock, timeout, [this]() {
    return this->exception_ || this->release_consumers_ || ((this->queue_data_).size() > 0);
  });
  if (is_timeout) {
    return std::unique_ptr<T>(nullptr);
  }
  if (exception_) {
    auto e = exception_;
    exception_ = nullptr;
    std::rethrow_exception(e);
  }
  if (release_consumers_) {
    return std::unique_ptr<T>(nullptr);
  }
  ConcordAssertGT(queue_data_.size(), 0);
  std::unique_ptr<T> ret = move(queue_data_.front());
  queue_data_.pop_front();
  return ret;
}

template <typename T>
std::unique_ptr<T> BasicThreadSafeQueue<T>::tryPop() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (exception_) {
    auto e = exception_;
    exception_ = nullptr;
    std::rethrow_exception(e);
  }
  if (queue_data_.size() > 0) {
    std::unique_ptr<T> ret = move(queue_data_.front());
    queue_data_.pop_front();
    return ret;
  } else {
    return std::unique_ptr<T>(nullptr);
  }
}

template <typename T>
uint64_t BasicThreadSafeQueue<T>::size() {
  std::scoped_lock sl(mutex_);
  return queue_data_.size();
}

template <typename T>
void BasicThreadSafeQueue<T>::setException(std::exception_ptr e) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    exception_ = e;
  }
  condition_.notify_all();
}

}  // namespace concord::client::concordclient