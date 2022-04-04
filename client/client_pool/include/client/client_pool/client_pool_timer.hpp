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
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <chrono>
#include <future>

#include <asio/steady_timer.hpp>
#include <asio/io_context.hpp>

namespace concord_client_pool {
template <typename ClientT>
class Timer {
 public:
  using Clock = std::chrono::high_resolution_clock;
  Timer(std::chrono::milliseconds timeout, std::function<void(ClientT&&)> on_timeout)
      : timeout_{timeout}, on_timeout_{on_timeout}, timer_(io_context_) {
    if (timeout_.count() > 0) {
      timer_thread_future_ = std::async(std::launch::async, &Timer::work, this);
    }
  }

  ~Timer() { stopTimerThread(); }

  void stopTimerThread() {
    if (timeout_.count() == 0) {
      return;
    }
    io_context_.stop();
    timer_thread_future_.wait();
  }

  void start(const ClientT& client) {
    if (timeout_.count() == 0 || not timer_thread_future_.valid() || io_context_.stopped()) {
      return;
    }
    client_ = client;
    auto handler = [this](const asio::error_code& error) {
      if (error != asio::error::operation_aborted) {
        on_timeout_(std::move(client_));
      }
    };

    start_timer_ = std::chrono::steady_clock::now();
    timer_.expires_at(start_timer_ + timeout_);
    timer_.async_wait(handler);
  }

  std::chrono::milliseconds cancel() {
    if (timeout_.count() == 0) {
      return timeout_;
    }
    timer_.cancel();
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_timer_);
  }

 private:
  void work() {
    // Add a work guard so that io_context_.run() keeps running even if there is no work item
    asio::executor_work_guard<asio::io_context::executor_type> work_guard(io_context_.get_executor());
    io_context_.run();
  }

  ClientT client_;

  std::future<void> timer_thread_future_;

  const std::chrono::milliseconds timeout_;
  std::function<void(ClientT&&)> on_timeout_;
  asio::io_context io_context_;
  asio::steady_timer timer_;
  std::chrono::steady_clock::time_point start_timer_;
};
}  // namespace concord_client_pool
