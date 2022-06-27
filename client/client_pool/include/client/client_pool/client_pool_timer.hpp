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
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <chrono>
#include <future>

#include <asio/steady_timer.hpp>
#include <asio/io_context.hpp>

#include "Logger.hpp"

namespace concord_client_pool {

/*
 * Timer type used by Concord's Client Pool implementation for managing batching timeouts.
 *
 * Note oddity: ClientT has to implement `operator<<` (for logging purposes)
 */
template <typename ClientT>
class Timer {
 public:
  using Clock = std::chrono::high_resolution_clock;

  /*
   * Constructor for a Timer<ClientT> to (once started) asynchronously call on_timeout after timeout milliseconds. A
   * value of 0 milliseconds for timeout is interpreted as meaning there is no timeout, and in that case, starting the
   * constructed Timer will never cause it to call on_timeout. Any behavior of the constructed Timer should be
   * considered undefined if timeout is a negative number of milliseconds.
   */
  Timer(std::chrono::milliseconds timeout, std::function<void(ClientT&&)> on_timeout)
      : timeout_{timeout},
        on_timeout_{on_timeout},
        timer_(io_context_),
        logger_{logging::getLogger("concord.client.client_pool.timer")} {
    if (timeout_.count() > 0) {
      timer_thread_future_ = std::async(std::launch::async, &Timer::work, this);
    }
  }

  /*
   * Destructor for Timer<ClientT>. This Timer will not make any calls to its on_timeout function after its destructor
   * has returned, however, if there is an ongoing timeout when this destructor begins, this Timer may call the
   * on_timeout function concurrently with this destructor.
   */
  ~Timer() { stopTimerThread(); }

  /*
   * Stop and end any asynchronous thread this Timer has for managing timeouts and callbacks. This puts this Timer
   * object into an inactive "done" state (this transition cannot be reversed). Once this funciton has returned, this
   * Timer will not make any calls to its on_timeout function, however, if there is an ongoing timeout when
   * stopTimerThread begins, this Timer may call its on_timeout function concurrently with stopTimerThread.
   */
  void stopTimerThread() {
    if (timeout_.count() == 0) {
      return;
    }
    io_context_.stop();
    timer_thread_future_.wait();
  }

  /*
   * Start the timer for a timeout, and make an asynchronous call to the on_timeout function (given at this Timer's
   * construction) with client as a parameter after the timeout duration (also given at this Timer's construction).
   * Note that, though this Timer's implementation should make a reasonable effort to make this callback after about
   * the given timeout duration, no hard guarantees about the precise timing of this callback are provided. No callback
   * will be made if this Timer was constructed with a timeout of 0 milliseconds or if stopTimerThread has previously
   * been called for this Timer. Behavior is undefined if start is called at any time when there is an ongoing timeout
   * from a previous call to start that has not either completed its callback or been successfully cancelled via
   * cancel() or stopTimerThread().
   */
  void start(const ClientT& client) {
    if (timeout_.count() == 0 || not timer_thread_future_.valid() || io_context_.stopped()) {
      LOG_WARN(logger_, "Timer cannot start for client " << client_);
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
    LOG_DEBUG(logger_, "Timer set for client " << client_);
  }

  /*
   * Attempt to cancel any ongoing timeout. If there is such an ongoing timeout and it is cancelled successfully, the
   * call to this Timer's on_timeout function scheduled by the most recent call to start will not occur. If there is
   * such an ongoing timeout and it could not be cancelled successfully, that call will still occur. Returns the
   * approximate duration, in milliseconds, between the most recent time start begin timing a timeout and the time this
   * call to cancel returns (this duration is guaranteed to be non-negative, but no other particular guarantees of its
   * accuracy or precision are given). Instead returns a duration of 0 milliseconds if this Timer was constructed with
   * a timeout duration of 0 milliseconds. Behavior is undefined if no calls to start have been made for this Timer
   * prior to this call to cancel.
   */
  std::chrono::milliseconds cancel() {
    if (timeout_.count() == 0) {
      return timeout_;
    }
    timer_.cancel();
    LOG_DEBUG(logger_, "Timer cancelled for client " << client_);
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
  logging::Logger logger_;
};
}  // namespace concord_client_pool
