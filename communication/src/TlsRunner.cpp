// Concord
//
// Copyright (c) 2021-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use
// this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license
// terms. Your use of these subcomponents is subject to the terms and conditions of the
// subcomponent's license, as noted in the LICENSE file.

#include "assertUtils.hpp"
#include "TlsRunner.h"

namespace bft::communication::tls {

Runner::Runner(const TlsTcpConfig& config, const size_t num_threads)
    : logger_(logging::getLogger("concord-bft.tls.runner")),
      num_threads_(num_threads),
      connectionManager_(config, io_context_) {}

bool Runner::isRunning() const {
  std::lock_guard<std::mutex> lock(start_stop_mutex_);
  return !io_threads_.empty();
}

void Runner::start() {
  std::lock_guard<std::mutex> lock(start_stop_mutex_);
  ConcordAssert(io_threads_.empty());
  LOG_INFO(logger_, "Starting TLS Runner");
  io_context_.restart();

  // Give the io_context work to do.
  connectionManager_.start();

  // Run the io_context in the thread pool
  for (std::size_t i = 0; i < num_threads_; i++) {
    io_threads_.emplace_back([this]() { io_context_.run(); });
  }
}

void Runner::stop() {
  std::lock_guard<std::mutex> lock(start_stop_mutex_);
  ConcordAssert(!io_threads_.empty());

  // We want to stop all the thread from processing data before we clean up the connection managers.
  io_context_.stop();
  for (auto& t : io_threads_) {
    t.join();
  }
  io_threads_.clear();

  // Synchronously close the socket and cleanup state in the connection manager.
  connectionManager_.stop();
}

}  // namespace bft::communication::tls
