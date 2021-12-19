// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use
// this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license
// terms. Your use of these subcomponents is subject to the terms and conditions of the
// subcomponent's license, as noted in the LICENSE file.

#include <asio.hpp>
#include <thread>

#include "communication/CommDefs.hpp"
#include "Logger.hpp"
#include "TlsConnectionManager.h"

#pragma once

namespace bft::communication::tls {

// The runner creates an `asio::io_context` and a pool of threads to serve that context.
//
// In order to allow multiple principals to share the same asio::io_context, such that we can
// cheaply use multiple clients without an extra thread pool, we create a `ConnMgr` for each
// principal and share a reference to the io_context among them.
class Runner {
 public:
  Runner(const std::vector<TlsTcpConfig>&, const size_t num_threads);
  void start();
  void stop();
  bool isRunning() const;
  std::map<NodeNum, ConnectionManager>& principals() { return principals_; }

 private:
  logging::Logger logger_;
  size_t num_threads_;

  // Protects io_threads_ whose emptiness is used as the condition of whether a thread is started or stopped.
  mutable std::mutex start_stop_mutex_;
  // A pool of threads from which completion handlers may be invoked.
  std::vector<std::thread> io_threads_;

  asio::io_context io_context_;

  std::map<NodeNum, ConnectionManager> principals_;
};

}  // namespace bft::communication::tls
