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
class Runner {
 public:
  Runner(const TlsTcpConfig& config, const size_t num_threads);
  void start();
  void stop();
  bool isRunning() const;
  ConnectionStatus getCurrentConnectionStatus(const NodeNum nodeNum) {
    return connectionManager_.getCurrentConnectionStatus(nodeNum);
  }
  int getMaxMessageSize() { return connectionManager_.getMaxMessageSize(); }
  void setReceiver(NodeNum id, IReceiver* receiver) { connectionManager_.setReceiver(id, receiver); }
  void send(NodeNum destNode, std::shared_ptr<tls::OutgoingMsg> msg) { connectionManager_.send(destNode, msg); }
  void send(std::set<NodeNum> dests, std::shared_ptr<tls::OutgoingMsg> msg) { connectionManager_.send(dests, msg); }

 private:
  logging::Logger logger_;
  size_t num_threads_;
  // Protects io_threads_ whose emptiness is used as the condition of whether a thread is started or stopped.
  mutable std::mutex start_stop_mutex_;
  // A pool of threads from which completion handlers may be invoked.
  std::vector<std::thread> io_threads_;
  asio::io_context io_context_;
  ConnectionManager connectionManager_;
};

}  // namespace bft::communication::tls
