// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "communication/CommDefs.hpp"
#include "TlsRunner.h"

// TODO: Make this configurable
static constexpr size_t NUM_THREADS = 1;

namespace bft::communication {

// This is the public interface to this library. TlsTcpCommunication implements ICommunication.
TlsTCPCommunication::TlsTCPCommunication(const TlsTcpConfig &config)
    : logger_(logging::getLogger("concord-bft.tls.comm")) {
  config_ = new TlsTcpConfig(config);
  int numOfRunnerThreads = config.selfId_ > static_cast<uint64_t>(config.maxServerId_) ? 1 : NUM_THREADS;
  runner_.reset(new tls::Runner(config, numOfRunnerThreads));
}

TlsTCPCommunication::~TlsTCPCommunication() { delete config_; }

TlsTCPCommunication *TlsTCPCommunication::create(const TlsTcpConfig &config) { return new TlsTCPCommunication(config); }

int TlsTCPCommunication::getMaxMessageSize() { return runner_->getMaxMessageSize(); }

int TlsTCPCommunication::start() {
  runner_->start();
  return 0;
}

int TlsTCPCommunication::stop() {
  if (!runner_) {
    return -1;
  }
  runner_->stop();
  return 0;
}

bool TlsTCPCommunication::isRunning() const { return runner_->isRunning(); }

ConnectionStatus TlsTCPCommunication::getCurrentConnectionStatus(const NodeNum node) {
  return runner_->getCurrentConnectionStatus(node);
}

int TlsTCPCommunication::send(NodeNum destNode, std::vector<uint8_t> &&msg, NodeNum endpointNum) {
  auto outgoingMsg = std::make_shared<tls::OutgoingMsg>(std::move(msg), endpointNum);
  runner_->send(destNode, outgoingMsg);
  return 0;
}

std::set<NodeNum> TlsTCPCommunication::send(std::set<NodeNum> dests,
                                            std::vector<uint8_t> &&msg,
                                            NodeNum srcEndpointNum) {
  std::set<NodeNum> failed_nodes;
  auto outgoingMsg = std::make_shared<tls::OutgoingMsg>(std::move(msg), srcEndpointNum);
  runner_->send(dests, outgoingMsg);
  return failed_nodes;
}

void TlsTCPCommunication::setReceiver(NodeNum id, IReceiver *receiver) { runner_->setReceiver(id, receiver); }

void TlsTCPCommunication::restartCommunication(NodeNum i) {
  if (i == config_->selfId_) {
    runner_->stop();
    runner_->start();
  }
}
}  // namespace bft::communication
