// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
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
static constexpr size_t NUM_THREADS = 2;

namespace bft::communication {

// This is the public interface to this library. TlsTcpCommunication implements ICommunication.
TlsTCPCommunication::TlsTCPCommunication(const TlsTcpConfig &config) : config_(config) {
  // Runners can actually support multiple principals. The Communication interface does not though. Currently we are
  // focused on backwards compatibility, and will use the future runner functionality to replace client thread pools.
  const auto configs = std::vector<TlsTcpConfig>{config};
  if (config.selfId > static_cast<uint64_t>(config.maxServerId))
    runner_.reset(new tls::Runner(configs, 1));
  else
    runner_.reset(new tls::Runner(configs, NUM_THREADS));
}

TlsTCPCommunication::~TlsTCPCommunication() {}

TlsTCPCommunication *TlsTCPCommunication::create(const TlsTcpConfig &config) { return new TlsTCPCommunication(config); }

int TlsTCPCommunication::getMaxMessageSize() {
  try {
    return runner_->principals().at(config_.selfId).getMaxMessageSize();
  } catch (const std::out_of_range &e) {
    LOG_FATAL(GL, "runner_->principals.at() failed for " << KVLOG(config_.selfId) << e.what());
    throw;
  }
}

int TlsTCPCommunication::Start() {
  runner_->start();
  return 0;
}

int TlsTCPCommunication::Stop() {
  if (!runner_) {
    return -1;
  }
  runner_->stop();
  return 0;
}

bool TlsTCPCommunication::isRunning() const { return runner_->isRunning(); }

ConnectionStatus TlsTCPCommunication::getCurrentConnectionStatus(const NodeNum node) {
  try {
    return runner_->principals().at(config_.selfId).getCurrentConnectionStatus(node);
  } catch (const std::out_of_range &e) {
    LOG_FATAL(GL, "runner_->principals.at() failed for " << KVLOG(config_.selfId) << e.what());
    throw;
  }
}

int TlsTCPCommunication::send(NodeNum destNode, std::vector<uint8_t> &&msg) {
  auto omsg = std::make_shared<tls::OutgoingMsg>(std::move(msg));
  try {
    runner_->principals().at(config_.selfId).send(destNode, omsg);
  } catch (const std::out_of_range &e) {
    LOG_FATAL(GL, "runner_->principals.at() failed for " << KVLOG(config_.selfId) << e.what());
    throw;
  }
  return 0;
}

std::set<NodeNum> TlsTCPCommunication::send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg) {
  std::set<NodeNum> failed_nodes;
  auto omsg = std::make_shared<tls::OutgoingMsg>(std::move(msg));
  try {
    runner_->principals().at(config_.selfId).send(dests, omsg);
  } catch (const std::out_of_range &e) {
    LOG_FATAL(GL, "runner_->principals.at() failed for " << KVLOG(config_.selfId) << e.what());
    throw;
  }
  return failed_nodes;
}

void TlsTCPCommunication::setReceiver(NodeNum id, IReceiver *receiver) {
  try {
    runner_->principals().at(config_.selfId).setReceiver(id, receiver);
  } catch (const std::out_of_range &e) {
    LOG_FATAL(GL, "runner_->principals.at() failed for " << KVLOG(config_.selfId) << e.what());
    throw;
  }
}

}  // namespace bft::communication
