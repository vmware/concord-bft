// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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

using namespace std;

namespace bft::communication {

//********************** Class TlsMultiplexReceiver **********************

void TlsMultiplexCommunication::TlsMultiplexReceiver::setReceiver(NodeNum receiverNum, IReceiver *receiver) {
  receiversMap_.insert_or_assign(receiverNum, receiver);
}

void TlsMultiplexCommunication::TlsMultiplexReceiver::onNewMessage(NodeNum sourceNode,
                                                                   const char *const message,
                                                                   size_t messageLength,
                                                                   NodeNum endpointNum) {
  // client -> replica: endpointNum = clientId
  // replica -> client: endpointNum = clientId
  // replica1 -> replica2: endpointNum = destNode = replica2
  NodeNum receiverId = MAX_ENDPOINT_NUM;
  if (config_.amIReplica_) {
    receiverId = config_.selfId_;
    if (config_.isClient(endpointNum))
      sourceNode = endpointNum;  // Replica received client message => set correct source node
  } else
    receiverId = endpointNum;

  const auto &receiver = receiversMap_.find(receiverId);
  if (receiver != receiversMap_.end()) {
    receiver->second->onNewMessage(sourceNode, message, messageLength);
    LOG_DEBUG(logger_, "Receiver found for" << KVLOG(receiverId, endpointNum, sourceNode));
    return;
  }
  LOG_ERROR(logger_, "Receiver not found for" << KVLOG(receiverId, endpointNum, sourceNode));
}

void TlsMultiplexCommunication::TlsMultiplexReceiver::onConnectionStatusChanged(NodeNum node,
                                                                                ConnectionStatus newStatus) {}

//********************** Class TlsMultiplexCommunication **********************

TlsMultiplexCommunication::TlsMultiplexCommunication(const TlsMultiplexConfig &config)
    : TlsTCPCommunication(config), logger_(logging::getLogger("concord-bft.tls.multiplex")) {
  ownReceiver_ = new TlsMultiplexReceiver(config);
  config_ = static_cast<TlsMultiplexConfig *>(config_);
}

TlsMultiplexCommunication *TlsMultiplexCommunication::create(const TlsMultiplexConfig &config) {
  return new TlsMultiplexCommunication(config);
}

TlsMultiplexCommunication::~TlsMultiplexCommunication() { delete ownReceiver_; }

int TlsMultiplexCommunication::start() {
  auto const result = TlsTCPCommunication::start();
  if (!result) runner_->setReceiver(config_->selfId_, ownReceiver_);
  return result;
}

void TlsMultiplexCommunication::setReceiver(NodeNum receiverNum, IReceiver *receiver) {
  // For a replica: the same receiver object serves all endpoint clients.
  // For a client: one receiver object serves one endpoint client.
  ownReceiver_->setReceiver(receiverNum, receiver);
}

int TlsMultiplexCommunication::send(NodeNum destNode, std::vector<uint8_t> &&msg, NodeNum endpointNum) {
  // client -> replica: endpointNum = clientId
  // replica -> client: endpointNum = clientId
  // replica1 -> replica2: endpointNum = destNode = replica2
  if (endpointNum == MAX_ENDPOINT_NUM) endpointNum = destNode;

  // Find connection-id to be used to reach specified endpoint entity.
  // clientservice connection to be used to reach clients, replicas' connection - to reach replicas.
  const auto &connectionId = config_->endpointIdToNodeIdMap_.find(endpointNum);
  if (connectionId != config_->endpointIdToNodeIdMap_.end()) {
    LOG_DEBUG(logger_, "Connection found:" << KVLOG(destNode, connectionId->second));
    return TlsTCPCommunication::send(connectionId->second, move(msg), endpointNum);
  }
  LOG_ERROR(logger_, "Connection not found for destination endpoint" << KVLOG(destNode));
  return 1;
}

ConnectionStatus TlsMultiplexCommunication::getCurrentConnectionStatus(NodeNum endpointNum) {
  auto const nodeEntryIt = config_->endpointIdToNodeIdMap_.find(endpointNum);
  if (nodeEntryIt != config_->endpointIdToNodeIdMap_.end()) {
    const auto connectionStatus = runner_->getCurrentConnectionStatus(nodeEntryIt->second);
    LOG_DEBUG(logger_, "Connection status:" << KVLOG(endpointNum, static_cast<int>(connectionStatus)));
    return connectionStatus;
  }
  LOG_WARN(logger_, "An active connection not found for node" << KVLOG(endpointNum));
  return ConnectionStatus::Unknown;
}

}  // namespace bft::communication
