// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "communication/CommDefs.hpp"
#include "TlsTcpImpl.h"

namespace bft::communication {

// This is the public interface to this library. TlsTcpCommunication implements ICommunication.
TlsTCPCommunication::TlsTCPCommunication(const TlsTcpConfig &config) : impl_(TlsTcpImpl::create(config)) {}

TlsTCPCommunication::~TlsTCPCommunication() {}

TlsTCPCommunication *TlsTCPCommunication::create(const TlsTcpConfig &config) { return new TlsTCPCommunication(config); }

int TlsTCPCommunication::getMaxMessageSize() { return impl_->getMaxMessageSize(); }

int TlsTCPCommunication::Start() { return impl_->Start(); }

int TlsTCPCommunication::Stop() {
  if (!impl_) {
    return -1;
  }
  return impl_->Stop();
}

bool TlsTCPCommunication::isRunning() const { return impl_->isRunning(); }

ConnectionStatus TlsTCPCommunication::getCurrentConnectionStatus(const NodeNum node) {
  return impl_->getCurrentConnectionStatus(node);
}

int TlsTCPCommunication::sendAsyncMessage(NodeNum destNode, std::vector<uint8_t> &&msg) {
  auto omsg = std::make_shared<OutgoingMsg>(std::move(msg));
  return impl_->sendAsyncMessage(destNode, omsg);
}

std::set<NodeNum> TlsTCPCommunication::multiSendMessage(const std::set<NodeNum> dests, std::vector<uint8_t> &&msg) {
  std::set<NodeNum> failed_nodes;
  auto omsg = std::make_shared<OutgoingMsg>(std::move(msg));
  for (auto &d : dests) {
    if (impl_->sendAsyncMessage(d, omsg) != 0) {
      failed_nodes.insert(d);
    }
  }
  return failed_nodes;
}

void TlsTCPCommunication::setReceiver(NodeNum receiverNum, IReceiver *receiver) {
  impl_->setReceiver(receiverNum, receiver);
}

}  // namespace bft::communication
