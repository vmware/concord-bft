// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "MsgReceiver.hpp"
#include "messages/MessageBase.hpp"
#include "ReplicaConfig.hpp"
#include <cstring>

namespace bftEngine::impl {

using namespace std;
using namespace bft::communication;

MsgReceiver::MsgReceiver(std::shared_ptr<IncomingMsgsStorage> &storage) : incomingMsgsStorage_(storage) {}

void MsgReceiver::onNewMessage(NodeNum sourceNode, const char *const message, size_t messageLength) {
  if (messageLength > ReplicaConfig::instance().getmaxExternalMessageSize()) {
    LOG_WARN(GL, "Msg exceeds allowed max msg size, size " << messageLength << " source " << sourceNode);
    return;
  }
  if (messageLength < sizeof(MessageBase::Header)) {
    LOG_WARN(GL, "Msg length is smaller than expected msg header, size " << messageLength << " source " << sourceNode);
    return;
  }

  auto *msgBody = (MessageBase::Header *)std::malloc(messageLength);
  memcpy(msgBody, message, messageLength);

  auto node = sourceNode;

  std::unique_ptr<MessageBase> pMsg(new MessageBase(node, msgBody, messageLength, true));

  incomingMsgsStorage_->pushExternalMsg(std::move(pMsg));
}

void MsgReceiver::onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) {}

}  // namespace bftEngine::impl
