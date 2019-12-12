// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "MsgsCommunicator.hpp"

namespace bftEngine::impl {

using namespace std;

MsgsCommunicator::MsgsCommunicator(ICommunication* comm)
    : incomingMsgsStorage_(maxMessagesNumInStorage_), msgReceiver_(new MsgReceiver(incomingMsgsStorage_)) {
  communication_ = comm;
}

int MsgsCommunicator::start(ReplicaId myReplicaId) {
  communication_->setReceiver(myReplicaId, msgReceiver_.get());
  return communication_->Start();
}

int MsgsCommunicator::stop() { return communication_->Stop(); }

int MsgsCommunicator::sendAsyncMessage(NodeNum destNode, char* message, size_t messageLength) {
  return communication_->sendAsyncMessage(destNode, message, messageLength);
}

void MsgsCommunicator::pushInternalMsg(std::unique_ptr<InternalMessage> msg) {
  incomingMsgsStorage_.pushInternalMsg(move(msg));
}

IncomingMsg MsgsCommunicator::popInternalOrExternalMsg(chrono::milliseconds timeout) {
  return incomingMsgsStorage_.popInternalOrExternalMsg(timeout);
}

}  // namespace bftEngine::impl
