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
#include "assertUtils.hpp"

namespace bftEngine::impl {

using namespace std;

MsgsCommunicator::MsgsCommunicator(ICommunication* comm,
                                   shared_ptr<IncomingMsgsStorage>& incomingMsgsStorage,
                                   shared_ptr<IReceiver>& msgReceiver) {
  communication_ = comm;
  incomingMsgsStorage_ = incomingMsgsStorage;
  msgReceiver_ = msgReceiver;
}

int MsgsCommunicator::startCommunication(uint16_t replicaId) {
  replicaId_ = replicaId;
  communication_->setReceiver(replicaId_, msgReceiver_.get());
  int commStatus = communication_->Start();
  Assert(commStatus == 0);
  LOG_INFO(GL, "Communication for replica " << replicaId_ << " started");
  return commStatus;
}

int MsgsCommunicator::stopCommunication() {
  int res = communication_->Stop();
  LOG_INFO(GL, "Communication for replica " << replicaId_ << " stopped");
  return res;
}

void MsgsCommunicator::startMsgsProcessing(uint16_t replicaId) {
  replicaId_ = replicaId;
  incomingMsgsStorage_->start();
  LOG_INFO(GL, "Messages processing for replica " << replicaId_ << " started");
}

void MsgsCommunicator::stopMsgsProcessing() {
  incomingMsgsStorage_->stop();
  LOG_INFO(GL, "Messages processing for replica " << replicaId_ << " stopped");
}

int MsgsCommunicator::sendAsyncMessage(NodeNum destNode, char* message, size_t messageLength) {
  return communication_->sendAsyncMessage(destNode, message, messageLength);
}

}  // namespace bftEngine::impl