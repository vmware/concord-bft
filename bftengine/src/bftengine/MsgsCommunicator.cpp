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
#include "ReplicaConfig.hpp"
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

int MsgsCommunicator::start() {
  incomingMsgsStorage_->start();
  communication_->setReceiver(ReplicaConfigSingleton::GetInstance().GetReplicaId(), msgReceiver_.get());
  int commStatus = communication_->Start();
  Assert(commStatus == 0);
  LOG_INFO(GL, "MsgsCommunicator for replica " << ReplicaConfigSingleton::GetInstance().GetReplicaId() << " started");
  return commStatus;
}

int MsgsCommunicator::stop() {
  incomingMsgsStorage_->stop();
  int res =  communication_->Stop();
  LOG_INFO(GL, "MsgsCommunicator for replica " << ReplicaConfigSingleton::GetInstance().GetReplicaId() << " stopped");
  return res;
}

int MsgsCommunicator::sendAsyncMessage(NodeNum destNode, char* message, size_t messageLength) {
  return communication_->sendAsyncMessage(destNode, message, messageLength);
}

}  // namespace bftEngine::impl