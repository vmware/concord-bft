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

#pragma once

#include "MsgReceiver.hpp"
#include "messages/IncomingMsg.hpp"

namespace bftEngine::impl {

class MsgsCommunicator {
 public:
  explicit MsgsCommunicator(ICommunication* comm);
  virtual ~MsgsCommunicator() = default;

  int start(ReplicaId myReplicaId);
  int stop();
  int sendAsyncMessage(NodeNum destNode, char* message, size_t messageLength);
  void pushInternalMsg(std::unique_ptr<InternalMessage> msg);
  IncomingMsg popInternalOrExternalMsg(std::chrono::milliseconds timeout);

 private:
  const uint32_t maxMessagesNumInStorage_ = 20000;
  IncomingMsgsStorage incomingMsgsStorage_;
  std::shared_ptr<MsgReceiver> msgReceiver_;
  ICommunication* communication_;
};

}  // namespace bftEngine::impl
