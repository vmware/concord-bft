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

#include "PrimitiveTypes.hpp"
#include "ICommunication.hpp"
#include "IncomingMsgsStorage.hpp"
#include "messages/IncomingMsg.hpp"

namespace bftEngine::impl {

class MsgsCommunicator {
 public:
  explicit MsgsCommunicator(ICommunication* comm,
                            std::shared_ptr<IncomingMsgsStorage>& incomingMsgsStorage,
                            std::shared_ptr<IReceiver>& msgReceiver);
  virtual ~MsgsCommunicator() = default;

  int startCommunication(uint16_t replicaId);
  int stopCommunication();
  void startMsgsProcessing(uint16_t replicaId);
  void stopMsgsProcessing();

  [[nodiscard]] bool isMsgsProcessingRunning() const { return incomingMsgsStorage_->isRunning(); }
  int sendAsyncMessage(NodeNum destNode, char* message, size_t messageLength);

  std::shared_ptr<IncomingMsgsStorage>& getIncomingMsgsStorage() { return incomingMsgsStorage_; }

 private:
  uint16_t replicaId_ = 0;
  std::shared_ptr<IncomingMsgsStorage> incomingMsgsStorage_;
  std::shared_ptr<IReceiver> msgReceiver_;
  ICommunication* communication_;
};

}  // namespace bftEngine::impl
