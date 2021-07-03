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

#pragma once

#include "communication/ICommunication.hpp"
#include "communication/CommDefs.hpp"
#include "communication/CommFactory.hpp"

#include "messages/MessageBase.hpp"

#include "TesterReplica/strategy/ByzantineStrategy.hpp"

namespace bft::communication {

using concord::kvbc::strategy::IByzantineStrategy;
using bftEngine::impl::MessageBase;

class WrapCommunication : public ICommunication {
 public:
  explicit WrapCommunication(std::unique_ptr<bft::communication::ICommunication> comm)
      : communication_(std::move(comm)) {}

  int getMaxMessageSize() override { return communication_->getMaxMessageSize(); }
  int Start() override { return communication_->Start(); }
  int Stop() override { return communication_->Stop(); }
  bool isRunning() const override { return communication_->isRunning(); }
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override {
    return communication_->getCurrentConnectionStatus(node);
  }

  int send(NodeNum destNode, std::vector<uint8_t> &&msg) override {
    std::shared_ptr<MessageBase> newMsg;
    if (changeMesssage(msg, newMsg) && newMsg) {
      std::vector<uint8_t> chgMsg(newMsg->body(), newMsg->body() + newMsg->size());
      return communication_->send(destNode, std::move(chgMsg));
    }
    return communication_->send(destNode, std::move(msg));
  }
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg) override {
    std::set<NodeNum> failedNodes;
    for (auto dst : dests) {
      std::vector<uint8_t> nMsg(msg);
      if (send(dst, std::move(nMsg)) != 0) {
        failedNodes.insert(dst);
      }
    }
    return failedNodes;
  }

  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override {
    communication_->setReceiver(receiverNum, receiver);
  }

  static void addStrategies(std::string const &strategies,
                            char delim,
                            std::vector<std::shared_ptr<IByzantineStrategy>> const &allStrategies);

  virtual ~WrapCommunication() override {}

 private:
  bool changeMesssage(std::vector<uint8_t> const &msg, std::shared_ptr<MessageBase> &newMsg);
  static void addStrategy(uint16_t msgCode, std::shared_ptr<IByzantineStrategy> byzantineStrategy);

 private:
  std::unique_ptr<bft::communication::ICommunication> communication_;
  static std::map<uint16_t, std::shared_ptr<IByzantineStrategy>> changeStrategy;
};

}  // namespace bft::communication
