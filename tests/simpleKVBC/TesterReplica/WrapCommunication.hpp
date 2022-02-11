// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
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
#include "Logger.hpp"
#include "messages/MessageBase.hpp"

#include "TesterReplica/strategy/ByzantineStrategy.hpp"

namespace bft::communication {

class WrapCommunication : public ICommunication {
 public:
  explicit WrapCommunication(std::unique_ptr<bft::communication::ICommunication> comm,
                             bool separate_communication,
                             logging::Logger logger)
      : communication_(std::move(comm)), separate_communication_(separate_communication), logger_(logger) {}

  int getMaxMessageSize() override { return communication_->getMaxMessageSize(); }
  int start() override { return communication_->start(); }
  int stop() override { return communication_->stop(); }
  bool isRunning() const override { return communication_->isRunning(); }
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override {
    return communication_->getCurrentConnectionStatus(node);
  }

  int send(NodeNum destNode, std::vector<uint8_t> &&msg, NodeNum endpointNum = MAX_ENDPOINT_NUM) override;
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg, NodeNum srcEndpointNum) override;

  void setReceiver(NodeNum receiverNum, IReceiver *receiver) override {
    communication_->setReceiver(receiverNum, receiver);
  }

  void restartCommunication(NodeNum i) override {}
  static void addStrategies(
      std::string const &strategies,
      char delim,
      std::vector<std::shared_ptr<concord::kvbc::strategy::IByzantineStrategy>> const &allStrategies);

  virtual ~WrapCommunication() override {}

 private:
  bool changeMesssage(std::vector<uint8_t> const &msg, std::shared_ptr<bftEngine::impl::MessageBase> &newMsg);
  static void addStrategy(uint16_t msgCode,
                          std::shared_ptr<concord::kvbc::strategy::IByzantineStrategy> byzantineStrategy);

 private:
  std::unique_ptr<bft::communication::ICommunication> communication_;
  bool separate_communication_;
  static std::map<uint16_t, std::shared_ptr<concord::kvbc::strategy::IByzantineStrategy>> changeStrategy;
  logging::Logger logger_;
};

}  // namespace bft::communication
