// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
#include "AsyncStateTransferCRE.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "bftengine/SigManager.hpp"

namespace bftEngine::bcst::asyncCRE {
class InternalSigner : public concord::util::crypto::ISigner {
 public:
  std::string sign(const std::string& data) override {
    std::string out;
    out.resize(bftEngine::impl::SigManager::instance()->getMySigLength());
    bftEngine::impl::SigManager::instance()->sign(data.data(), data.size(), out.data(), signatureLength());
    return out;
  }
  uint32_t signatureLength() const override { return bftEngine::impl::SigManager::instance()->getMySigLength(); }
};
bftEngine::bcst::asyncCRE::Communication::Communication(std::shared_ptr<MsgsCommunicator> msgsCommunicator,
                                                        std::shared_ptr<MsgHandlersRegistrator> msgHandlers)
    : msgsCommunicator_{msgsCommunicator} {
  msgHandlers->registerMsgHandler(MsgCode::ClientReply, [&](bftEngine::impl::MessageBase* message) {
    if (receiver_) receiver_->onNewMessage(message->senderId(), message->body(), message->size());
  });
}
int Communication::start() {
  is_running_ = true;
  return 0;
}
int Communication::stop() {
  is_running_ = false;
  return 0;
}
bool Communication::isRunning() const { return is_running_; }
bft::communication::ConnectionStatus Communication::getCurrentConnectionStatus(bft::communication::NodeNum) {
  if (!is_running_) return bft::communication::ConnectionStatus::Disconnected;
  return bft::communication::ConnectionStatus::Connected;
}
int Communication::send(bft::communication::NodeNum destNode, std::vector<uint8_t>&& msg) {
  return msgsCommunicator_->sendAsyncMessage(destNode, reinterpret_cast<char*>(msg.data()), msg.size());
}
std::set<bft::communication::NodeNum> Communication::send(std::set<bft::communication::NodeNum> dests,
                                                          std::vector<uint8_t>&& msg) {
  auto ret = dests;
  msgsCommunicator_->send(dests, reinterpret_cast<char*>(msg.data()), msg.size());
  return ret;
}
void Communication::setReceiver(bft::communication::NodeNum receiverNum, bft::communication::IReceiver* receiver) {
  receiver_ = receiver;
}

std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine> CreFactory::create(
    std::shared_ptr<MsgsCommunicator> msgsCommunicator, std::shared_ptr<MsgHandlersRegistrator> msgHandlers) {
  bft::client::ClientConfig bftClientConf;
  auto& repConfig = bftEngine::ReplicaConfig::instance();
  bftClientConf.f_val = repConfig.fVal;
  bftClientConf.c_val = repConfig.cVal;
  bftClientConf.id = bft::client::ClientId{repConfig.replicaId};
  for (uint16_t i = 0; i < repConfig.numReplicas; i++) {
    bftClientConf.all_replicas.emplace(bft::client::ReplicaId{i});
  }
  for (uint16_t i = repConfig.numReplicas; i < repConfig.numReplicas + repConfig.numRoReplicas; i++) {
    bftClientConf.ro_replicas.emplace(bft::client::ReplicaId{i});
  }
  std::unique_ptr<bft::communication::ICommunication> comm =
      std::make_unique<Communication>(msgsCommunicator, msgHandlers);
  bft::client::Client* bftClient = new bft::client::Client(std::move(comm), bftClientConf);
  bftClient->setTransactionSigner(new InternalSigner());
  concord::client::reconfiguration::Config cre_config;
  cre_config.id_ = repConfig.replicaId;
  cre_config.interval_timeout_ms_ = 1000;
  concord::client::reconfiguration::IStateClient* pbc = new concord::client::reconfiguration::PollBasedStateClient(
      bftClient, cre_config.interval_timeout_ms_, 0, cre_config.id_);
  return std::make_shared<concord::client::reconfiguration::ClientReconfigurationEngine>(
      cre_config, pbc, std::make_shared<concordMetrics::Aggregator>());
}
}  // namespace bftEngine::bcst::asyncCRE
