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
#include "bftengine/EpochManager.hpp"
#include "secrets_manager_plain.h"
#include "communication/StateControl.hpp"
#include "client/reconfiguration/poll_based_state_client.hpp"
#include "communication/ICommunication.hpp"
#include "bftclient/bft_client.h"
#include "ControlStateManager.hpp"
#include "reconfiguration/ireconfiguration.hpp"

namespace bftEngine::bcst::asyncCRE {
using namespace concord::client::reconfiguration;
using namespace bft::communication;
class Communication : public ICommunication {
 public:
  Communication(std::shared_ptr<MsgsCommunicator> msgsCommunicator, std::shared_ptr<MsgHandlersRegistrator> msgHandlers)
      : msgsCommunicator_{msgsCommunicator}, repId_{bftEngine::ReplicaConfig::instance().replicaId} {
    msgHandlers->registerMsgHandler(MsgCode::ClientReply, [&](bftEngine::impl::MessageBase* message) {
      if (receiver_) receiver_->onNewMessage(message->senderId(), message->body(), message->size());
    });
  }

  int getMaxMessageSize() override { return 128 * 1024; }  // 128KB
  int start() override {
    is_running_ = true;
    return 0;
  }
  int stop() override {
    is_running_ = false;
    return 0;
  }
  bool isRunning() const override { return is_running_; }
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override {
    if (!is_running_) return ConnectionStatus::Disconnected;
    return ConnectionStatus::Connected;
  }
  int send(NodeNum destNode, std::vector<uint8_t>&& msg, NodeNum endpointNum) override {
    if (destNode == repId_) {
      return msg.size();
    }
    return msgsCommunicator_->sendAsyncMessage(destNode, reinterpret_cast<char*>(msg.data()), msg.size());
  }
  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t>&& msg, NodeNum endpointNum) override {
    auto ret = dests;
    dests.erase(repId_);

    msgsCommunicator_->send(dests, reinterpret_cast<char*>(msg.data()), msg.size());
    return ret;
  }
  void setReceiver(NodeNum receiverNum, IReceiver* receiver) override { receiver_ = receiver; }
  void restartCommunication(NodeNum i) override {}

 private:
  std::shared_ptr<MsgsCommunicator> msgsCommunicator_;
  bool is_running_ = false;
  IReceiver* receiver_ = nullptr;
  uint16_t repId_;
};

class InternalSigner : public concord::util::crypto::ISigner {
 public:
  std::string sign(const std::string& data) override {
    std::string out;
    out.resize(bftEngine::impl::SigManager::instance()->getMySigLength());
    bftEngine::impl::SigManager::instance()->sign(data.data(), data.size(), out.data(), signatureLength());
    return out;
  }
  uint32_t signatureLength() const override { return bftEngine::impl::SigManager::instance()->getMySigLength(); }
  std::string getPrivKey() const override { return ""; }
};

class ScalingReplicaHandler : public IStateHandler {
 public:
  ScalingReplicaHandler() {}
  bool validate(const State& state) const override {
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    if (crep.epoch < EpochManager::instance().getSelfEpochNumber()) return false;
    if (std::holds_alternative<concord::messages::ClientsAddRemoveExecuteCommand>(crep.response)) {
      concord::messages::ClientsAddRemoveExecuteCommand command =
          std::get<concord::messages::ClientsAddRemoveExecuteCommand>(crep.response);
      std::ofstream configurations_file;
      configurations_file.open(bftEngine::ReplicaConfig::instance().configurationViewFilePath + "/" +
                                   concord::reconfiguration::configurationsFileName + "." +
                                   std::to_string(bftEngine::ReplicaConfig::instance().replicaId),
                               std::ios_base::app);
      if (configurations_file.good()) {
        std::stringstream stream;
        stream << configurations_file.rdbuf();
        std::string configs = stream.str();
        return (configs.empty()) || (configs.find(command.config_descriptor) != std::string::npos);
      }
    }
    return false;
  }

  bool execute(const State& state, WriteState&) override {
    LOG_INFO(getLogger(), "execute AddRemoveWithWedgeCommand");
    concord::messages::ClientStateReply crep;
    concord::messages::deserialize(state.data, crep);
    concord::messages::ClientsAddRemoveExecuteCommand command =
        std::get<concord::messages::ClientsAddRemoveExecuteCommand>(crep.response);
    std::ofstream configuration_file;
    configuration_file.open(bftEngine::ReplicaConfig::instance().configurationViewFilePath + "/" +
                                concord::reconfiguration::configurationsFileName + "." +
                                std::to_string(bftEngine::ReplicaConfig::instance().replicaId),
                            std::ios_base::app);
    if (!configuration_file.good()) {
      LOG_FATAL(getLogger(), "unable to open the reconfigurations file");
    }
    configuration_file << (command.config_descriptor + "\n");
    configuration_file.close();
    LOG_INFO(getLogger(), "getting new configuration");
    bftEngine::ControlStateManager::instance().getNewConfiguration(command.config_descriptor, command.token);
    bftEngine::ControlStateManager::instance().markRemoveMetadata();
    LOG_INFO(getLogger(), "completed scaling procedure for " << command.config_descriptor << " restarting the replica");
    if (command.restart) bftEngine::ControlStateManager::instance().restart();
    return true;
  }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("bftEngine::bcst::asyncCRE.ScalingReplicaHandler"));
    return logger_;
  }
};
std::shared_ptr<ClientReconfigurationEngine> CreFactory::create(std::shared_ptr<MsgsCommunicator> msgsCommunicator,
                                                                std::shared_ptr<MsgHandlersRegistrator> msgHandlers) {
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
  bftClientConf.replicas_master_key_folder_path = std::nullopt;
  std::unique_ptr<ICommunication> comm = std::make_unique<Communication>(msgsCommunicator, msgHandlers);
  bft::client::Client* bftClient = new bft::client::Client(std::move(comm), bftClientConf);
  bftClient->setTransactionSigner(new InternalSigner());
  Config cre_config;
  cre_config.id_ = repConfig.replicaId;
  cre_config.interval_timeout_ms_ = 1000;
  IStateClient* pbc = new PollBasedStateClient(bftClient, cre_config.interval_timeout_ms_, 0, cre_config.id_);
  auto cre =
      std::make_shared<ClientReconfigurationEngine>(cre_config, pbc, std::make_shared<concordMetrics::Aggregator>());
  if (!bftEngine::ReplicaConfig::instance().isReadOnly) cre->registerHandler(std::make_shared<ScalingReplicaHandler>());
  return cre;
}
}  // namespace bftEngine::bcst::asyncCRE
