// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "reconfiguration/reconfiguration_handler.hpp"

#include "bftengine/KeyExchangeManager.hpp"
#include "bftengine/ControlStateManager.hpp"
#include "bftengine/EpochManager.hpp"
#include "Replica.hpp"
#include "kvstream.h"

using namespace concord::messages;
namespace concord::reconfiguration {

bool ReconfigurationHandler::handle(const WedgeCommand& cmd,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "Wedge command instructs replica to stop at sequence number " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  return true;
}

bool ReconfigurationHandler::handle(const WedgeStatusRequest& req,
                                    uint64_t,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse& rres) {
  concord::messages::WedgeStatusResponse response;
  if (req.fullWedge) {
    response.stopped = bftEngine::IControlHandler::instance()->isOnNOutOfNCheckpoint();
  } else {
    response.stopped = bftEngine::IControlHandler::instance()->isOnStableCheckpoint();
  }
  rres.response = response;
  return true;
}

bool ReconfigurationHandler::handle(const KeyExchangeCommand& command,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  std::ostringstream oss;
  std::copy(command.target_replicas.begin(), command.target_replicas.end(), std::ostream_iterator<int>(oss, " "));

  LOG_INFO(GL, KVLOG(command.id, command.sender_id, sequence_number) << " target replicas: [" << oss.str() << "]");
  if (std::find(command.target_replicas.begin(),
                command.target_replicas.end(),
                bftEngine::ReplicaConfig::instance().getreplicaId()) != command.target_replicas.end())
    bftEngine::impl::KeyExchangeManager::instance().sendKeyExchange(sequence_number);
  else
    LOG_INFO(GL, "not among target replicas, ignoring...");

  return true;
}
bool ReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeCommand& command,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "AddRemoveWithWedgeCommand instructs replica to stop at seq_num " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  handleWedgeCommands(command.bft_support, true, command.restart, true);
  return true;
}
void ReconfigurationHandler::handleWedgeCommands(bool bft_support, bool remove_metadata, bool restart, bool unwedge) {
  if (restart) bftEngine::ControlStateManager::instance().setRestartBftFlag(bft_support);
  if (bft_support) {
    if (remove_metadata)
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    if (unwedge)
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bftEngine::EpochManager::instance().setNewEpochFlag(true); });
    if (restart)
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(); });
  } else {
    if (remove_metadata)
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    if (unwedge)
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bftEngine::EpochManager::instance().setNewEpochFlag(true); });
    if (restart)
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(); });
  }
}
bool ReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeStatus& req,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse& rres) {
  concord::messages::AddRemoveWithWedgeStatusResponse response;
  if (std::holds_alternative<concord::messages::AddRemoveWithWedgeStatusResponse>(rres.response)) {
    response = std::get<concord::messages::AddRemoveWithWedgeStatusResponse>(rres.response);
    if (!response.bft_flag) {
      response.wedge_status = bftEngine::IControlHandler::instance()->isOnNOutOfNCheckpoint();
    } else {
      response.wedge_status = bftEngine::IControlHandler::instance()->isOnStableCheckpoint();
    }
    LOG_INFO(getLogger(), "AddRemoveWithWedgeStatus. wedge_status " << KVLOG(response.wedge_status));
  } else {
    LOG_WARN(getLogger(), "AddRemoveWithWedgeCommand is not logged into the chain. Return wedge_status false");
  }
  rres.response = std::move(response);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::RestartCommand& command,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "RestartCommand instructs replica to stop at seq_num " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  handleWedgeCommands(command.bft_support, true, command.restart, true);
  return true;
}

BftReconfigurationHandler::BftReconfigurationHandler() {
  auto operatorPubKeyPath = bftEngine::ReplicaConfig::instance().pathToOperatorPublicKey_;
  if (operatorPubKeyPath.empty()) {
    LOG_WARN(getLogger(),
             "The operator public key is missing, the reconfiguration handler won't be able to execute the requests");
    return;
  }
  std::ifstream key_content;
  key_content.open(operatorPubKeyPath);
  if (!key_content) {
    LOG_ERROR(getLogger(), "unable to read the operator public key file");
    return;
  }
  auto key_str = std::string{};
  auto buf = std::string(4096, '\0');
  while (key_content.read(&buf[0], 4096)) {
    key_str.append(buf, 0, key_content.gcount());
  }
  key_str.append(buf, 0, key_content.gcount());
  verifier_.reset(new concord::util::crypto::ECDSAVerifier(key_str, concord::util::crypto::KeyFormat::PemFormat));
}
bool BftReconfigurationHandler::verifySignature(uint32_t sender_id,
                                                const std::string& data,
                                                const std::string& signature) const {
  if (verifier_ == nullptr) return false;
  return verifier_->verify(data, signature);
}

bool ClientReconfigurationHandler::handle(const concord::messages::ClientExchangePublicKey& msg,
                                          uint64_t,
                                          uint32_t sender_id,
                                          concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "public key: " << msg.pub_key << " sender: " << sender_id);
  std::vector<uint32_t> affected_clients;
  if (!msg.affected_clients.empty()) {
    for (const auto& clientId : msg.affected_clients) {
      affected_clients.push_back(clientId);
    }
  } else {
    LOG_INFO(getLogger(), "apply all public key to the whole relevant group");
    for (const auto& [_, cg] : bftEngine::ReplicaConfig::instance().publicKeysOfClients) {
      (void)_;
      if (std::find(cg.begin(), cg.end(), sender_id) != cg.end()) {
        affected_clients.assign(cg.begin(), cg.end());
        break;
      }
    }
  }
  // TODO: [YB] verify the sender and the affected clients are in the same group
  // assuming we always send hex DER over the wire
  for (const auto& clientId : affected_clients)
    bftEngine::impl::KeyExchangeManager::instance().onClientPublicKeyExchange(
        msg.pub_key, concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat, clientId);
  return true;
}
}  // namespace concord::reconfiguration
