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
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "Wedge command instructs replica to stop at sequence number " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  return true;
}

bool ReconfigurationHandler::handle(const WedgeStatusRequest& req,
                                    uint64_t,
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
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "AddRemoveWithWedgeCommand instructs replica to stop at seq_num " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  bftEngine::ControlStateManager::instance().setRestartBftFlag(command.bft_support);
  if (command.bft_support) {
    bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack([=]() {
      bftEngine::ControlStateManager::instance().markRemoveMetadata();
      bftEngine::EpochManager::instance().setNewEpochFlag(true);
    });
    if (command.restart) {
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(); });
    }
  } else {
    bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack([=]() {
      bftEngine::ControlStateManager::instance().markRemoveMetadata();
      bftEngine::EpochManager::instance().setNewEpochFlag(true);
    });
    if (command.restart) {
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(); });
    }
  }
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::RestartCommand& command,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "RestartCommand instructs replica to stop at seq_num " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  bftEngine::ControlStateManager::instance().setRestartBftFlag(command.bft_support);
  if (command.bft_support) {
    bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
        [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    if (command.restart) {
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(); });
    }
  } else {
    bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
        [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    if (command.restart) {
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(); });
    }
  }
  return true;
}

BftReconfigurationHandler::BftReconfigurationHandler() {
  auto operatorPubKeyPath = bftEngine::ReplicaConfig::instance().pathToOperatorPublicKey_;
  if (operatorPubKeyPath.empty()) {
    LOG_WARN(getLogger(),
             "The operator public key is missing, the reconfiguration handler won't be able to execute the requests");
    return;
  }
  try {
    pub_key_ = concord::util::openssl_utils::deserializePublicKeyFromPem(operatorPubKeyPath, "secp256r1");
  } catch (const std::exception& e) {
    LOG_ERROR(
        getLogger(),
        "(1) Unable to read operator key, the replica won't be able to perform reconfiguration actions " << e.what());
    pub_key_ = nullptr;
  }
  try {
    verifier_ = std::make_unique<bftEngine::impl::ECDSAVerifier>(operatorPubKeyPath);
  } catch (const std::exception& e) {
    LOG_ERROR(
        getLogger(),
        "(2) Unable to read operator key, the replica won't be able to perform reconfiguration actions " << e.what());
    verifier_ = nullptr;
  }
}
bool BftReconfigurationHandler::verifySignature(uint32_t sender_id,
                                                const std::string& data,
                                                const std::string& signature) const {
  if (pub_key_ == nullptr && verifier_ == nullptr) return false;
  return pub_key_->verify(data, signature) || verifier_->verify(data, signature);
}

bool ClientReconfigurationHandler::handle(const concord::messages::ClientExchangePublicKey& msg,
                                          uint64_t,
                                          concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "public key: " << msg.pub_key << " sender: " << msg.sender_id);
  bftEngine::impl::KeyExchangeManager::instance().onClientPublicKeyExchange(msg.pub_key, msg.sender_id);
  return true;
}
}  // namespace concord::reconfiguration
