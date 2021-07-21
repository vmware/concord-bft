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
#include "bftengine/ReconfigurationCmd.hpp"
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
  bftEngine::ControlStateManager::instance().setRestartBftFlag(command.bft);
  if (command.bft) {
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
  // update reserved pages for RO replica
  auto epochNum = bftEngine::EpochManager::instance().getSelfEpochNumber();
  bftEngine::ReconfigurationCmd::instance().saveReconfigurationCmdToResPages(command, bft_seq_num, epochNum);

  return true;
}  // namespace concord::reconfiguration

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

bool ReconfigurationHandler::handle(const UnwedgeCommand& cmd,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "Unwedge command started");
  auto& controlStateManager = bftEngine::ControlStateManager::instance();
  bool valid = controlStateManager.verifyUnwedgeSignatures(cmd.signatures);
  if (valid) {
    controlStateManager.clearCheckpointToStopAt();
    bftEngine::IControlHandler::instance()->resetState();
    LOG_INFO(getLogger(), "Unwedge command completed sucessfully");
  }
  return valid;
}

bool ReconfigurationHandler::handle(const UnwedgeStatusRequest& req,
                                    uint64_t,
                                    concord::messages::ReconfigurationResponse& rres) {
  concord::messages::UnwedgeStatusResponse response;
  auto can_unwedge = bftEngine::ControlStateManager::instance().canUnwedge();
  response.replica_id = bftEngine::ReplicaConfig::instance().replicaId;
  if (!can_unwedge.first) {
    response.can_unwedge = false;
    response.reason = can_unwedge.second;
    LOG_INFO(getLogger(), "Replica is not ready to unwedge. Reason: " << can_unwedge.second);
  } else {
    response.can_unwedge = true;
    response.signature = std::vector<uint8_t>(can_unwedge.second.begin(), can_unwedge.second.end());
    LOG_INFO(getLogger(), "Replica is ready to unwedge");
  }
  rres.response = response;
  return true;
}
}  // namespace concord::reconfiguration
