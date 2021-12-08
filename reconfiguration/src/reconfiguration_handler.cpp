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
#include "messages/ReplicaRestartReadyMsg.hpp"
#include "bftengine/EpochManager.hpp"
#include "Replica.hpp"
#include "kvstream.h"
#include "communication/StateControl.hpp"
#include "secrets_manager_plain.h"
#include "bftengine/DbCheckpointManager.hpp"

#include <fstream>

using namespace concord::messages;
namespace concord::reconfiguration {

bool ReconfigurationHandler::handle(const WedgeCommand& cmd,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    const std::optional<bftEngine::Timestamp>&,
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "Wedge command instructs replica to stop at sequence number " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  return true;
}

bool ReconfigurationHandler::handle(const WedgeStatusRequest& req,
                                    uint64_t,
                                    uint32_t,
                                    const std::optional<bftEngine::Timestamp>&,
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
                                    const std::optional<bftEngine::Timestamp>&,
                                    concord::messages::ReconfigurationResponse& rres) {
  if (command.tls && command.target_replicas.size() > bftEngine::ReplicaConfig::instance().fVal) {
    concord::messages::ReconfigurationErrorMsg error_msg{
        "Unable to perform tls key exchange for more than f replicas at once"};
    rres.response = error_msg;
    return false;
  }
  std::ostringstream oss;
  std::copy(command.target_replicas.begin(), command.target_replicas.end(), std::ostream_iterator<int>(oss, " "));

  LOG_INFO(GL, KVLOG(command.id, command.sender_id, sequence_number) << " target replicas: [" << oss.str() << "]");
  if (std::find(command.target_replicas.begin(),
                command.target_replicas.end(),
                bftEngine::ReplicaConfig::instance().getreplicaId()) == command.target_replicas.end())
    return true;
  if (command.tls) {
    bftEngine::impl::KeyExchangeManager::instance().exchangeTlsKeys(sequence_number);
  } else {
    bftEngine::impl::KeyExchangeManager::instance().sendKeyExchange(sequence_number);
  }
  return true;
}
bool ReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeCommand& command,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    const std::optional<bftEngine::Timestamp>&,
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "AddRemoveWithWedgeCommand instructs replica to stop at seq_num " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  handleWedgeCommands(command.bft_support, true, command.restart, true, true);
  std::ofstream configuration_file;
  configuration_file.open(bftEngine::ReplicaConfig::instance().configurationViewFilePath + "/" +
                              configurationsFileName + "." +
                              std::to_string(bftEngine::ReplicaConfig::instance().replicaId),
                          std::ios_base::app);
  if (!configuration_file.good()) {
    LOG_FATAL(getLogger(), "unable to open the reconfigurations file");
  }
  configuration_file << (command.config_descriptor + "\n");
  configuration_file.close();
  return true;
}
void ReconfigurationHandler::handleWedgeCommands(
    bool bft_support, bool remove_metadata, bool restart, bool unwedge, bool blockNewConnections) {
  if (restart) bftEngine::ControlStateManager::instance().setRestartBftFlag(bft_support);
  if (bft_support) {
    if (remove_metadata)
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    if (unwedge)
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bftEngine::EpochManager::instance().setNewEpochFlag(true); });
    if (restart)
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack([=]() {
        bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(
            static_cast<uint8_t>(ReplicaRestartReadyMsg::Reason::Scale), std::string{});
      });
    if (blockNewConnections) {
      bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
          [=]() { bft::communication::StateControl::instance().lockComm(); });
    }
  } else {
    if (remove_metadata)
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bftEngine::ControlStateManager::instance().markRemoveMetadata(); });
    if (unwedge)
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bftEngine::EpochManager::instance().setNewEpochFlag(true); });
    if (restart)
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack([=]() {
        bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(
            static_cast<uint8_t>(ReplicaRestartReadyMsg::Reason::Scale), std::string{});
      });
    if (blockNewConnections) {
      bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
          [=]() { bft::communication::StateControl::instance().lockComm(); });
    }
  }
}
bool ReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeStatus& req,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    const std::optional<bftEngine::Timestamp>&,
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
                                    const std::optional<bftEngine::Timestamp>&,
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "RestartCommand instructs replica to stop at seq_num " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  handleWedgeCommands(command.bft_support, true, command.restart, true, false);
  return true;
}
bool ReconfigurationHandler::handle(const concord::messages::InstallCommand& cmd,
                                    uint64_t sequence_num,
                                    uint32_t,
                                    const std::optional<bftEngine::Timestamp>&,
                                    concord::messages::ReconfigurationResponse& rres) {
  concord::messages::ReconfigurationErrorMsg error_msg;
  if (cmd.version.empty()) {
    LOG_WARN(getLogger(), "InstallCommand received with empty version string at seq_num " << sequence_num);
    return false;
  }
  LOG_INFO(getLogger(), "InstallCommand instructs replica to stop at seq_num " << KVLOG(sequence_num, cmd.version));
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(sequence_num);
  // TODO(NK): set remove_metadata and unwedge flag to True once we support start new epoch with (n-f) nodes
  // crrently, keyExchange manager requires all n nodes to the start to complete key exchange. So, if we
  // execute install with (n-f) nodes, post inatall, replicas won't be live
  bftEngine::ControlStateManager::instance().setRestartBftFlag(cmd.bft_support);
  if (cmd.bft_support) {
    bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack([=]() {
      bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(
          static_cast<uint8_t>(ReplicaRestartReadyMsg::Reason::Install), cmd.version);
    });
    bftEngine::IControlHandler::instance()->addOnStableCheckpointCallBack(
        [=]() { bftEngine::EpochManager::instance().setNewEpochFlag(true); });
  } else {
    bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack([=]() {
      bftEngine::ControlStateManager::instance().sendRestartReadyToAllReplica(
          static_cast<uint8_t>(ReplicaRestartReadyMsg::Reason::Install), cmd.version);
    });
    bftEngine::IControlHandler::instance()->addOnSuperStableCheckpointCallBack(
        [=]() { bftEngine::EpochManager::instance().setNewEpochFlag(true); });
  }
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::GetDbCheckpointInfoRequest& req,
                                    uint64_t,
                                    uint32_t,
                                    const std::optional<bftEngine::Timestamp>&,
                                    concord::messages::ReconfigurationResponse& rres) {
  concord::messages::GetDbCheckpointInfoStatusResponse response;
  const auto& dbCheckpointList = DbCheckpointManager::instance().getListOfDbCheckpoints();
  for (const auto& kv : dbCheckpointList) {
    concord::messages::DbCheckpointInfo dbcpinfo_msg;
    dbcpinfo_msg.seq_num = kv.second.lastDbCheckpointSeqNum_;
    dbcpinfo_msg.block_id = kv.second.lastBlockId_;
    dbcpinfo_msg.timestamp = kv.second.creationTimeSinceEpoch_.count();

    response.db_checkpoint_info.push_back(dbcpinfo_msg);
    LOG_INFO(getLogger(),
             "GetDbCheckpointInfoStatus checkpoint id's are "
                 << KVLOG(kv.second.lastBlockId_, kv.second.lastDbCheckpointSeqNum_));
  }
  rres.response = std::move(response);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::CreateDbCheckpointCommand& cmd,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    const std::optional<bftEngine::Timestamp>&,
                                    concord::messages::ReconfigurationResponse& rres) {
  if (bftEngine::ReplicaConfig::instance().maxNumberOfDbCheckpoints) {
    LOG_INFO(getLogger(), "CreateDbCheckpointCommand, " << KVLOG(sequence_number));
    DbCheckpointManager::instance().setNextStableSeqNumToCreateSnapshot(sequence_number);
    return true;
  } else {
    LOG_WARN(getLogger(), "db checkpoint is disabled. CreateDbCheckpointCommand failed.");
    return false;
  }
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
    LOG_WARN(getLogger(), "unable to read the operator public key file");
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
                                          const std::optional<bftEngine::Timestamp>&,
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
