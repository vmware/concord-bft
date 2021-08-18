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

#include "reconfiguration_kvbc_handler.hpp"
#include "ControlStateManager.hpp"
#include "bftengine/EpochManager.hpp"
#include "bftengine/ReconfigurationCmd.hpp"
#include "endianness.hpp"
#include "kvbc_app_filter/kvbc_key_types.h"

namespace concord::kvbc::reconfiguration {

kvbc::BlockId ReconfigurationBlockTools::persistReconfigurationBlock(const std::vector<uint8_t>& data,
                                                                     const uint64_t bft_seq_num,
                                                                     std::string key,
                                                                     bool include_wedge) {
  concord::kvbc::categorization::VersionedUpdates ver_updates;
  ver_updates.addUpdate(std::move(key), std::string(data.begin(), data.end()));

  // All blocks are expected to have the BFT sequence number as a key.
  ver_updates.addUpdate(std::string{kvbc::keyTypes::bft_seq_num_key}, block_metadata_.serialize(bft_seq_num));
  uint64_t epoch = 0;
  auto value = ro_storage_.getLatest(kConcordInternalCategoryId, std::string{keyTypes::reconfiguration_epoch_key});
  if (value.has_value()) {
    const auto& epoch_str = std::get<categorization::VersionedValue>(*value).data;
    ConcordAssertEQ(epoch_str.size(), sizeof(uint64_t));
    epoch = concordUtils::fromBigEndianBuffer<uint64_t>(epoch_str.data());
  }
  auto current_epoch_buf = concordUtils::toBigEndianStringBuffer(epoch);
  ver_updates.addUpdate(std::string{keyTypes::reconfiguration_epoch_key}, std::move(current_epoch_buf));
  if (include_wedge) {
    concord::messages::WedgeCommand wedge_command;
    std::vector<uint8_t> wedge_buf;
    concord::messages::serialize(wedge_buf, wedge_command);
    ver_updates.addUpdate(std::string{keyTypes::reconfiguration_wedge_key},
                          std::string(wedge_buf.begin(), wedge_buf.end()));
  }
  concord::kvbc::categorization::Updates updates;
  updates.add(kvbc::kConcordInternalCategoryId, std::move(ver_updates));
  try {
    return blocks_adder_.add(std::move(updates));
  } catch (const std::exception& e) {
    LOG_ERROR(GL, "failed to persist the reconfiguration block: " << e.what());
    throw;
  }
}

kvbc::BlockId ReconfigurationBlockTools::persistReconfigurationBlock(
    concord::kvbc::categorization::VersionedUpdates& ver_updates, const uint64_t bft_seq_num, bool include_wedge) {
  // All blocks are expected to have the BFT sequence number as a key.
  ver_updates.addUpdate(std::string{kvbc::keyTypes::bft_seq_num_key}, block_metadata_.serialize(bft_seq_num));
  if (include_wedge) {
    concord::messages::WedgeCommand wedge_command;
    std::vector<uint8_t> wedge_buf;
    concord::messages::serialize(wedge_buf, wedge_command);
    ver_updates.addUpdate(std::string{keyTypes::reconfiguration_wedge_key},
                          std::string(wedge_buf.begin(), wedge_buf.end()));
  }
  concord::kvbc::categorization::Updates updates;
  updates.add(kvbc::kConcordInternalCategoryId, std::move(ver_updates));
  try {
    return blocks_adder_.add(std::move(updates));
  } catch (const std::exception& e) {
    LOG_ERROR(GL, "failed to persist the reconfiguration block: " << e.what());
    throw;
  }
}

kvbc::BlockId ReconfigurationBlockTools::persistNewEpochBlock(const uint64_t bft_seq_num) {
  auto newEpoch = bftEngine::EpochManager::instance().getSelfEpochNumber() + 1;
  concord::kvbc::categorization::VersionedUpdates ver_updates;
  ver_updates.addUpdate(std::string{kvbc::keyTypes::reconfiguration_epoch_key},
                        concordUtils::toBigEndianStringBuffer(newEpoch));
  auto block_id = persistReconfigurationBlock(ver_updates, bft_seq_num, false);
  bftEngine::EpochManager::instance().setSelfEpochNumber(newEpoch);
  bftEngine::EpochManager::instance().setGlobalEpochNumber(newEpoch);
  LOG_INFO(GL, "Starting new epoch " << KVLOG(newEpoch, block_id));
  return block_id;
}
concord::messages::ClientStateReply KvbcClientReconfigurationHandler::buildClientStateReply(
    kvbc::keyTypes::CLIENT_COMMAND_TYPES command_type, uint32_t clientid) {
  concord::messages::ClientStateReply creply;
  creply.block_id = 0;
  auto res = ro_storage_.getLatest(
      kvbc::kConcordInternalCategoryId,
      std::string{kvbc::keyTypes::reconfiguration_client_data_prefix, static_cast<char>(command_type)} +
          std::to_string(clientid));
  if (res.has_value()) {
    std::visit(
        [&](auto&& arg) {
          auto strval = arg.data;
          std::vector<uint8_t> data_buf(strval.begin(), strval.end());
          switch (command_type) {
            case kvbc::keyTypes::CLIENT_COMMAND_TYPES::PUBLIC_KEY_EXCHANGE: {
              concord::messages::ClientExchangePublicKey cmd;
              concord::messages::deserialize(data_buf, cmd);
              creply.response = cmd;
              break;
            }
            case kvbc::keyTypes::CLIENT_COMMAND_TYPES::CLIENT_KEY_EXCHANGE_COMMAND: {
              concord::messages::ClientKeyExchangeCommand cmd;
              concord::messages::deserialize(data_buf, cmd);
              creply.response = cmd;
              break;
            }
            case kvbc::keyTypes::CLIENT_COMMAND_TYPES::CLIENT_SCALING_COMMAND: {
              concord::messages::ClientsAddRemoveCommand cmd;
              concord::messages::deserialize(data_buf, cmd);
              creply.response = cmd;
              break;
            }
            case kvbc::keyTypes::CLIENT_COMMAND_TYPES::CLIENT_SCALING_COMMAND_STATUS: {
              concord::messages::ClientsAddRemoveUpdateCommand cmd;
              concord::messages::deserialize(data_buf, cmd);
              creply.response = cmd;
              break;
            }
            default:
              break;
          }
          creply.block_id = arg.block_id;
        },
        *res);
  }
  return creply;
}
bool KvbcClientReconfigurationHandler::handle(const concord::messages::ClientReconfigurationStateRequest& command,
                                              uint64_t bft_seq_num,
                                              uint32_t sender_id,
                                              concord::messages::ReconfigurationResponse& rres) {
  concord::messages::ClientReconfigurationStateReply rep;
  for (uint8_t i = kvbc::keyTypes::CLIENT_COMMAND_TYPES::start_ + 1; i < kvbc::keyTypes::CLIENT_COMMAND_TYPES::end_;
       i++) {
    auto csrep = buildClientStateReply(static_cast<keyTypes::CLIENT_COMMAND_TYPES>(i), sender_id);
    if (csrep.block_id == 0) continue;
    rep.states.push_back(csrep);
  }
  LOG_INFO(GL, "b(2)");
  concord::messages::serialize(rres.additional_data, rep);
  return true;
}

bool KvbcClientReconfigurationHandler::handle(const concord::messages::ClientExchangePublicKey& command,
                                              uint64_t bft_seq_num,
                                              uint32_t sender_id,
                                              concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command,
      bft_seq_num,
      std::string{kvbc::keyTypes::reconfiguration_client_data_prefix,
                  static_cast<char>(kvbc::keyTypes::CLIENT_COMMAND_TYPES::PUBLIC_KEY_EXCHANGE)} +
          std::to_string(sender_id),
      false);
  LOG_INFO(getLogger(), "block id: " << blockId);
  return true;
}

bool KvbcClientReconfigurationHandler::handle(const concord::messages::ClientsAddRemoveUpdateCommand& command,
                                              uint64_t bft_seq_num,
                                              uint32_t sender_id,
                                              concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command,
      bft_seq_num,
      std::string{kvbc::keyTypes::reconfiguration_client_data_prefix,
                  static_cast<char>(kvbc::keyTypes::CLIENT_COMMAND_TYPES::CLIENT_SCALING_COMMAND_STATUS)} +
          std::to_string(sender_id),
      false);
  LOG_INFO(getLogger(), "block id: " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::ClientsAddRemoveStatusCommand&,
                                    uint64_t,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse& rres) {
  concord::messages::ClientsAddRemoveStatusResponse stats;
  for (const auto& gr : txKeysClientGroups_) {
    for (auto cid : gr) {
      std::string key =
          std::string{kvbc::keyTypes::reconfiguration_client_data_prefix,
                      static_cast<char>(kvbc::keyTypes::CLIENT_COMMAND_TYPES::CLIENT_SCALING_COMMAND_STATUS)} +
          std::to_string(cid);
      auto res = ro_storage_.getLatest(kvbc::kConcordInternalCategoryId, key);
      if (res.has_value()) {
        auto strval = std::visit([](auto&& arg) { return arg.data; }, *res);
        concord::messages::ClientsAddRemoveUpdateCommand cmd;
        std::vector<uint8_t> bytesval(strval.begin(), strval.end());
        concord::messages::deserialize(bytesval, cmd);

        LOG_INFO(getLogger(), "found scaling status for client" << KVLOG(cid, cmd.config_descriptor));
        stats.clients_status.push_back(std::make_pair(cid, cmd.config_descriptor));
      }
    }
  }
  rres.response = stats;
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::ClientKeyExchangeStatus&,
                                    uint64_t,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse& rres) {
  concord::messages::ClientKeyExchangeStatusResponse stats;
  for (const auto& gr : txKeysClientGroups_) {
    for (auto cid : gr) {
      std::string key = std::string{kvbc::keyTypes::reconfiguration_client_data_prefix,
                                    static_cast<char>(kvbc::keyTypes::CLIENT_COMMAND_TYPES::PUBLIC_KEY_EXCHANGE)} +
                        std::to_string(cid);
      auto res = ro_storage_.getLatest(kvbc::kConcordInternalCategoryId, key);
      if (res.has_value()) {
        auto strval = std::visit([](auto&& arg) { return arg.data; }, *res);
        concord::messages::ClientExchangePublicKey cmd;
        std::vector<uint8_t> bytesval(strval.begin(), strval.end());
        concord::messages::deserialize(bytesval, cmd);

        LOG_INFO(getLogger(), "found public key exchange status for client" << KVLOG(cid));
        stats.clients_keys.push_back(std::make_pair(cid, cmd));
      }
    }
  }
  rres.response = stats;
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::WedgeCommand& command,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_wedge_key}, false);
  LOG_INFO(getLogger(), "WedgeCommand block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::DownloadCommand& command,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_download_key}, false);
  LOG_INFO(getLogger(), "DownloadCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::InstallCommand& command,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_install_key}, false);
  LOG_INFO(getLogger(), "InstallCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::KeyExchangeCommand& command,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, sequence_number, std::string{kvbc::keyTypes::reconfiguration_key_exchange}, false);
  LOG_INFO(getLogger(), "KeyExchangeCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveCommand& command,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, sequence_number, std::string{kvbc::keyTypes::reconfiguration_add_remove}, false);
  LOG_INFO(getLogger(), "AddRemoveCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeCommand& command,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  concord::kvbc::categorization::VersionedUpdates ver_updates;
  ver_updates.addUpdate(std::string{kvbc::keyTypes::reconfiguration_add_remove, 0x1},
                        std::string(serialized_command.begin(), serialized_command.end()));
  auto epoch = bftEngine::EpochManager::instance().getSelfEpochNumber();
  ver_updates.addUpdate(std::string{keyTypes::reconfiguration_epoch_key}, concordUtils::toBigEndianStringBuffer(epoch));
  auto blockId = persistReconfigurationBlock(ver_updates, sequence_number, true);
  LOG_INFO(getLogger(), "AddRemove configuration command block is " << blockId);
  // update reserved pages for RO replica
  auto epochNum = bftEngine::EpochManager::instance().getSelfEpochNumber();
  auto wedgePoint = (sequence_number + 2 * checkpointWindowSize);
  wedgePoint = wedgePoint - (wedgePoint % checkpointWindowSize);
  concord::messages::ReconfigurationRequest rreqWithoutSignature;
  rreqWithoutSignature.command = command;
  bftEngine::ReconfigurationCmd::instance().saveReconfigurationCmdToResPages(
      rreqWithoutSignature,
      std::string{kvbc::keyTypes::reconfiguration_add_remove, 0x1},
      blockId,
      wedgePoint,
      epochNum);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::RestartCommand& command,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_restart_key}, true);
  LOG_INFO(getLogger(), "RestartCommand block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveStatus& command,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse& response) {
  auto res =
      ro_storage_.getLatest(kvbc::kConcordInternalCategoryId, std::string{kvbc::keyTypes::reconfiguration_add_remove});
  if (res.has_value()) {
    auto strval = std::visit([](auto&& arg) { return arg.data; }, *res);
    concord::messages::AddRemoveCommand cmd;
    std::vector<uint8_t> bytesval(strval.begin(), strval.end());
    concord::messages::deserialize(bytesval, cmd);
    concord::messages::AddRemoveStatusResponse addRemoveResponse;
    addRemoveResponse.reconfiguration = cmd.reconfiguration;
    LOG_INFO(getLogger(), "AddRemoveCommand response: " << addRemoveResponse.reconfiguration);
    response.response = std::move(addRemoveResponse);
  } else {
    concord::messages::ReconfigurationErrorMsg error_msg;
    error_msg.error_msg = "key_not_found";
    response.response = std::move(error_msg);
    LOG_INFO(getLogger(), "AddRemoveCommand key not found");
    return false;
  }
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeStatus& command,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse& response) {
  auto res = ro_storage_.getLatest(kvbc::kConcordInternalCategoryId,
                                   std::string{kvbc::keyTypes::reconfiguration_add_remove, 0x1});
  if (res.has_value()) {
    auto strval = std::visit([](auto&& arg) { return arg.data; }, *res);
    concord::messages::AddRemoveWithWedgeCommand cmd;
    std::vector<uint8_t> bytesval(strval.begin(), strval.end());
    concord::messages::deserialize(bytesval, cmd);
    concord::messages::AddRemoveWithWedgeStatusResponse addRemoveResponse;
    if (std::holds_alternative<concord::messages::AddRemoveWithWedgeStatusResponse>(response.response)) {
      addRemoveResponse = std::get<concord::messages::AddRemoveWithWedgeStatusResponse>(response.response);
    }
    addRemoveResponse.config_descriptor = cmd.config_descriptor;
    addRemoveResponse.restart_flag = cmd.restart;
    addRemoveResponse.bft_flag = cmd.bft_support;
    LOG_INFO(getLogger(), "AddRemoveWithWedgeCommand response: " << addRemoveResponse.config_descriptor);
    response.response = std::move(addRemoveResponse);
  } else {
    concord::messages::ReconfigurationErrorMsg error_msg;
    error_msg.error_msg = "key_not_found";
    response.response = std::move(error_msg);
    LOG_INFO(getLogger(), "AddRemoveWithWedgeCommand key not found");
    return false;
  }
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::PruneRequest& command,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, sequence_number, std::string{kvbc::keyTypes::reconfiguration_pruning_key, 0x1}, false);
  LOG_INFO(getLogger(), "PruneRequest configuration command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::ClientKeyExchangeCommand& command,
                                    uint64_t sequence_number,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse& response) {
  std::vector<uint32_t> target_clients;
  for (auto& cid : command.target_clients) {
    target_clients.push_back(cid);
  }
  if (target_clients.empty()) {
    LOG_INFO(getLogger(), "exchange client keys for all clients");
    // We don't want to assume anything about the CRE client id. Hence, we write the update to all clients.
    // However, only the CRE client will be able to execute the requests.
    for (const auto& cg : txKeysClientGroups_) {
      for (auto cid : cg) {
        target_clients.push_back(cid);
      }
    }
  }
  std::ostringstream oss;
  std::copy(target_clients.begin(), target_clients.end(), std::ostream_iterator<std::uint64_t>(oss, " "));
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto key_prefix = std::string{kvbc::keyTypes::reconfiguration_client_data_prefix,
                                static_cast<char>(kvbc::keyTypes::CLIENT_COMMAND_TYPES::CLIENT_KEY_EXCHANGE_COMMAND)};
  concord::kvbc::categorization::VersionedUpdates ver_updates;
  concord::messages::ClientKeyExchangeCommandResponse ckecr;
  for (auto clientid : target_clients) {
    ver_updates.addUpdate(key_prefix + std::to_string(clientid),
                          std::string(serialized_command.begin(), serialized_command.end()));
  }
  ckecr.block_id = persistReconfigurationBlock(ver_updates, sequence_number, false);
  LOG_INFO(getLogger(), "target clients: [" << oss.str() << "] block: " << ckecr.block_id);
  response.response = ckecr;
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::ClientsAddRemoveCommand& command,
                                    uint64_t sequence_number,
                                    uint32_t sender_id,
                                    concord::messages::ReconfigurationResponse& response) {
  std::vector<uint32_t> target_clients;
  // We don't want to assume anything about the CRE client id. Hence, we write the update to all clients.
  // However, only the CRE client will be able to execute the requests.
  for (const auto& cg : txKeysClientGroups_) {
    for (auto cid : cg) {
      target_clients.push_back(cid);
    }
  }
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto key_prefix = std::string{kvbc::keyTypes::reconfiguration_client_data_prefix,
                                static_cast<char>(kvbc::keyTypes::CLIENT_COMMAND_TYPES::CLIENT_SCALING_COMMAND)};
  concord::kvbc::categorization::VersionedUpdates ver_updates;
  for (auto clientid : target_clients) {
    ver_updates.addUpdate(key_prefix + std::to_string(clientid),
                          std::string(serialized_command.begin(), serialized_command.end()));
  }
  auto block_id = persistReconfigurationBlock(ver_updates, sequence_number, false);
  LOG_INFO(getLogger(), "ClientsAddRemoveCommand block_id is: " << block_id);
  return true;
}

bool ReconfigurationHandler::handle(const messages::UnwedgeCommand& cmd,
                                    uint64_t bft_seq_num,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse&) {
  if (!bftEngine::ControlStateManager::instance().getCheckpointToStopAt().has_value()) {
    LOG_INFO(getLogger(), "replica is already unwedge");
    return true;
  }
  LOG_INFO(getLogger(), "Unwedge command started " << KVLOG(cmd.bft_support));
  auto curr_epoch = bftEngine::EpochManager::instance().getSelfEpochNumber();
  auto quorum_size = bftEngine::ReplicaConfig::instance().numReplicas;
  if (cmd.bft_support)
    quorum_size = 2 * bftEngine::ReplicaConfig::instance().fVal + bftEngine::ReplicaConfig::instance().cVal + 1;
  uint32_t valid_sigs{0};
  for (auto const& [id, unwedge_stat] : cmd.unwedges) {
    if (unwedge_stat.curr_epoch < curr_epoch) continue;
    std::string sig_data = std::to_string(id) + std::to_string(unwedge_stat.curr_epoch);
    auto& sig = unwedge_stat.signature;
    std::string signature(sig.begin(), sig.end());
    bool valid = bftEngine::impl::SigManager::instance()->verifySig(
        id, sig_data.c_str(), sig_data.size(), signature.data(), signature.size());
    if (valid) valid_sigs++;
  }
  LOG_INFO(getLogger(), "verified " << valid_sigs << " unwedge signatures, required quorum is " << quorum_size);
  bool can_unwedge = (valid_sigs >= quorum_size);
  if (can_unwedge) {
    if (!cmd.restart) {
      persistNewEpochBlock(bft_seq_num);
      bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(0);
      bftEngine::IControlHandler::instance()->resetState();
      LOG_INFO(getLogger(), "Unwedge command completed successfully");
    } else {
      bftEngine::EpochManager::instance().setNewEpochFlag(true);
      bftEngine::ControlStateManager::instance().restart();
    }
  }
  return can_unwedge;
}

bool ReconfigurationHandler::handle(const messages::UnwedgeStatusRequest& req,
                                    uint64_t,
                                    uint32_t,
                                    concord::messages::ReconfigurationResponse& rres) {
  concord::messages::UnwedgeStatusResponse response;
  response.replica_id = bftEngine::ReplicaConfig::instance().replicaId;
  if (bftEngine::ControlStateManager::instance().getCheckpointToStopAt().has_value()) {
    if ((!req.bft_support && !bftEngine::IControlHandler::instance()->isOnNOutOfNCheckpoint()) ||
        (req.bft_support && !bftEngine::IControlHandler::instance()->isOnStableCheckpoint())) {
      response.can_unwedge = false;
      response.reason = "replica is not at wedge point";
      rres.response = response;
      return true;
    }
  }
  auto curr_epoch = bftEngine::EpochManager::instance().getSelfEpochNumber();
  std::string sig_data =
      std::to_string(bftEngine::ReplicaConfig::instance().getreplicaId()) + std::to_string(curr_epoch);
  auto sig_manager = bftEngine::impl::SigManager::instance();
  std::string sig(sig_manager->getMySigLength(), '\0');
  sig_manager->sign(sig_data.c_str(), sig_data.size(), sig.data(), sig.size());
  response.can_unwedge = true;
  response.curr_epoch = curr_epoch;
  response.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  LOG_INFO(getLogger(), "Replica is ready to unwedge " << KVLOG(curr_epoch));
  rres.response = response;
  return true;
}

bool InternalKvReconfigurationHandler::verifySignature(uint32_t sender_id,
                                                       const std::string& data,
                                                       const std::string& signature) const {
  if (sender_id >= bftEngine::ReplicaConfig::instance().numReplicas) return false;
  return bftEngine::impl::SigManager::instance()->verifySig(
      sender_id, data.data(), data.size(), signature.data(), signature.size());
}

bool InternalKvReconfigurationHandler::handle(const concord::messages::WedgeCommand& command,
                                              uint64_t bft_seq_num,
                                              uint32_t,
                                              concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  if (command.noop) {
    auto seq_num_to_stop_at = bftEngine::ControlStateManager::instance().getCheckpointToStopAt();
    if (!seq_num_to_stop_at.has_value() || bft_seq_num > seq_num_to_stop_at) {
      LOG_ERROR(getLogger(), "Invalid noop wedge command, it won't be writen to the blockchain");
      return false;
    }
    auto blockId = persistReconfigurationBlock(
        serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_wedge_key, 0x1}, false);
    LOG_INFO(getLogger(), "received noop command, a new block will be written" << KVLOG(bft_seq_num, blockId));
    return true;
  }
  return false;
}

bool InternalPostKvReconfigurationHandler::handle(const concord::messages::ClientExchangePublicKey& command,
                                                  uint64_t sequence_number,
                                                  uint32_t,
                                                  concord::messages::ReconfigurationResponse& response) {
  concord::kvbc::categorization::VersionedUpdates ver_updates;
  auto updated_client_keys = SigManager::instance()->getClientsPublicKeys();

  ver_updates.addUpdate(std::string(1, concord::kvbc::kClientsPublicKeys), std::string(updated_client_keys));
  auto id = persistReconfigurationBlock(ver_updates, sequence_number, false);
  LOG_INFO(getLogger(),
           "Writing client keys to block [" << id << "] after key exchange, keys "
                                            << std::hash<std::string>{}(updated_client_keys));
  return true;
}

}  // namespace concord::kvbc::reconfiguration
