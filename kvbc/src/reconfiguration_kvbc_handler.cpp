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
#include "endianness.hpp"

namespace concord::kvbc::reconfiguration {

kvbc::BlockId ReconfigurationBlockTools::persistReconfigurationBlock(const std::vector<uint8_t>& data,
                                                                     const uint64_t bft_seq_num,
                                                                     string key) {
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
    concord::kvbc::categorization::VersionedUpdates& ver_updates, const uint64_t bft_seq_num) {
  // All blocks are expected to have the BFT sequence number as a key.
  ver_updates.addUpdate(std::string{kvbc::keyTypes::bft_seq_num_key}, block_metadata_.serialize(bft_seq_num));

  concord::kvbc::categorization::Updates updates;
  updates.add(kvbc::kConcordInternalCategoryId, std::move(ver_updates));
  try {
    return blocks_adder_.add(std::move(updates));
  } catch (const std::exception& e) {
    LOG_ERROR(GL, "failed to persist the reconfiguration block: " << e.what());
    throw;
  }
}

concord::messages::ClientReconfigurationStateReply KvbcClientReconfigurationHandler::buildClientStateReply(
    kvbc::BlockId blockid, kvbc::keyTypes::CLIENT_COMMAND_TYPES command_type, uint32_t clientid) {
  concord::messages::ClientReconfigurationStateReply creply;
  creply.block_id = 0;
  if (blockid > 0) {
    creply.block_id = blockid;
    auto res = ro_storage_.get(
        kvbc::kConcordInternalCategoryId,
        std::string{kvbc::keyTypes::reconfiguration_client_data_prefix, static_cast<char>(command_type)} +
            std::to_string(clientid),
        blockid);
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
            }
            default:
              break;
          }
        },
        *res);
  }
  return creply;
}
bool KvbcClientReconfigurationHandler::handle(const concord::messages::ClientReconfigurationStateRequest& command,
                                              uint64_t bft_seq_num,
                                              concord::messages::ReconfigurationResponse& rres) {
  // We want to rotate over the latest updates of this client and find the earliest one which is higher than the client
  // last known block
  kvbc::BlockId minKnownUpdate{0};
  uint8_t command_type = 0;
  for (uint8_t i = kvbc::keyTypes::CLIENT_COMMAND_TYPES::start_ + 1; i < kvbc::keyTypes::CLIENT_COMMAND_TYPES::end_;
       i++) {
    auto key = std::string{kvbc::keyTypes::reconfiguration_client_data_prefix, static_cast<char>(i)} +
               std::to_string(command.sender_id);
    auto res = ro_storage_.getLatestVersion(kvbc::kConcordInternalCategoryId, key);
    if (res.has_value()) {
      auto blockid = res.value().version;
      if (blockid > command.last_known_block && (minKnownUpdate == 0 || minKnownUpdate > blockid)) {
        minKnownUpdate = blockid;
        command_type = i;
      }
      LOG_INFO(getLogger(), "found a client update on chain " << KVLOG(command.sender_id, i, blockid));
    }
  }
  auto creply = buildClientStateReply(
      minKnownUpdate, static_cast<keyTypes::CLIENT_COMMAND_TYPES>(command_type), command.sender_id);
  concord::messages::serialize(rres.additional_data, creply);
  return true;
}

bool KvbcClientReconfigurationHandler::handle(const concord::messages::ClientExchangePublicKey& command,
                                              uint64_t bft_seq_num,
                                              concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command,
      bft_seq_num,
      std::string{kvbc::keyTypes::reconfiguration_client_data_prefix,
                  static_cast<char>(kvbc::keyTypes::CLIENT_COMMAND_TYPES::PUBLIC_KEY_EXCHANGE)} +
          std::to_string(command.sender_id));
  LOG_INFO(getLogger(), "block id: " << blockId);
  return true;
}
bool KvbcClientReconfigurationHandler::handle(const concord::messages::ClientReconfigurationLastUpdate& command,
                                              uint64_t,
                                              concord::messages::ReconfigurationResponse& rres) {
  // We want to rotate over the latest updates of this client and find the latest one in the blockchain
  kvbc::BlockId maxKnownUpdate{0};
  uint8_t command_type = 0;
  for (uint8_t i = kvbc::keyTypes::CLIENT_COMMAND_TYPES::start_ + 1; i < kvbc::keyTypes::CLIENT_COMMAND_TYPES::end_;
       i++) {
    auto key = std::string{kvbc::keyTypes::reconfiguration_client_data_prefix, static_cast<char>(i)} +
               std::to_string(command.sender_id);
    auto res = ro_storage_.getLatestVersion(kvbc::kConcordInternalCategoryId, key);
    if (res.has_value()) {
      auto blockid = res.value().version;
      if (maxKnownUpdate < blockid) {
        maxKnownUpdate = blockid;
        command_type = i;
      }
      LOG_INFO(getLogger(), "found a client update on chain " << KVLOG(command.sender_id, i, blockid));
    }
  }
  auto creply = buildClientStateReply(
      maxKnownUpdate, static_cast<keyTypes::CLIENT_COMMAND_TYPES>(command_type), command.sender_id);
  concord::messages::serialize(rres.additional_data, creply);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::WedgeCommand& command,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_wedge_key});
  LOG_INFO(getLogger(), "WedgeCommand block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::DownloadCommand& command,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_download_key});
  LOG_INFO(getLogger(), "DownloadCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::InstallCommand& command,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_install_key});
  LOG_INFO(getLogger(), "InstallCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::KeyExchangeCommand& command,
                                    uint64_t sequence_number,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, sequence_number, std::string{kvbc::keyTypes::reconfiguration_key_exchange});
  LOG_INFO(getLogger(), "KeyExchangeCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveCommand& command,
                                    uint64_t sequence_number,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, sequence_number, std::string{kvbc::keyTypes::reconfiguration_add_remove});
  LOG_INFO(getLogger(), "AddRemoveCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeCommand& command,
                                    uint64_t sequence_number,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, sequence_number, std::string{kvbc::keyTypes::reconfiguration_add_remove, 0x1});
  LOG_INFO(getLogger(), "AddRemove configuration command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::RestartCommand& command,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_restart_key});
  LOG_INFO(getLogger(), "RestartCommand block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveStatus& command,
                                    uint64_t sequence_number,
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
                                    concord::messages::ReconfigurationResponse& response) {
  auto res = ro_storage_.getLatest(kvbc::kConcordInternalCategoryId,
                                   std::string{kvbc::keyTypes::reconfiguration_add_remove, 0x1});
  if (res.has_value()) {
    auto strval = std::visit([](auto&& arg) { return arg.data; }, *res);
    concord::messages::AddRemoveWithWedgeCommand cmd;
    std::vector<uint8_t> bytesval(strval.begin(), strval.end());
    concord::messages::deserialize(bytesval, cmd);
    concord::messages::AddRemoveWithWedgeStatusResponse addRemoveResponse;
    addRemoveResponse.config_descriptor = cmd.config_descriptor;
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
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(
      serialized_command, sequence_number, std::string{kvbc::keyTypes::reconfiguration_pruning_key, 0x1});
  LOG_INFO(getLogger(), "PruneRequest configuration command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::ClientKeyExchangeCommand& command,
                                    uint64_t sequence_number,
                                    concord::messages::ReconfigurationResponse& response) {
  if (command.target_clients.empty()) {
    return true;
  }
  std::ostringstream oss;
  std::copy(
      command.target_clients.begin(), command.target_clients.end(), std::ostream_iterator<std::uint64_t>(oss, " "));
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto key_prefix = std::string{kvbc::keyTypes::reconfiguration_client_data_prefix,
                                static_cast<char>(kvbc::keyTypes::CLIENT_COMMAND_TYPES::CLIENT_KEY_EXCHANGE_COMMAND)};
  concord::kvbc::categorization::VersionedUpdates ver_updates;
  concord::messages::ClientKeyExchangeCommandResponse ckecr;
  for (auto clientid : command.target_clients) {
    ver_updates.addUpdate(key_prefix + std::to_string(clientid),
                          std::string(serialized_command.begin(), serialized_command.end()));
  }
  ckecr.block_id = persistReconfigurationBlock(ver_updates, sequence_number);
  LOG_INFO(getLogger(), "target clients: [" << oss.str() << "] block: " << ckecr.block_id);
  response.response = ckecr;
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
        serialized_command, bft_seq_num, std::string{kvbc::keyTypes::reconfiguration_wedge_key, 0x1});
    LOG_INFO(getLogger(), "received noop command, a new block will be written" << KVLOG(bft_seq_num, blockId));
    return true;
  }
  return false;
}
}  // namespace concord::kvbc::reconfiguration
