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

#include "reconfiguration_add_block_handler.hpp"
#include "bftengine/ControlStateManager.hpp"
#include "bftengine/EpochsManager.hpp"
#include "endianness.hpp"
namespace concord::kvbc::reconfiguration {
kvbc::BlockId persistReconfigurationBlock(kvbc::IBlockAdder& blocks_adder,
                                          BlockMetadata& block_metadata,
                                          const std::vector<uint8_t>& data,
                                          const uint64_t bft_seq_num,
                                          string key,
                                          bool include_epoch = false) {
  concord::kvbc::categorization::VersionedUpdates ver_updates;
  ver_updates.addUpdate(std::move(key), std::string(data.begin(), data.end()));

  // All blocks are expected to have the BFT sequence number as a key.
  ver_updates.addUpdate(std::string{kvbc::keyTypes::bft_seq_num_key}, block_metadata.serialize(bft_seq_num));

  if (include_epoch) {
    // All replicas should have the very same epoch on the same sequence number execution, hence it is safe to write
    // the self epoch number
    auto curr_epoch = bftEngine::EpochManager::instance().getSelfEpoch();
    LOG_INFO(GL, "writing new global epoch number " << KVLOG(bft_seq_num, curr_epoch));
    ver_updates.addUpdate(std::string{kvbc::keyTypes::reconfiguration_epoch_prefix},
                          concordUtils::toBigEndianStringBuffer(curr_epoch));
  }
  concord::kvbc::categorization::Updates updates;
  updates.add(kvbc::kConcordInternalCategoryId, std::move(ver_updates));
  try {
    return blocks_adder.add(std::move(updates));
  } catch (...) {
    LOG_FATAL(GL, "Reconfiguration Handler failed to persist the reconfiguration block");
  }
  return 0;
}
bool ReconfigurationHandler::handle(const concord::messages::WedgeCommand& command,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(blocks_adder_,
                                             block_metadata_,
                                             serialized_command,
                                             bft_seq_num,
                                             std::string{kvbc::keyTypes::reconfiguration_wedge_key});
  LOG_INFO(getLogger(), "WedgeCommand block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::DownloadCommand& command,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(blocks_adder_,
                                             block_metadata_,
                                             serialized_command,
                                             bft_seq_num,
                                             std::string{kvbc::keyTypes::reconfiguration_download_key});
  LOG_INFO(getLogger(), "DownloadCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::InstallCommand& command,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(blocks_adder_,
                                             block_metadata_,
                                             serialized_command,
                                             bft_seq_num,
                                             std::string{kvbc::keyTypes::reconfiguration_install_key});
  LOG_INFO(getLogger(), "InstallCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::KeyExchangeCommand& command,
                                    uint64_t sequence_number,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(blocks_adder_,
                                             block_metadata_,
                                             serialized_command,
                                             sequence_number,
                                             std::string{kvbc::keyTypes::reconfiguration_key_exchange});
  LOG_INFO(getLogger(), "KeyExchangeCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveCommand& command,
                                    uint64_t sequence_number,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(blocks_adder_,
                                             block_metadata_,
                                             serialized_command,
                                             sequence_number,
                                             std::string{kvbc::keyTypes::reconfiguration_add_remove});
  LOG_INFO(getLogger(), "AddRemoveCommand command block is " << blockId);
  return true;
}

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveWithWedgeCommand& command,
                                    uint64_t sequence_number,
                                    concord::messages::ReconfigurationResponse&) {
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId = persistReconfigurationBlock(blocks_adder_,
                                             block_metadata_,
                                             serialized_command,
                                             sequence_number,
                                             std::string{kvbc::keyTypes::reconfiguration_add_remove, 0x1},
                                             true);
  LOG_INFO(getLogger(), "AddRemove configuration command block is " << blockId);

  // From now on, we are going to start wedge process, at the end of this process we will conisdered
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
auto blockId = persistReconfigurationBlock(blocks_adder_,
                                           block_metadata_,
                                           serialized_command,
                                           sequence_number,
                                           std::string{kvbc::keyTypes::reconfiguration_pruning_key, 0x1});
LOG_INFO(getLogger(), "PruneRequest configuration command block is " << blockId);
return true;
}
bool InternalKvReconfigurationHandler::verifySignature(const std::string& data, const std::string& signature) const {
  bool valid = false;
  for (auto& verifier : internal_verifiers_) {
    valid |= verifier->verify(data.c_str(), data.size(), signature.c_str(), signature.size());
    if (valid) break;
  }
  return valid;
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
    auto blockId = persistReconfigurationBlock(blocks_adder_,
                                               block_metadata_,
                                               serialized_command,
                                               bft_seq_num,
                                               std::string{kvbc::keyTypes::reconfiguration_wedge_key, 0x1});
    LOG_INFO(getLogger(), "received noop command, a new block will be written" << KVLOG(bft_seq_num, blockId));
    return true;
  }
  return false;
}

bool InternalKvReconfigurationHandler::handle(const concord::messages::EpochUpdateMsg& command,
                                              uint64_t bft_seq_num,
                                              concord::messages::ReconfigurationResponse&) {
  auto source = command.replica_id;
  std::vector<uint8_t> serialized_command;
  concord::messages::serialize(serialized_command, command);
  auto blockId =
      persistReconfigurationBlock(blocks_adder_,
                                  block_metadata_,
                                  serialized_command,
                                  bft_seq_num,
                                  std::string{kvbc::keyTypes::reconfiguration_epoch_prefix, static_cast<char>(source)});
  LOG_INFO(getLogger(),
           "received new epoch message, a new block will be written" << KVLOG(source, bft_seq_num, blockId));
  // update Epoch manager with the new state
  for (uint32_t i = 0; i < num_replicas_; i++) {
    auto val = ro_storage_.getLatest(kConcordInternalCategoryId,
                                     std::string{kvbc::keyTypes::reconfiguration_epoch_prefix, static_cast<char>(i)});
    uint64_t epoch = 0;
    if (val.has_value()) {
      auto strval = std::visit([](auto&& arg) { return arg.data; }, *val);
      std::vector<uint8_t> data(strval.begin(), strval.end());
      concord::messages::EpochUpdateMsg epoch_msg;
      concord::messages::deserialize(data, epoch_msg);
      epoch = epoch_msg.epoch_number;
      bftEngine::EpochManager::instance().updateEpochForReplica(i, epoch);
    }
  }
  bftEngine::EpochManager::instance().save();
  return true;
}
}  // namespace concord::kvbc::reconfiguration
