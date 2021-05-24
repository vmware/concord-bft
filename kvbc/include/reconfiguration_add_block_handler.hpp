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

#pragma once

#include "reconfiguration/ireconfiguration.hpp"
#include "db_interfaces.h"
#include "hex_tools.h"
#include "block_metadata.hpp"
#include "kvbc_key_types.hpp"

namespace concord::kvbc::reconfiguration {
class ReconfigurationHandler : public concord::reconfiguration::BftReconfigurationHandler {
 public:
  ReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : blocks_adder_{block_adder}, block_metadata_{ro_storage} {}
  bool handle(const concord::messages::WedgeCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, bft_seq_num, kvbc::keyTypes::reconfiguration_wedge_key);
    LOG_INFO(getLogger(), "WedgeCommand block is " << blockId);
    return true;
  }

  bool handle(const concord::messages::DownloadCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, bft_seq_num, kvbc::keyTypes::reconfiguration_download_key);
    LOG_INFO(getLogger(), "DownloadCommand command block is " << blockId);
    return true;
  }

  bool handle(const concord::messages::InstallCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, bft_seq_num, kvbc::keyTypes::reconfiguration_install_key);
    LOG_INFO(getLogger(), "InstallCommand command block is " << blockId);
    return true;
  }

  bool handle(const concord::messages::KeyExchangeCommand& command,
              uint64_t sequence_number,
              concord::messages::ReconfigurationResponse&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, sequence_number, kvbc::keyTypes::reconfiguration_key_exchange);
    LOG_INFO(getLogger(), "KeyExchangeCommand command block is " << blockId);
    return true;
  }

  bool handle(const concord::messages::AddRemoveCommand& command,
              uint64_t sequence_number,
              concord::messages::ReconfigurationResponse&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, sequence_number, kvbc::keyTypes::reconfiguration_add_remove);
    LOG_INFO(getLogger(), "AddRemoveCommand command block is " << blockId);
    return true;
  }

 protected:
  kvbc::BlockId persistReconfigurationBlock(const std::vector<uint8_t>& data,
                                            const uint64_t bft_seq_num,
                                            const char& key) {
    concord::kvbc::categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string{key}, std::string(data.begin(), data.end()));

    // All blocks are expected to have the BFT sequence number as a key.
    ver_updates.addUpdate(std::string{kvbc::keyTypes::bft_seq_num_key}, block_metadata_.serialize(bft_seq_num));

    concord::kvbc::categorization::Updates updates;
    updates.add(kvbc::kConcordInternalCategoryId, std::move(ver_updates));
    try {
      return blocks_adder_.add(std::move(updates));
    } catch (...) {
      LOG_ERROR(getLogger(), "Reconfiguration Handler failed to persist the reconfiguration block");
      throw;
    }
  }

 private:
  kvbc::IBlockAdder& blocks_adder_;
  BlockMetadata block_metadata_;
};
class InternalKvReconfigurationHandler : public concord::kvbc::reconfiguration::ReconfigurationHandler {
 public:
  InternalKvReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : concord::kvbc::reconfiguration::ReconfigurationHandler{block_adder, ro_storage} {
    for (const auto& [rep, pk] : bftEngine::ReplicaConfig::instance().publicKeysOfReplicas) {
      internal_verifiers_.emplace_back(std::make_unique<bftEngine::impl::RSAVerifier>(pk.c_str()));
      (void)rep;
    }
  }
  bool verifySignature(const concord::messages::ReconfigurationRequest& request,
                       concord::messages::ReconfigurationResponse&) const override {
    bool valid = false;
    concord::messages::ReconfigurationErrorMsg error_msg;
    concord::messages::ReconfigurationRequest request_without_sig = request;
    request_without_sig.signature = {};
    std::vector<uint8_t> serialized_cmd;
    concord::messages::serialize(serialized_cmd, request_without_sig);

    auto ser_data = std::string(serialized_cmd.begin(), serialized_cmd.end());
    auto ser_sig = std::string(request.signature.begin(), request.signature.end());

    if (!request.additional_data.empty() && request.additional_data.front() == internalCommandKey()) {
      // It means we got an internal command, lets verify it with the internal verifiers
      for (auto& verifier : internal_verifiers_) {
        valid |= verifier->verify(ser_data.c_str(), ser_data.size(), ser_sig.c_str(), ser_sig.size());
        if (valid) break;
      }
    }
    return valid;
  }

  bool handle(const concord::messages::WedgeCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    if (command.noop) {
      auto seq_num_to_stop_at = bftEngine::ControlStateManager::instance().getCheckpointToStopAt();
      if (!seq_num_to_stop_at.has_value() || bft_seq_num > seq_num_to_stop_at) {
        LOG_ERROR(getLogger(), "Invalid noop wedge command, it won't be writen to the blockchain");
        return false;
      }
      auto blockId =
          persistReconfigurationBlock(serialized_command, bft_seq_num, kvbc::keyTypes::reconfiguration_wedge_noop_key);
      LOG_INFO(getLogger(), "received noop command, a new block will be written" << KVLOG(bft_seq_num, blockId));
      return true;
    }
    return false;
  }

 private:
  std::vector<std::unique_ptr<bftEngine::impl::RSAVerifier>> internal_verifiers_;
};
}  // namespace concord::kvbc::reconfiguration
