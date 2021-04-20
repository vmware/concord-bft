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
class ReconfigurationHandler : public concord::reconfiguration::IReconfigurationHandler {
 public:
  ReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : blocks_adder_{block_adder}, block_metadata_{ro_storage} {}
  bool handle(const concord::messages::WedgeCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationErrorMsg&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, bft_seq_num, kvbc::keyTypes::reconfiguration_wedge_key);
    LOG_INFO(getLogger(), "WedgeCommand block is " << blockId);
    return true;
  }
  bool handle(const concord::messages::WedgeStatusRequest&,
              concord::messages::WedgeStatusResponse&,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::GetVersionCommand&,
              concord::messages::GetVersionResponse&,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::DownloadCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationErrorMsg&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, bft_seq_num, kvbc::keyTypes::reconfiguration_download_key);
    LOG_INFO(getLogger(), "DownloadCommand command block is " << blockId);
    return true;
  }
  bool handle(const concord::messages::DownloadStatusCommand&,
              concord::messages::DownloadStatus&,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::InstallCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationErrorMsg&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, bft_seq_num, kvbc::keyTypes::reconfiguration_install_key);
    LOG_INFO(getLogger(), "InstallCommand command block is " << blockId);
    return true;
  }
  bool handle(const concord::messages::InstallStatusCommand&,
              concord::messages::InstallStatusResponse&,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool verifySignature(const concord::messages::ReconfigurationRequest&,
                       concord::messages::ReconfigurationErrorMsg&) const override {
    return true;
  }

  bool handle(const concord::messages::KeyExchangeCommand& command,
              concord::messages::ReconfigurationErrorMsg&,
              uint64_t sequence_number) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, sequence_number, kvbc::keyTypes::reconfiguration_key_exchange);
    LOG_INFO(getLogger(), "KeyExchangeCommand command block is " << blockId);
    return true;
  }

  bool handle(const concord::messages::AddRemoveCommand& command,
              concord::messages::ReconfigurationErrorMsg&,
              uint64_t sequence_number) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId =
        persistReconfigurationBlock(serialized_command, sequence_number, kvbc::keyTypes::reconfiguration_add_remove);
    LOG_INFO(getLogger(), "AddRemoveCommand command block is " << blockId);
    return true;
  }

 private:
  kvbc::IBlockAdder& blocks_adder_;
  BlockMetadata block_metadata_;

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
      LOG_ERROR(getLogger(), "Reconfiguration Handler failed to persist last agreed prunable block ID");
      throw;
    }
  }

  logging::Logger getLogger() const {
    static logging::Logger logger_(logging::getLogger("concord.kvbc.reconfiguration"));
    return logger_;
  }
};
}  // namespace concord::kvbc::reconfiguration
