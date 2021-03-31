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

namespace concord::kvbc::reconfiguration {
class ReconfigurationHandler : public concord::reconfiguration::IReconfigurationHandler {
 public:
  ReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : blocks_adder_{block_adder}, block_metadata_{ro_storage} {}
  bool handle(const concord::messages::WedgeCommand& command, concord::messages::ReconfigurationErrorMsg&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId = persistReconfigurationBlock(serialized_command, command.bft_seq_num, reconfiguration_key_prefix + 1);
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
  bool handle(const concord::messages::DownloadCommand& command, concord::messages::ReconfigurationErrorMsg&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId = persistReconfigurationBlock(serialized_command, command.bft_seq_num, reconfiguration_key_prefix + 2);
    LOG_INFO(getLogger(), "DownloadCommand command block is " << blockId);
    return true;
  }
  bool handle(const concord::messages::DownloadStatusCommand&,
              concord::messages::DownloadStatus&,
              concord::messages::ReconfigurationErrorMsg&) override {
    return true;
  }
  bool handle(const concord::messages::InstallCommand& command,
              uint64_t,
              concord::messages::ReconfigurationErrorMsg&) override {
    std::vector<uint8_t> serialized_command;
    concord::messages::serialize(serialized_command, command);
    auto blockId = persistReconfigurationBlock(serialized_command, command.bft_seq_num, reconfiguration_key_prefix + 3);
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

  static const char bft_seq_num_key_ = 0x21;
  static const char reconfiguration_key_prefix = 0x25;

 private:
  kvbc::IBlockAdder& blocks_adder_;
  BlockMetadata block_metadata_;

  kvbc::BlockId persistReconfigurationBlock(const std::vector<uint8_t>& data,
                                            const uint64_t bft_seq_num,
                                            const char& key) {
    concord::kvbc::categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string{reconfiguration_key_prefix, key}, std::string(data.begin(), data.end()));

    // All blocks are expected to have the BFT sequence number as a key.
    ver_updates.addUpdate(std::string{bft_seq_num_key_}, block_metadata_.serialize(bft_seq_num));

    concord::kvbc::categorization::Updates updates;
    updates.add(kvbc::kConcordInternalCategoryId, std::move(ver_updates));
    try {
      return blocks_adder_.add(std::move(updates));
    } catch (...) {
      throw std::runtime_error{"Reconfiguration Handler failed to persist last agreed prunable block ID"};
    }
  }

  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.kvbc.reconfiguration"));
    return logger_;
  }
};
}  // namespace concord::kvbc::reconfiguration
