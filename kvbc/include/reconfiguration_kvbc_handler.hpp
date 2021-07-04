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
#include "SigManager.hpp"
#include "../../reconfiguration/include/reconfiguration/reconfiguration_handler.hpp"

namespace concord::kvbc::reconfiguration {
class KvbcClientReconfigurationHandler : public concord::reconfiguration::ClientReconfigurationHandler {
 public:
  bool handle(const concord::messages::ClientExchangePublicKey&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::ClientReconfigurationLastUpdate&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override;
};
/**
 * TODO [YB] - add description
 */
class ReconfigurationHandler : public concord::reconfiguration::BftReconfigurationHandler {
 public:
  ReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : blocks_adder_{block_adder}, block_metadata_{ro_storage}, ro_storage_{ro_storage} {}
  bool handle(const concord::messages::WedgeCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::DownloadCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::InstallCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::KeyExchangeCommand& command,
              uint64_t sequence_number,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::AddRemoveCommand& command,
              uint64_t sequence_number,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::AddRemoveWithWedgeCommand& command,
              uint64_t sequence_number,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::AddRemoveStatus& command,
              uint64_t sequence_number,
              concord::messages::ReconfigurationResponse& response) override;

  bool handle(const concord::messages::AddRemoveWithWedgeStatus& command,
              uint64_t sequence_number,
              concord::messages::ReconfigurationResponse& response) override;

  bool handle(const concord::messages::PruneRequest& command,
              uint64_t sequence_number,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::ClientKeyExchangeCommand& command,
              uint64_t sequence_number,
              concord::messages::ReconfigurationResponse& response) override;

 protected:
  kvbc::BlockId persistReconfigurationBlock(const std::vector<uint8_t>& data, const uint64_t bft_seq_num, string key);

 private:
  kvbc::IBlockAdder& blocks_adder_;
  BlockMetadata block_metadata_;
  kvbc::IReader& ro_storage_;
};
/**
 * TODO [YB] - add description
 */
class InternalKvReconfigurationHandler : public concord::kvbc::reconfiguration::ReconfigurationHandler {
 public:
  InternalKvReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : concord::kvbc::reconfiguration::ReconfigurationHandler{block_adder, ro_storage} {}
  bool verifySignature(uint32_t sender_id, const std::string& data, const std::string& signature) const override;

  bool handle(const concord::messages::WedgeCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override;
};
}  // namespace concord::kvbc::reconfiguration
