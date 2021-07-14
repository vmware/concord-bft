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
#include "reconfiguration/reconfiguration_handler.hpp"

namespace concord::kvbc::reconfiguration {
class ReconfigurationBlockTools {
 protected:
  ReconfigurationBlockTools(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : blocks_adder_{block_adder}, block_metadata_{ro_storage}, ro_storage_{ro_storage} {}
  kvbc::BlockId persistReconfigurationBlock(const std::vector<uint8_t>& data,
                                            const uint64_t bft_seq_num,
                                            string key,
                                            bool include_epoch);
  kvbc::BlockId persistReconfigurationBlock(concord::kvbc::categorization::VersionedUpdates& ver_updates,
                                            const uint64_t bft_seq_num);
  kvbc::IBlockAdder& blocks_adder_;
  BlockMetadata block_metadata_;
  kvbc::IReader& ro_storage_;
};

/*
 * This component is responsible for logging a reconfiguration requests (issued by a specific bft CRE) to the blockchain
 */
class KvbcClientReconfigurationHandler : public concord::reconfiguration::ClientReconfigurationHandler,
                                         public ReconfigurationBlockTools {
 public:
  KvbcClientReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : ReconfigurationBlockTools{block_adder, ro_storage} {}
  bool handle(const concord::messages::ClientExchangePublicKey&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::ClientReconfigurationLastUpdate&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::ClientReconfigurationStateRequest&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override;

 private:
  concord::messages::ClientReconfigurationStateReply buildClientStateReply(
      kvbc::BlockId, kvbc::keyTypes::CLIENT_COMMAND_TYPES command_type, uint32_t clientid);
};
/**
 * This component is reposnsible for logging reconfiguraiton request (issued by an autherated operator) in the
 * blockchian.
 */
class ReconfigurationHandler : public concord::reconfiguration::BftReconfigurationHandler,
                               public ReconfigurationBlockTools {
 public:
  ReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : ReconfigurationBlockTools{block_adder, ro_storage} {}
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
};
/**
 * This component is reposnsible for logging internal reconfiguration requests to the blockchain (such as noop commands)
 */
class InternalKvReconfigurationHandler : public concord::reconfiguration::IReconfigurationHandler,
                                         public ReconfigurationBlockTools {
 public:
  InternalKvReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : ReconfigurationBlockTools{block_adder, ro_storage} {}
  bool verifySignature(uint32_t sender_id, const std::string& data, const std::string& signature) const override;

  bool handle(const concord::messages::WedgeCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override;
};
}  // namespace concord::kvbc::reconfiguration
