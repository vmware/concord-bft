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

#include "reconfiguration/reconfiguration_handler.hpp"
#include "db_interfaces.h"
#include "hex_tools.h"
#include "block_metadata.hpp"
#include "kvbc_key_types.hpp"

namespace concord::kvbc::reconfiguration {

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

 private:
  kvbc::IBlockAdder& blocks_adder_;
  BlockMetadata block_metadata_;
  kvbc::IReader& ro_storage_;
};
class InternalKvReconfigurationHandler : public concord::reconfiguration::IReconfigurationHandler {
 public:
  InternalKvReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage, uint32_t num_replicas)
      : blocks_adder_{block_adder}, block_metadata_{ro_storage}, ro_storage_{ro_storage}, num_replicas_(num_replicas) {
    (void)ro_storage_;
    for (const auto& [rep, pk] : bftEngine::ReplicaConfig::instance().publicKeysOfReplicas) {
      internal_verifiers_.emplace_back(std::make_unique<bftEngine::impl::RSAVerifier>(pk.c_str()));
      (void)rep;
    }
  }
  bool verifySignature(const std::string& data, const std::string& signature) const override;

  bool handle(const concord::messages::WedgeCommand& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::EpochUpdateMsg& command,
              uint64_t bft_seq_num,
              concord::messages::ReconfigurationResponse&) override;

  logging::Logger getLogger() const {
    static logging::Logger logger_(logging::getLogger("concord.bft.reconfiguration"));
    return logger_;
  }

 private:
  std::vector<std::unique_ptr<bftEngine::impl::RSAVerifier>> internal_verifiers_;
  kvbc::IBlockAdder& blocks_adder_;
  BlockMetadata block_metadata_;
  kvbc::IReader& ro_storage_;
  uint32_t num_replicas_;
};
}  // namespace concord::kvbc::reconfiguration
