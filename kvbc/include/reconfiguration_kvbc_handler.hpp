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
#include "categorization/kv_blockchain.h"
#include "AdaptivePruningManager.hpp"
#include "IntervalMappingResourceManager.hpp"
#include <functional>
#include <string>
#include <utility>

namespace concord::kvbc::reconfiguration {
class ReconfigurationBlockTools {
 protected:
  ReconfigurationBlockTools(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : blocks_adder_{block_adder}, block_metadata_{ro_storage}, ro_storage_{ro_storage} {}
  kvbc::BlockId persistReconfigurationBlock(const std::vector<uint8_t>& data,
                                            const uint64_t bft_seq_num,
                                            std::string key,
                                            const std::optional<bftEngine::Timestamp>&,
                                            bool include_wedge);
  kvbc::BlockId persistReconfigurationBlock(concord::kvbc::categorization::VersionedUpdates& ver_updates,
                                            const uint64_t bft_seq_num,
                                            const std::optional<bftEngine::Timestamp>&,
                                            bool include_wedge);
  kvbc::BlockId persistNewEpochBlock(const uint64_t bft_seq_num);

  kvbc::IBlockAdder& blocks_adder_;
  BlockMetadata block_metadata_;
  kvbc::IReader& ro_storage_;
};

// Handles State Snapshot requests from both the Operator and Clients.
class StateSnapshotReconfigurationHandler : public ReconfigurationBlockTools,
                                            public concord::reconfiguration::IReconfigurationHandler {
 public:
  StateSnapshotReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : ReconfigurationBlockTools{block_adder, ro_storage} {}

  bool handle(const concord::messages::StateSnapshotRequest&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::SignedPublicStateHashRequest&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::StateSnapshotReadAsOfRequest&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  // Allow snapshot requests from the Operator and from Clients.
  bool verifySignature(uint32_t sender_id, const std::string& data, const std::string& signature) const override {
    // Try the BFT reconfiguration handler first. If not verified by the reconfiguration handler, try the
    // ClientReconfigurationHandler.
    return (bft_reconf_handler_.verifySignature(sender_id, data, signature) ||
            client_reconf_handler_.verifySignature(sender_id, data, signature));
  }

 protected:
  // Allows users to convert state values to any format that is appropriate.
  // The default converter returns the input string as is, without modifying it.
  categorization::KeyValueBlockchain::Converter state_value_converter_{
      [](std::string&& v) -> std::string { return std::move(v); }};

 private:
  const concord::reconfiguration::BftReconfigurationHandler bft_reconf_handler_;
  const concord::reconfiguration::ClientReconfigurationHandler client_reconf_handler_;
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
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::ClientReconfigurationStateRequest&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::ClientsAddRemoveUpdateCommand&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::ClientsRestartUpdate&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

 private:
  concord::messages::ClientStateReply buildClientStateReply(kvbc::keyTypes::CLIENT_COMMAND_TYPES command_type,
                                                            uint32_t clientid);
  concord::messages::ClientStateReply buildReplicaStateReply(const std::string& command_type, uint32_t clientid);
};
/**
 * This component is responsible for logging reconfiguration request (issued by an authorized operator) in the
 * blockchian.
 */
class ReconfigurationHandler : public concord::reconfiguration::BftReconfigurationHandler,
                               public ReconfigurationBlockTools {
 public:
  ReconfigurationHandler(kvbc::IBlockAdder& block_adder,
                         kvbc::IReader& ro_storage,
                         concord::performance::AdaptivePruningManager& apm)
      : ReconfigurationBlockTools{block_adder, ro_storage}, apm_(apm) {
    (void)apm_;  // unused for now
  }
  bool handle(const concord::messages::WedgeCommand& command,
              uint64_t bft_seq_num,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::DownloadCommand& command,
              uint64_t bft_seq_num,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::InstallCommand& command,
              uint64_t bft_seq_num,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::KeyExchangeCommand& command,
              uint64_t sequence_number,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::AddRemoveCommand& command,
              uint64_t sequence_number,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::AddRemoveWithWedgeCommand& command,
              uint64_t sequence_number,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::AddRemoveStatus& command,
              uint64_t sequence_number,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse& response) override;

  bool handle(const concord::messages::AddRemoveWithWedgeStatus& command,
              uint64_t sequence_number,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse& response) override;

  bool handle(const concord::messages::PruneRequest& command,
              uint64_t sequence_number,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::ClientKeyExchangeCommand& command,
              uint64_t sequence_number,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse& response) override;
  bool handle(const concord::messages::RestartCommand&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::UnwedgeCommand&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::UnwedgeStatusRequest&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::ClientsAddRemoveCommand&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::ClientsAddRemoveStatusCommand&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::ClientKeyExchangeStatus&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::ClientsRestartCommand&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::ClientsRestartStatus& command,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::PruneSwitchModeRequest&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::PruneStopRequest&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;

 private:
  concord::performance::AdaptivePruningManager& apm_;
  concord::performance::ISystemResourceEntity& replicaResources_;
};
/**
 * This component is reposnsible for logging internal reconfiguration requests to the blockchain (such as noop
 * commands)
 */
class InternalKvReconfigurationHandler : public concord::reconfiguration::IReconfigurationHandler,
                                         public ReconfigurationBlockTools {
 public:
  InternalKvReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : ReconfigurationBlockTools{block_adder, ro_storage} {}
  bool verifySignature(uint32_t sender_id, const std::string& data, const std::string& signature) const override;

  bool handle(const concord::messages::WedgeCommand& command,
              uint64_t bft_seq_num,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::ReplicaTlsExchangeKey&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::ReplicaMainKeyUpdate&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::PruneTicksChangeRequest&,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse&) override;
};

class InternalPostKvReconfigurationHandler : public concord::reconfiguration::IReconfigurationHandler,
                                             public ReconfigurationBlockTools {
 public:
  InternalPostKvReconfigurationHandler(kvbc::IBlockAdder& block_adder, kvbc::IReader& ro_storage)
      : ReconfigurationBlockTools{block_adder, ro_storage} {}
  bool verifySignature(uint32_t sender_id, const std::string& data, const std::string& signature) const override {
    return true;
  }

  bool handle(const concord::messages::ClientExchangePublicKey& command,
              uint64_t sequence_number,
              uint32_t,
              const std::optional<bftEngine::Timestamp>&,
              concord::messages::ReconfigurationResponse& response) override;
};
}  // namespace concord::kvbc::reconfiguration
