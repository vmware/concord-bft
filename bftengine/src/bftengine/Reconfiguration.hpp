// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "reconfiguration/reconfiguration.hpp"
#include "log/logger.hpp"
#include "ReplicasInfo.hpp"

namespace bftEngine::impl {

class ReconfigurationHandler : public concord::reconfiguration::OperatorCommandsReconfigurationHandler {
 public:
  ReconfigurationHandler(const std::string &operator_pub_key_path, concord::crypto::SignatureAlgorithm type)
      : OperatorCommandsReconfigurationHandler{operator_pub_key_path, type} {}
  bool handle(const concord::messages::WedgeCommand &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;
  bool handle(const concord::messages::WedgeStatusRequest &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;
  bool handle(const concord::messages::KeyExchangeCommand &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;
  bool handle(const concord::messages::AddRemoveWithWedgeCommand &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;
  bool handle(const concord::messages::AddRemoveWithWedgeStatus &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;

  bool handle(const concord::messages::RestartCommand &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;

  bool handle(const concord::messages::InstallCommand &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;

  bool handle(const concord::messages::GetDbCheckpointInfoRequest &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;

  bool handle(const concord::messages::CreateDbCheckpointCommand &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;

  bool handle(const concord::messages::DbSizeReadRequest &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;

 private:
  void handleWedgeCommands(bool bft_support,
                           bool remove_metadata,
                           bool restart,
                           bool unwedge,
                           bool blockNewConnections,
                           bool createDbCheckpoint);
  void addCreateDbSnapshotCbOnWedge(bool bft_support);
};

class ClientReconfigurationHandler : public concord::reconfiguration::IReconfigurationHandler {
 public:
  ClientReconfigurationHandler() {}
  bool handle(const concord::messages::ClientExchangePublicKey &,
              uint64_t,
              uint32_t,
              const std::optional<bftEngine::Timestamp> &,
              concord::messages::ReconfigurationResponse &) override;

  bool verifySignature(uint32_t sender_id, const std::string &data, const std::string &signature) const override;
};
}  // namespace bftEngine::impl