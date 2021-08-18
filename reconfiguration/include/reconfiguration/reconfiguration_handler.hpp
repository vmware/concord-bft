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

#include "Logger.hpp"
#include "Replica.hpp"
#include "concord.cmf.hpp"
#include "ireconfiguration.hpp"
#include "OpenTracing.hpp"
#include "openssl_crypto.hpp"
#include "SigManager.hpp"
#include "crypto_utils.hpp"

namespace concord::reconfiguration {
class BftReconfigurationHandler : public IReconfigurationHandler {
 public:
  BftReconfigurationHandler();
  bool verifySignature(uint32_t sender_id, const std::string& data, const std::string& signature) const override;

  std::unique_ptr<concord::util::crypto::IVerifier> verifier_;
};
class ReconfigurationHandler : public BftReconfigurationHandler {
 public:
  ReconfigurationHandler() {}
  bool handle(const concord::messages::WedgeCommand&,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::WedgeStatusRequest&,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::KeyExchangeCommand&,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::AddRemoveWithWedgeCommand&,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::AddRemoveWithWedgeStatus&,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse&) override;

  bool handle(const concord::messages::RestartCommand&,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse&) override;

 private:
  void handleWedgeCommands(bool bft_support, bool remove_metadata, bool restart, bool unwedge);
};

class ClientReconfigurationHandler : public concord::reconfiguration::IReconfigurationHandler {
  bool handle(const concord::messages::ClientExchangePublicKey&,
              uint64_t,
              uint32_t,
              concord::messages::ReconfigurationResponse&) override;

  bool verifySignature(uint32_t sender_id, const std::string& data, const std::string& signature) const override {
    if (!bftEngine::impl::SigManager::instance()->hasVerifier(sender_id)) return false;
    return bftEngine::impl::SigManager::instance()->verifySig(
        sender_id, data.data(), data.size(), signature.data(), signature.size());
  }
};

}  // namespace concord::reconfiguration
