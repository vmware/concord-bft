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
#include "Crypto.hpp"

namespace concord::reconfiguration {
class BftReconfigurationHandler : public IReconfigurationHandler {
 public:
  BftReconfigurationHandler();
  bool handle(const concord::messages::WedgeCommand&, uint64_t, concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::WedgeStatusRequest&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::GetVersionCommand&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::DownloadCommand&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::DownloadStatusCommand&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::InstallCommand&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::InstallStatusCommand&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::KeyExchangeCommand&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::AddRemoveCommand&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::LatestPrunableBlockRequest&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::PruneStatusRequest&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool handle(const concord::messages::PruneRequest&, uint64_t, concord::messages::ReconfigurationResponse&) override {
    return true;
  }
  bool verifySignature(const concord::messages::ReconfigurationRequest&,
                       concord::messages::ReconfigurationResponse&) const override;

  static const unsigned char internalCommandKey() {
    static unsigned char key_ = 0x20;
    return key_;
  }

 protected:
  logging::Logger getLogger() const {
    static logging::Logger logger_(logging::getLogger("concord.reconfiguration"));
    return logger_;
  }

  std::unique_ptr<bftEngine::impl::IVerifier> verifier_ = nullptr;
  std::vector<std::unique_ptr<bftEngine::impl::RSAVerifier>> internal_verifiers_;
};
class ReconfigurationHandler : public BftReconfigurationHandler {
 public:
  ReconfigurationHandler() {}
  bool handle(const concord::messages::WedgeCommand&, uint64_t, concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::WedgeStatusRequest&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override;
  bool handle(const concord::messages::KeyExchangeCommand&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override;
};

}  // namespace concord::reconfiguration
