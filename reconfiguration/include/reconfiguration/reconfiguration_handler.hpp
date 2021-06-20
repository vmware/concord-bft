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
#include "openssl_crypto.hpp"

namespace concord::reconfiguration {
class BftReconfigurationHandler : public IReconfigurationHandler {
 public:
  BftReconfigurationHandler();
  bool verifySignature(const std::string& data, const std::string& signature) const override;

 protected:
  logging::Logger getLogger() const {
    static logging::Logger logger_(logging::getLogger("concord.bft.reconfiguration"));
    return logger_;
  }
  std::unique_ptr<concord::util::openssl_utils::AsymmetricPublicKey> pub_key_ = nullptr;
  std::unique_ptr<bftEngine::impl::IVerifier> verifier_ = nullptr;
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
  bool handle(const concord::messages::AddRemoveWithWedgeCommand&,
              uint64_t,
              concord::messages::ReconfigurationResponse&) override;
};
}  // namespace concord::reconfiguration
