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

class ReconfigurationHandler : public IReconfigurationHandler {
 public:
  ReconfigurationHandler();
  bool handle(const concord::messages::WedgeCommand&, uint64_t, concord::messages::ReconfigurationErrorMsg&) override;
  bool handle(const concord::messages::WedgeStatusRequest&,
              concord::messages::WedgeStatusResponse&,
              concord::messages::ReconfigurationErrorMsg&) override;
  bool handle(const concord::messages::GetVersionCommand&,
              concord::messages::GetVersionResponse&,
              concord::messages::ReconfigurationErrorMsg&) override;
  bool handle(const concord::messages::DownloadCommand&,
              uint64_t,
              concord::messages::ReconfigurationErrorMsg&) override;
  bool handle(const concord::messages::DownloadStatusCommand&,
              concord::messages::DownloadStatus&,
              concord::messages::ReconfigurationErrorMsg&) override;
  bool handle(const concord::messages::InstallCommand& cmd,
              uint64_t,
              concord::messages::ReconfigurationErrorMsg&) override;
  bool handle(const concord::messages::InstallStatusCommand& cmd,
              concord::messages::InstallStatusResponse&,
              concord::messages::ReconfigurationErrorMsg&) override;
  bool verifySignature(const concord::messages::ReconfigurationRequest&,
                       concord::messages::ReconfigurationErrorMsg&) const override;

 protected:
  logging::Logger getLogger() const {
    static logging::Logger logger_(logging::getLogger("concord.reconfiguration"));
    return logger_;
  }
  std::unique_ptr<bftEngine::impl::IVerifier> verifier_ = nullptr;
};

}  // namespace concord::reconfiguration
