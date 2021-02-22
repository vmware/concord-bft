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

namespace concord::reconfiguration {

class ReconfigurationHandler : public IReconfigurationHandler {
 public:
  bool handle(const concord::messages::WedgeCommand&) override;
  bool handle(const concord::messages::WedgeStatusRequest&, concord::messages::WedgeStatusResponse&) override;
  bool handle(const concord::messages::GetVersionCommand&) override;
  bool handle(const concord::messages::DownloadCommand&) override;
  bool handle(const concord::messages::UpgradeCommand&) override;
  bool verifySignature(const concord::messages::ReconfigurationRequest&) const override;

 protected:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.reconfiguration"));
    return logger_;
  }
};

}  // namespace concord::reconfiguration
