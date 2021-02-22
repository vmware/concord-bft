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

class PruningHandler : public IPruningHandler {
 public:
  bool handle(const concord::messages::LatestPrunableBlockRequest&, concord::messages::LatestPrunableBlock&) override {
    return false;
  }
  bool handle(const concord::messages::PruneStatusRequest&, concord::messages::PruneStatus&) override { return false; }
  bool handle(const concord::messages::PruneRequest&, kvbc::BlockId&) override { return false; }

 protected:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.pruning"));
    return logger_;
  }
};

}  // namespace concord::reconfiguration
