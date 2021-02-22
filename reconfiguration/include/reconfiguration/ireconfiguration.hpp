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

#include "concord.cmf.hpp"
#include "OpenTracing.hpp"
#include "kv_types.hpp"
#include "Replica.hpp"

namespace concord::reconfiguration {

// The IReconfigurationHandler interface defines all message handler. It is
// tightly coupled with the messages inside ReconfigurationSmRequest in the
// message definition.
class IReconfigurationHandler {
 public:
  // Message handlers
  virtual bool handle(const concord::messages::WedgeCommand&) = 0;
  virtual bool handle(const concord::messages::WedgeStatusRequest&, concord::messages::WedgeStatusResponse&) = 0;
  virtual bool handle(const concord::messages::GetVersionCommand&) = 0;
  virtual bool handle(const concord::messages::DownloadCommand&) = 0;
  virtual bool handle(const concord::messages::UpgradeCommand&) = 0;

  virtual bool verifySignature(const concord::messages::ReconfigurationRequest&) const = 0;

  virtual ~IReconfigurationHandler() = default;
};

class IPruningHandler {
 public:
  virtual bool handle(const concord::messages::LatestPrunableBlockRequest&,
                      concord::messages::LatestPrunableBlock&) = 0;
  virtual bool handle(const concord::messages::PruneStatusRequest&, concord::messages::PruneStatus&) = 0;
  virtual bool handle(const concord::messages::PruneRequest&, kvbc::BlockId&) = 0;

  virtual ~IPruningHandler() = default;
};

}  // namespace concord::reconfiguration
