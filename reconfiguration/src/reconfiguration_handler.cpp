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

#include "reconfiguration/reconfiguration_handler.hpp"
#include "bftengine/ControlStateManager.hpp"
#include "Replica.hpp"
#include "kvstream.h"

using namespace concord::messages;
namespace concord::reconfiguration {

bool ReconfigurationHandler::handle(const WedgeCommand& cmd) {
  LOG_INFO(getLogger(), "Wedge command instructs replica to stop at sequence number " << cmd.stop_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(cmd.stop_seq_num);
  return true;
}

bool ReconfigurationHandler::handle(const WedgeStatusRequest& req, WedgeStatusResponse& response) {
  response.stopped = bftEngine::IControlHandler::instance()->isOnNOutOfNCheckpoint();
  return true;
}

bool ReconfigurationHandler::handle(const GetVersionCommand&, concord::messages::GetVersionResponse&) { return true; }

bool ReconfigurationHandler::handle(const DownloadCommand&) { return true; }

bool ReconfigurationHandler::verifySignature(const concord::messages::ReconfigurationRequest&) const { return true; }

bool ReconfigurationHandler::handle(const concord::messages::DownloadStatusCommand&,
                                    concord::messages::DownloadStatus&) {
  return true;
}
bool ReconfigurationHandler::handle(const concord::messages::InstallCommand& cmd, uint64_t) { return true; }
bool ReconfigurationHandler::handle(const concord::messages::InstallStatusCommand& cmd,
                                    concord::messages::InstallStatusResponse& response) {
  return true;
}
}  // namespace concord::reconfiguration
