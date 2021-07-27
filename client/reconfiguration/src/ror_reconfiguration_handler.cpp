// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "client/reconfiguration/ror_reconfiguration_handler.hpp"
#include "bftengine/ReconfigurationCmd.hpp"

namespace concord::client::reconfiguration {
RorReconfigurationHandler::RorReconfigurationHandler(std::function<void(uint64_t)> fn)
    : storeReconfigBlockToMdtCb_(fn) {}

bool RorReconfigurationHandler::validate(const State& s) const {
  bftEngine::ReconfigurationCmd::ReconfigurationCmdData cmdData;
  std::istringstream inStream;
  std::string page(s.data.begin(), s.data.end());
  inStream.str(page);
  concord::serialize::Serializable::deserialize(inStream, cmdData);
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::deserialize(cmdData.data_, rreq);
  return std::holds_alternative<concord::messages::AddRemoveWithWedgeCommand>(rreq.command);
}
bool RorReconfigurationHandler::execute(const State& s, WriteState&) {
  bftEngine::ReconfigurationCmd::ReconfigurationCmdData cmdData;
  std::istringstream inStream;
  std::string page(s.data.begin(), s.data.end());
  inStream.str(page);
  concord::serialize::Serializable::deserialize(inStream, cmdData);
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::deserialize(cmdData.data_, rreq);
  concord::messages::AddRemoveWithWedgeCommand cmd =
      std::get<concord::messages::AddRemoveWithWedgeCommand>(rreq.command);
  LOG_INFO(getLogger(),
           "AddRemove command for RO replica:" << KVLOG(cmdData.blockId_, cmdData.wedgePoint_, cmd.config_descriptor));
  if (storeReconfigBlockToMdtCb_) storeReconfigBlockToMdtCb_(cmdData.blockId_);
  return false;
}
}  // namespace concord::client::reconfiguration