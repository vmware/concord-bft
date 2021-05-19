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

#include "bftengine/KeyExchangeManager.hpp"
#include "bftengine/ControlStateManager.hpp"
#include "Replica.hpp"
#include "kvstream.h"

using namespace concord::messages;
namespace concord::reconfiguration {

bool ReconfigurationHandler::handle(const WedgeCommand& cmd,
                                    uint64_t bft_seq_num,
                                    concord::messages::ReconfigurationResponse&) {
  LOG_INFO(getLogger(), "Wedge command instructs replica to stop at sequence number " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  return true;
}

bool ReconfigurationHandler::handle(const WedgeStatusRequest& req,
                                    uint64_t,
                                    concord::messages::ReconfigurationResponse& rres) {
  concord::messages::WedgeStatusResponse response;
  if (req.fullWedge) {
    response.stopped = bftEngine::IControlHandler::instance()->isOnNOutOfNCheckpoint();
  } else {
    response.stopped = bftEngine::IControlHandler::instance()->isOnStableCheckpoint();
  }
  rres.response = response;
  return true;
}

bool ReconfigurationHandler::handle(const KeyExchangeCommand& command, uint64_t sequence_number, concord::messages::ReconfigurationResponse&) {
  std::ostringstream oss;
  std::copy(command.target_replicas.begin(), command.target_replicas.end(), std::ostream_iterator<int>(oss, " "));

  LOG_INFO(GL, KVLOG(command.id, command.sender_id, sequence_number) << " target replicas: [" << oss.str() << "]");
  if (std::find(command.target_replicas.begin(),
                command.target_replicas.end(),
                bftEngine::ReplicaConfig::instance().getreplicaId()) != command.target_replicas.end())
    bftEngine::impl::KeyExchangeManager::instance().sendKeyExchange(sequence_number);
  else
    LOG_INFO(GL, "not among target replicas, ignoring...");

  return true;
}

BftReconfigurationHandler::BftReconfigurationHandler() {
  auto operatorPubKeyPath = bftEngine::ReplicaConfig::instance().pathToOperatorPublicKey_;
  if (operatorPubKeyPath.empty()) {
    LOG_WARN(getLogger(),
             "The operator public key is missing, the reconfiguration handler won't be able to execute the requests");
    return;
  }
  verifier_ = std::make_unique<bftEngine::impl::ECDSAVerifier>(operatorPubKeyPath);
}
bool BftReconfigurationHandler::verifySignature(const ReconfigurationRequest& request,
                                                ReconfigurationResponse& rres) const {
  bool valid = false;
  concord::messages::ReconfigurationErrorMsg error_msg;
  ReconfigurationRequest request_without_sig = request;
  request_without_sig.signature = {};
  std::vector<uint8_t> serialized_cmd;
  concord::messages::serialize(serialized_cmd, request_without_sig);

  auto ser_data = std::string(serialized_cmd.begin(), serialized_cmd.end());
  auto ser_sig = std::string(request.signature.begin(), request.signature.end());
  valid = verifier_->verify(ser_data, ser_sig);
  if (!valid) {
    error_msg.error_msg = "Invalid signature";
    rres.response = error_msg;
    LOG_ERROR(getLogger(), "The message's signature is invalid!");
  }
  return valid;
}
}  // namespace concord::reconfiguration
