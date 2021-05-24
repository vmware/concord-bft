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
                                    concord::messages::ReconfigurationErrorMsg&) {
  if (cmd.noop) {
    LOG_INFO(getLogger(), "received noop command, a new block will be written" << KVLOG(bft_seq_num));
    return true;
  }
  LOG_INFO(getLogger(), "Wedge command instructs replica to stop at sequence number " << bft_seq_num);
  bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(bft_seq_num);
  return true;
}

bool ReconfigurationHandler::handle(const WedgeStatusRequest& req,
                                    WedgeStatusResponse& response,
                                    concord::messages::ReconfigurationErrorMsg&) {
  if (req.fullWedge) {
    response.stopped = bftEngine::IControlHandler::instance()->isOnNOutOfNCheckpoint();
  } else {
    response.stopped = bftEngine::IControlHandler::instance()->isOnStableCheckpoint();
  }
  return true;
}

bool ReconfigurationHandler::handle(const GetVersionCommand&,
                                    concord::messages::GetVersionResponse&,
                                    concord::messages::ReconfigurationErrorMsg&) {
  return true;
}

bool ReconfigurationHandler::handle(const DownloadCommand&, uint64_t, concord::messages::ReconfigurationErrorMsg&) {
  return true;
}

bool ReconfigurationHandler::verifySignature(const concord::messages::ReconfigurationRequest& request,
                                             concord::messages::ReconfigurationErrorMsg& error_msg) const {
  bool valid = false;

  ReconfigurationRequest request_without_sig = request;
  request_without_sig.signature = {};
  std::vector<uint8_t> serialized_cmd;
  concord::messages::serialize(serialized_cmd, request_without_sig);

  auto ser_data = std::string(serialized_cmd.begin(), serialized_cmd.end());
  auto ser_sig = std::string(request.signature.begin(), request.signature.end());

  if (!request.additional_data.empty() && request.additional_data.front() == internalCommandKey()) {
    // It means we got an internal command, lets verify it with the internal verifiers
    for (auto& verifier : internal_verifiers_) {
      valid |= verifier->verify(ser_data.c_str(), ser_data.size(), ser_sig.c_str(), ser_sig.size());
      if (valid) break;
    }
  } else if (!verifier_) {
    LOG_WARN(getLogger(),
             "The public operator public key is missing, the reconfiguration engine assumes that some higher level "
             "implementation is verifying the operator requests");
    return true;
  } else {
    valid = verifier_->verify(ser_data, ser_sig);
  }
  if (!valid) {
    error_msg.error_msg = "Invalid signature";
    LOG_ERROR(getLogger(), "The message's signature is invalid!");
  }
  return valid;
}

bool ReconfigurationHandler::handle(const concord::messages::DownloadStatusCommand&,
                                    concord::messages::DownloadStatus&,
                                    concord::messages::ReconfigurationErrorMsg&) {
  return true;
}
bool ReconfigurationHandler::handle(const concord::messages::InstallCommand& cmd,
                                    uint64_t,
                                    concord::messages::ReconfigurationErrorMsg&) {
  return true;
}
bool ReconfigurationHandler::handle(const concord::messages::InstallStatusCommand& cmd,
                                    concord::messages::InstallStatusResponse& response,
                                    concord::messages::ReconfigurationErrorMsg&) {
  return true;
}
ReconfigurationHandler::ReconfigurationHandler() {
  for (const auto& [rep, pk] : bftEngine::ReplicaConfig::instance().publicKeysOfReplicas) {
    internal_verifiers_.emplace_back(std::make_unique<bftEngine::impl::RSAVerifier>(pk.c_str()));
    (void)rep;
  }
  auto operatorPubKeyPath = bftEngine::ReplicaConfig::instance().pathToOperatorPublicKey_;
  if (operatorPubKeyPath.empty()) {
    LOG_WARN(getLogger(),
             "The operator public key is missing, the replica won't be able to validate the operator requests");
  } else {
    verifier_ = std::make_unique<bftEngine::impl::ECDSAVerifier>(operatorPubKeyPath);
  }
}
bool ReconfigurationHandler::handle(const KeyExchangeCommand& command,
                                    ReconfigurationErrorMsg&,
                                    uint64_t sequence_number) {
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

bool ReconfigurationHandler::handle(const concord::messages::AddRemoveCommand&,
                                    concord::messages::ReconfigurationErrorMsg&,
                                    uint64_t) {
  return true;
}
}  // namespace concord::reconfiguration
