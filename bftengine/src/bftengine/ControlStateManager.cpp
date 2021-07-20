// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ControlStateManager.hpp"
#include "SigManager.hpp"
#include "Logger.hpp"
#include "ReplicaConfig.hpp"
#include "Replica.hpp"
namespace bftEngine {

/*
 * This method should be called by the command handler in order to mark that we want to stop the system in the next
 * possible checkpoint. Now, consider the following:
 * 1. The wedge command is on seqNum 290.
 * 2. The concurrency level is 30 and thus we first decided on 291 -> 319.
 * 3. Then we decide on 290 and execute it.
 * The next possible checkpoint to stop at is 450 (rather than 300). Thus, we calculate the next next checkpoint w.r.t
 * given sequence number and mark it as a checkpoint to stop at in the reserved pages.
 */
void ControlStateManager::setStopAtNextCheckpoint(int64_t currentSeqNum) {
  if (currentSeqNum == 0) {
    wedgePoint = 0;
    return;
  }
  uint64_t seq_num_to_stop_at = (currentSeqNum + 2 * checkpointWindowSize);
  seq_num_to_stop_at = seq_num_to_stop_at - (seq_num_to_stop_at % checkpointWindowSize);
  wedgePoint = seq_num_to_stop_at;
}

std::optional<int64_t> ControlStateManager::getCheckpointToStopAt() {
  if (wedgePoint == 0) return {};
  return wedgePoint;
}

void ControlStateManager::addOnRestartProofCallBack(std::function<void()> cb, RestartProofHandlerPriorities priority) {
  if (onRestartProofCbRegistry_.find(priority) == onRestartProofCbRegistry_.end()) {
    onRestartProofCbRegistry_[static_cast<uint32_t>(priority)];
  }
  onRestartProofCbRegistry_.at(static_cast<uint32_t>(priority)).add(std::move(cb));
}
void ControlStateManager::onRestartProof(const SeqNum& seq_num) {
  // If operator sends add-remove request with bft option then
  // It can happen that some replicas receives a restart proof and yet to reach
  // stable checkpoint. We should not rstart replica in that case since
  // configuration update happens on stable checkpoint.
  if ((restartBftEnabled_ && IControlHandler::instance()->isOnStableCheckpoint()) ||
      IControlHandler::instance()->isOnNOutOfNCheckpoint()) {
    for (const auto& kv : onRestartProofCbRegistry_) {
      kv.second.invokeAll();
    }
  }
  hasRestartProofAtSeqNum_.emplace(seq_num);
}
void ControlStateManager::checkForReplicaReconfigurationAction() {
  // restart replica is there is proof
  auto seq_num_to_stop_at = getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value() && hasRestartProofAtSeqNum_.has_value() &&
      (seq_num_to_stop_at.value() == hasRestartProofAtSeqNum_.value())) {
    for (const auto& kv : onRestartProofCbRegistry_) {
      kv.second.invokeAll();
    }
  }
}

std::pair<bool, std::string> ControlStateManager::canUnwedge(bool bft) {
  if ((!bft && !bftEngine::IControlHandler::instance()->isOnNOutOfNCheckpoint()) ||
      (bft && !bftEngine::IControlHandler::instance()->isOnStableCheckpoint())) {
    return {false, "Replica has not reached the wedge point yet"};
  }

  auto last_checkpoint = getCheckpointToStopAt();
  ConcordAssert(last_checkpoint.has_value());

  std::string sig_data =
      std::to_string(ReplicaConfig::instance().getreplicaId()) + std::to_string(last_checkpoint.value());
  auto sig_manager = impl::SigManager::instance();
  std::string sig(sig_manager->getMySigLength(), '\0');
  sig_manager->sign(sig_data.c_str(), sig_data.size(), sig.data(), sig.size());

  return {true, sig};
}

bool ControlStateManager::verifyUnwedgeSignatures(
    std::vector<std::pair<uint64_t, std::vector<uint8_t>>> const& signatures, bool bft) {
  size_t quorum = ReplicaConfig::instance().numReplicas;
  if (bft) quorum -= ReplicaConfig::instance().fVal;
  if (signatures.size() < quorum) {
    LOG_INFO(GL, "Not enough signatures for verification");
    return false;
  }

  auto last_checkpoint = getCheckpointToStopAt();
  if (!last_checkpoint.has_value()) {
    return false;
  }

  auto sig_manager = impl::SigManager::instance();
  size_t verified_sigs = 0;

  for (auto const& sig : signatures) {
    std::string sig_data = std::to_string(sig.first) + std::to_string(last_checkpoint.value());
    std::string signature(sig.second.begin(), sig.second.end());
    bool valid =
        sig_manager->verifySig(sig.first, sig_data.c_str(), sig_data.size(), signature.data(), signature.size());
    if (!valid) {
      LOG_INFO(GL, "Invalid signature for principal id " << sig.first);
    } else {
      verified_sigs++;
    }
  }

  if (verified_sigs < quorum) {
    LOG_INFO(GL, "Not enough valid signatures for unwedge");
    return false;
  }
  LOG_INFO(GL, "Successfully validated unwedge signatures");
  return true;
}

void ControlStateManager::restart() {
  for (const auto& kv : onRestartProofCbRegistry_) {
    kv.second.invokeAll();
  }
}

}  // namespace bftEngine
