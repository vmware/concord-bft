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

#include "log/logger.hpp"
#include "SigManager.hpp"
#include "ReplicaConfig.hpp"
#include "Replica.hpp"
#include "messages/ReplicasRestartReadyProofMsg.hpp"

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

void ControlStateManager::addOnRestartProofCallBack(std::function<void()> cb,
                                                    uint8_t reason,
                                                    RestartProofHandlerPriorities priority) {
  auto& cbRegistry = onRestartProofCbRegistry_[reason];
  if (cbRegistry.find(priority) == cbRegistry.end()) {
    cbRegistry[static_cast<uint32_t>(priority)];
  }
  cbRegistry.at(static_cast<uint32_t>(priority)).add(std::move(cb));
}
void ControlStateManager::onRestartProof(const SeqNum& seq_num, uint8_t reason) {
  // If operator sends add-remove request with bft option then
  // It can happen that some replicas receives a restart proof and yet to reach
  // stable checkpoint. We should not restart replica in that case since
  // configuration update happens on stable checkpoint.

  if ((restartBftEnabled_ && IControlHandler::instance()->isOnStableCheckpoint()) ||
      IControlHandler::instance()->isOnNOutOfNCheckpoint()) {
    auto seq_num_to_stop_at = getCheckpointToStopAt();
    if (seq_num_to_stop_at.has_value() && seq_num) {
      // A nasty hack to allow state transfer replicas to get the update in high probability - we are going to sleep for
      // 10 seconds, assuming that it is enough for state transfer replicas to get the state change and perform the
      // update
      if (reason == static_cast<uint8_t>(ReplicaRestartReadyMsg::Reason::Scale))
        std::this_thread::sleep_for(std::chrono::seconds(10));
      hasRestartProofAtSeqNum_[reason] = seq_num;
      for (const auto& kv : onRestartProofCbRegistry_[reason]) {
        kv.second.invokeAll();
      }
    }
  }
}
void ControlStateManager::checkForReplicaReconfigurationAction() {
  // restart replica is there is proof
  auto seq_num_to_stop_at = getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value()) {
    for (auto& [k, v] : hasRestartProofAtSeqNum_) {
      if (v == seq_num_to_stop_at.value()) {
        for (const auto& kv : onRestartProofCbRegistry_[k]) {
          kv.second.invokeAll();
        }
      }
    }
  }
}

void ControlStateManager::restart() {
  for (const auto& kv : onRestartProofCbRegistry_[static_cast<uint8_t>(ReplicaRestartReadyMsg::Reason::Scale)]) {
    kv.second.invokeAll();
  }
}

}  // namespace bftEngine
