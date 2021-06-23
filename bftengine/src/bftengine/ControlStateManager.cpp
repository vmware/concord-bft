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
  if (!enabled_) return;
  uint64_t seq_num_to_stop_at = (currentSeqNum + 2 * checkpointWindowSize);
  seq_num_to_stop_at = seq_num_to_stop_at - (seq_num_to_stop_at % checkpointWindowSize);
  std::ostringstream outStream;
  page_.seq_num_to_stop_at_ = seq_num_to_stop_at;
  concord::serialize::Serializable::serialize(outStream, page_);
  auto data = outStream.str();
  saveReservedPage(0, data.size(), data.data());
}

std::optional<int64_t> ControlStateManager::getCheckpointToStopAt() {
  if (!enabled_) return {};
  if (page_.seq_num_to_stop_at_ != 0) return page_.seq_num_to_stop_at_;
  if (!loadReservedPage(0, sizeOfReservedPage(), scratchPage_.data())) {
    return {};
  }
  std::istringstream inStream;
  inStream.str(scratchPage_);
  concord::serialize::Serializable::deserialize(inStream, page_);
  if (page_.seq_num_to_stop_at_ == 0) return {};
  if (page_.seq_num_to_stop_at_ < 0) {
    LOG_FATAL(GL, "sequence num to stop at is negative!");
    std::terminate();
  }
  return page_.seq_num_to_stop_at_;
}

void ControlStateManager::setEraseMetadataFlag(int64_t currentSeqNum) {
  if (!enabled_) return;
  uint64_t seq_num_to_erase_at = (currentSeqNum + 2 * checkpointWindowSize);
  seq_num_to_erase_at = seq_num_to_erase_at - (seq_num_to_erase_at % checkpointWindowSize);
  std::ostringstream outStream;
  page_.erase_metadata_at_seq_num_ = seq_num_to_erase_at;
  concord::serialize::Serializable::serialize(outStream, page_);
  auto data = outStream.str();
  saveReservedPage(0, data.size(), data.data());
}

std::optional<int64_t> ControlStateManager::getEraseMetadataFlag() {
  if (!enabled_) return {};
  if (page_.erase_metadata_at_seq_num_ != 0) return page_.erase_metadata_at_seq_num_;
  if (!loadReservedPage(0, sizeOfReservedPage(), scratchPage_.data())) {
    return {};
  }
  std::istringstream inStream;
  inStream.str(scratchPage_);
  concord::serialize::Serializable::deserialize(inStream, page_);
  if (page_.erase_metadata_at_seq_num_ == 0) return {};
  if (page_.erase_metadata_at_seq_num_ < 0) {
    LOG_FATAL(GL, "sequence num to set erase metadata flag at is negative!");
    std::terminate();
  }
  return page_.erase_metadata_at_seq_num_;
}
void ControlStateManager::clearCheckpointToStopAt() {
  page_.seq_num_to_stop_at_ = 0;
  std::ostringstream outStream;
  concord::serialize::Serializable::serialize(outStream, page_);
  auto data = outStream.str();
  saveReservedPage(0, data.size(), data.data());
}
void ControlStateManager::addOnRestartProofCallBack(std::function<void()> cb, RestartProofHandlerPriorities priority) {
  if (onRestartProofCbRegistery_.find(priority) == onRestartProofCbRegistery_.end()) {
    onRestartProofCbRegistery_[static_cast<uint32_t>(priority)];
  }
  onRestartProofCbRegistery_.at(static_cast<uint32_t>(priority)).add(std::move(cb));
}
void ControlStateManager::onRestartProof() {
  for (const auto& kv : onRestartProofCbRegistery_) {
    kv.second.invokeAll();
  }
}

std::pair<bool, std::string> ControlStateManager::canUnwedge() {
  std::optional<std::string> result;
  if (!bftEngine::IControlHandler::instance()->isOnNOutOfNCheckpoint()) {
    return {false, "Replica has not reached the wedge point yet"};
  }

  std::string replica_id = std::to_string(ReplicaConfig::instance().getreplicaId());
  auto sigManager = impl::SigManager::instance();
  std::string sig(sigManager->getMySigLength(), '\0');
  sigManager->sign(replica_id.c_str(), replica_id.size(), sig.data(), sig.size());

  return {true, sig};
}

bool ControlStateManager::verifyUnwedgeSignatures(
    std::vector<std::pair<uint64_t, std::vector<uint8_t>>> const& signatures) {
  auto sigManager = impl::SigManager::instance();

  size_t quorum = ReplicaConfig::instance().numReplicas;
  if (signatures.size() < quorum) {
    LOG_INFO(GL, "Not enough signatures for verification");
    return false;
  }
  size_t verified_sigs = 0;
  for (auto const& sig : signatures) {
    // SigManager cannot verify signature coming from same replica
    // Generate the signature again and compare them.
    std::string data = std::to_string(sig.first);
    std::string signature(sig.second.begin(), sig.second.end());
    bool valid = false;
    if (sig.first == ReplicaConfig::instance().replicaId) {
      verified_sigs++;
      std::string sign(sigManager->getMySigLength(), '\0');
      std::string replica_id = std::to_string(ReplicaConfig::instance().getreplicaId());
      sigManager->sign(replica_id.c_str(), replica_id.size(), sign.data(), sign.size());
      if (sign == std::string(sig.second.begin(), sig.second.end())) {
        valid = true;
        verified_sigs++;
      }
    } else {
      valid = sigManager->verifySig(sig.first, data.c_str(), data.size(), signature.data(), signature.size());
    }
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

}  // namespace bftEngine
