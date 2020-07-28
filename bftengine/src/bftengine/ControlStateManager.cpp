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
#include "Logger.hpp"
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
  controlStateMessages::StopAtNextCheckpointMessage msg(seq_num_to_stop_at);
  concord::serialize::Serializable::serialize(outStream, msg);
  auto data = outStream.str();
  state_transfer_->saveReservedPage(getUpdateReservedPageIndex(), data.size(), data.data());
}

std::optional<int64_t> ControlStateManager::getCheckpointToStopAt() {
  if (!enabled_) return {};
  if (!state_transfer_->loadReservedPage(getUpdateReservedPageIndex(), sizeOfReservedPage_, scratchPage_.data())) {
    return {};
  }
  std::istringstream inStream;
  inStream.str(scratchPage_);
  controlStateMessages::StopAtNextCheckpointMessage msg;
  concord::serialize::Serializable::deserialize(inStream, msg);
  if (msg.seqNumToStopAt_ < 0) {
    LOG_WARN(GL, "sequence num to stop at is negative!");
    return {};
  }
  return msg.seqNumToStopAt_;
}

ControlStateManager::ControlStateManager(IStateTransfer* state_transfer, uint32_t sizeOfReservedPages)
    : state_transfer_{state_transfer}, sizeOfReservedPage_{sizeOfReservedPages} {
  scratchPage_.resize(sizeOfReservedPage_);
}

}  // namespace bftEngine