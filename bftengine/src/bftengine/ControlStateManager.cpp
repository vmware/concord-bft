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

void ControlStateManager::clearCheckpointToStopAt() {
  page_.seq_num_to_stop_at_ = 0;
  std::ostringstream outStream;
  concord::serialize::Serializable::serialize(outStream, page_);
  auto data = outStream.str();
  saveReservedPage(0, data.size(), data.data());
}

}  // namespace bftEngine
