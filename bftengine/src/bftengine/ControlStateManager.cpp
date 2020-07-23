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
namespace bftEngine {

// This method will be called from the command handler. Notice that the command handler does not aware to the number of
// the checkpoint. Thus, it can only raise a flag to stop.
void ControlStateManager::setStopAtNextCheckpoint(uint64_t currentSeqNum) {
  uint64_t seq_num_to_stop_at = (currentSeqNum + 2 * checkpointWindowSize);
  seq_num_to_stop_at = seq_num_to_stop_at - (seq_num_to_stop_at % checkpointWindowSize);
  std::ostringstream outStream;
  controlStateMessages::StopAtNextCheckpointMessage msg(seq_num_to_stop_at);
  concord::serialize::Serializable::serialize(outStream, msg);
  auto data = outStream.str();
  state_transfer_->saveReservedPage(
      resPageOffset() + reserved_pages_indexer_.update_reserved_page_, data.size(), data.data());
}

std::optional<uint64_t> ControlStateManager::getCheckpointToStopAt() {
  if (!state_transfer_->loadReservedPage(resPageOffset() + reserved_pages_indexer_.update_reserved_page_,
                                         sizeOfReservedPage_,
                                         scratchPage_.data())) {
    return {};
  }
  std::istringstream inStream;
  inStream.str(scratchPage_);
  controlStateMessages::StopAtNextCheckpointMessage msg;
  concord::serialize::Serializable::deserialize(inStream, msg);
  return msg.seqNumToStopAt_;
}

ControlStateManager::ControlStateManager(IStateTransfer* state_transfer,  uint32_t sizeOfReservedPages) : state_transfer_{state_transfer}, sizeOfReservedPage_{sizeOfReservedPages} {
  scratchPage_.resize(sizeOfReservedPage_);
}

}  // namespace bftEngine