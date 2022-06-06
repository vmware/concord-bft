// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "CorruptCheckpointMsgStrategy.hpp"
#include "messages/CheckpointMsg.hpp"
#include "SigManager.hpp"

namespace concord::kvbc::strategy {

CorruptCheckpointMsgStrategy::CorruptCheckpointMsgStrategy(logging::Logger& logger,
                                                           std::unordered_set<ReplicaId>&& byzantine_replica_ids)
    : logger_(logger), byzantine_replica_ids_(std::move(byzantine_replica_ids)) {
  srand(time(NULL));
}

std::string CorruptCheckpointMsgStrategy::getStrategyName() { return CLASSNAME(CorruptCheckpointMsgStrategy); }
uint16_t CorruptCheckpointMsgStrategy::getMessageCode() { return static_cast<uint16_t>(MsgCode::Checkpoint); }

bool CorruptCheckpointMsgStrategy::changeMessage(std::shared_ptr<bftEngine::impl::MessageBase>& msg) {
  CheckpointMsg& checkpoint_message = static_cast<CheckpointMsg&>(*(msg.get()));
  LOG_INFO(logger_, KVLOG(byzantine_replica_ids_.size(), checkpoint_message.senderId()));

  if (byzantine_replica_ids_.find(checkpoint_message.senderId()) != byzantine_replica_ids_.end()) {
    Digest& current_rvb_data_digest = checkpoint_message.rvbDataDigest();
    // Modify a random byte on the rvb Data Digest
    LOG_INFO(logger_, "Before changing RVB data digest:" << KVLOG(current_rvb_data_digest));
    auto n = rand() % sizeof(Digest);
    current_rvb_data_digest.getForUpdate()[n]++;
    LOG_INFO(logger_, "After changing RVB data digest:" << KVLOG(current_rvb_data_digest, n));
    checkpoint_message.sign();
    return true;
  }
  return false;
}
}  // end of namespace concord::kvbc::strategy
