// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "CheckpointMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

CheckpointMsg::CheckpointMsg(ReplicaId senderId, SeqNum seqNum, const Digest& stateDigest, bool stateIsStable)
    : MessageBase(senderId, MsgCode::Checkpoint, sizeof(CheckpointMsgHeader)) {
  b()->seqNum = seqNum;
  b()->stateDigest = stateDigest;
  b()->flags = 0;
  if (stateIsStable) b()->flags |= 0x1;
}

CheckpointMsg* CheckpointMsg::clone() {
  CheckpointMsg* c = new CheckpointMsg(senderId(), seqNumber(), digestOfState(), isStableState());

  return c;
}

void CheckpointMsg::validate(const ReplicasInfo& repInfo) const {
  Assert(type() == MsgCode::Checkpoint);
  Assert(senderId() != repInfo.myId());

  if (size() < sizeof(CheckpointMsgHeader) || (!repInfo.isIdOfReplica(senderId())) ||
      (seqNumber() % checkpointWindowSize != 0) || (digestOfState().isZero()))
    throw std::runtime_error(__PRETTY_FUNCTION__);

  // TODO(GG): consider to protect against messages that are larger than needed (here and in other messages)
}

MsgSize CheckpointMsg::maxSizeOfCheckpointMsg() { return sizeof(CheckpointMsgHeader); }

MsgSize CheckpointMsg::maxSizeOfCheckpointMsgInLocalBuffer() {
  return maxSizeOfCheckpointMsg() + sizeof(RawHeaderOfObjAndMsg);
}

}  // namespace impl
}  // namespace bftEngine
