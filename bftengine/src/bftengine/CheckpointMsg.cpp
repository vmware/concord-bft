// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "CheckpointMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

CheckpointMsg::CheckpointMsg(ReplicaId senderId,
                             SeqNum seqNum,
                             const Digest& stateDigest,
                             bool stateIsStable)
    : MessageBase(senderId, MsgCode::Checkpoint, sizeof(CheckpointMsgHeader)) {
  b()->seqNum = seqNum;
  b()->stateDigest = stateDigest;
  b()->flags = 0;
  if (stateIsStable) b()->flags |= 0x1;
}

CheckpointMsg* CheckpointMsg::clone() {
  CheckpointMsg* c = new CheckpointMsg(
      senderId(), seqNumber(), digestOfState(), isStableState());

  return c;
}

bool CheckpointMsg::ToActualMsgType(const ReplicasInfo& repInfo,
                                    MessageBase* inMsg,
                                    CheckpointMsg*& outMsg) {
  // Logger::printInfo("CheckpointMsg::ToActualMsgType - 1");

  Assert(inMsg->type() == MsgCode::Checkpoint);
  if (inMsg->size() < sizeof(CheckpointMsgHeader)) return false;

  CheckpointMsg* t = (CheckpointMsg*)inMsg;

  // Logger::printInfo("CheckpointMsg::ToActualMsgType - 2");

  if (t->senderId() == repInfo.myId())
    return false;  // TODO(GG): TBD- use assert instead

  // Logger::printInfo("CheckpointMsg::ToActualMsgType - 3");

  if (!repInfo.isIdOfReplica(t->senderId())) return false;

  // Logger::printInfo("CheckpointMsg::ToActualMsgType - 4");

  if (t->seqNumber() % checkpointWindowSize != 0) return false;

  // Logger::printInfo("CheckpointMsg::ToActualMsgType - 5");

  if (t->digestOfState().isZero()) return false;

  // Logger::printInfo("CheckpointMsg::ToActualMsgType - 6");

  // TODO(GG): consider to protect against messages that are larger than needed
  // (here and in other messages)

  outMsg = t;

  return true;
}
}  // namespace impl
}  // namespace bftEngine
