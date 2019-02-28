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

#include "StartSlowCommitMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

StartSlowCommitMsg::StartSlowCommitMsg(ReplicaId senderId, ViewNum v, SeqNum s)
    : MessageBase(senderId,
                  MsgCode::StartSlowCommit,
                  sizeof(StartSlowCommitMsgHeader)) {
  b()->viewNum = v;
  b()->seqNum = s;
}

bool StartSlowCommitMsg::ToActualMsgType(const ReplicasInfo& repInfo,
                                         MessageBase* inMsg,
                                         StartSlowCommitMsg*& outMsg) {
  Assert(inMsg->type() == MsgCode::StartSlowCommit);
  if (inMsg->size() < sizeof(StartSlowCommitMsgHeader)) return false;

  StartSlowCommitMsg* t = (StartSlowCommitMsg*)inMsg;

  if (repInfo.primaryOfView(t->viewNumber()) != t->senderId()) return false;

  outMsg = t;
  return true;
}

}  // namespace impl
}  // namespace bftEngine
