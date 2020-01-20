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

#include <string.h>
#include "FullCommitProofMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

FullCommitProofMsg::FullCommitProofMsg(
    ReplicaId senderId, ViewNum v, SeqNum s, const char* commitProofSig, uint16_t commitProofSigLength)
    : MessageBase(senderId, MsgCode::FullCommitProof, sizeof(FullCommitProofMsgHeader) + commitProofSigLength) {
  b()->viewNum = v;
  b()->seqNum = s;
  b()->thresholSignatureLength = commitProofSigLength;
  memcpy(body() + sizeof(FullCommitProofMsgHeader), commitProofSig, commitProofSigLength);
}

void FullCommitProofMsg::validate(const ReplicasInfo& repInfo) {
  if (size() < sizeof(FullCommitProofMsgHeader) || senderId() == repInfo.myId() || !repInfo.isIdOfReplica(senderId()) ||
      size() < (sizeof(FullCommitProofMsgHeader) + thresholSignatureLength()))
    throw std::runtime_error(__PRETTY_FUNCTION__);

  // TODO(GG): TBD - check something about the collectors identity (and in other similar messages)
}

MsgSize FullCommitProofMsg::maxSizeOfFullCommitProofMsg() {
  return sizeof(FullCommitProofMsgHeader) + maxSizeOfCombinedSignature;
}

MsgSize FullCommitProofMsg::maxSizeOfFullCommitProofMsgInLocalBuffer() {
  return maxSizeOfFullCommitProofMsg() + sizeof(RawHeaderOfObjAndMsg);
}

}  // namespace impl
}  // namespace bftEngine
