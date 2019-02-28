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

#include "PartialExecProofMsg.hpp"
#include "assertUtils.hpp"
#include "ReplicasInfo.hpp"
#include "Crypto.hpp"

namespace bftEngine {
namespace impl {

PartialExecProofMsg::PartialExecProofMsg(ReplicaId senderId,
                                         ViewNum v,
                                         SeqNum s,
                                         Digest& digest,
                                         IThresholdSigner* thresholdSigner)
    : MessageBase(senderId,
                  MsgCode::PartialExecProof,
                  sizeof(PartialExecProofMsgHeader) +
                      thresholdSigner->requiredLengthForSignedData()) {
  uint16_t thresholSignatureLength =
      (uint16_t)thresholdSigner->requiredLengthForSignedData();

  b()->viewNum = v;
  b()->seqNum = s;
  b()->thresholSignatureLength = thresholSignatureLength;

  thresholdSigner->signData((const char*)(&(digest)),
                            sizeof(Digest),
                            body() + sizeof(PartialExecProofMsgHeader),
                            thresholSignatureLength);
}

bool PartialExecProofMsg::ToActualMsgType(const ReplicasInfo& repInfo,
                                          MessageBase* inMsg,
                                          PartialExecProofMsg*& outMsg) {
  Assert(inMsg->type() == MsgCode::PartialExecProof);
  if (inMsg->size() < sizeof(PartialExecProofMsgHeader)) return false;

  PartialExecProofMsg* t = (PartialExecProofMsg*)inMsg;

  uint16_t thresholSignatureLength = t->thresholSignatureLength();
  if ((size_t)(t->size()) <
      (sizeof(PartialExecProofMsgHeader) + thresholSignatureLength))
    return false;

  if (t->senderId() == repInfo.myId())
    return false;  // TODO(GG) - TBD: we should use Assert for this condition
                   // (also in other messages)

  if (!repInfo.isIdOfReplica(t->senderId())) return false;

  outMsg = (PartialExecProofMsg*)t;

  return true;
}

}  // namespace impl
}  // namespace bftEngine
