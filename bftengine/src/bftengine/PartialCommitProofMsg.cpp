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

#include "PartialCommitProofMsg.hpp"
#include "assertUtils.hpp"
#include "ReplicasInfo.hpp"
#include "Crypto.hpp"

namespace bftEngine {
namespace impl {

PartialCommitProofMsg::PartialCommitProofMsg(ReplicaId senderId,
                                             ViewNum v,
                                             SeqNum s,
                                             CommitPath commitPath,
                                             Digest& digest,
                                             IThresholdSigner* thresholdSigner)
    : MessageBase(senderId,
                  MsgCode::PartialCommitProof,
                  sizeof(PartialCommitProofMsgHeader) +
                      thresholdSigner->requiredLengthForSignedData()) {
  uint16_t thresholSignatureLength =
      (uint16_t)thresholdSigner->requiredLengthForSignedData();

  b()->viewNum = v;
  b()->seqNum = s;
  b()->commitPath = commitPath;
  b()->thresholSignatureLength = thresholSignatureLength;

  thresholdSigner->signData((const char*)(&(digest)),
                            sizeof(Digest),
                            body() + sizeof(PartialCommitProofMsgHeader),
                            thresholSignatureLength);
}

bool PartialCommitProofMsg::ToActualMsgType(const ReplicasInfo& repInfo,
                                            MessageBase* inMsg,
                                            PartialCommitProofMsg*& outMsg) {
  Assert(inMsg->type() == MsgCode::PartialCommitProof);
  if (inMsg->size() < sizeof(PartialCommitProofMsgHeader)) return false;

  PartialCommitProofMsg* t = (PartialCommitProofMsg*)inMsg;

  if (t->senderId() == repInfo.myId())
    return false;  // TODO(GG) - TBD: we should use Assert for this condition
                   // (also in other messages)

  if (!repInfo.isIdOfReplica(t->senderId())) return false;

  if ((t->commitPath() == CommitPath::FAST_WITH_THRESHOLD) &&
      (repInfo.cVal() == 0))
    return false;

  if (t->commitPath() == CommitPath::SLOW) return false;

  uint16_t thresholSignatureLength = t->thresholSignatureLength();
  if (t->size() <
      (sizeof(PartialCommitProofMsgHeader) + thresholSignatureLength))
    return false;

  if (!repInfo.isCollectorForPartialProofs(t->viewNumber(), t->seqNumber()))
    return false;

  outMsg = (PartialCommitProofMsg*)t;

  return true;
}

}  // namespace impl
}  // namespace bftEngine
