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
#include "EpochManager.hpp"

namespace bftEngine {
namespace impl {

FullCommitProofMsg::FullCommitProofMsg(ReplicaId senderId,
                                       ViewNum v,
                                       SeqNum s,
                                       const char* commitProofSig,
                                       uint16_t commitProofSigLength,
                                       const concordUtils::SpanContext& spanContext)
    : MessageBase(
          senderId, MsgCode::FullCommitProof, spanContext.data().size(), sizeof(Header) + commitProofSigLength) {
  b()->viewNum = v;
  b()->seqNum = s;
  b()->epochNum = EpochManager::instance().getSelfEpochNumber();
  b()->thresholSignatureLength = commitProofSigLength;
  auto position = body() + sizeof(Header);
  memcpy(position, spanContext.data().data(), spanContext.data().size());
  position += spanContext.data().size();
  memcpy(position, commitProofSig, commitProofSigLength);
}

void FullCommitProofMsg::validate(const ReplicasInfo& repInfo) const {
  if (size() < sizeof(Header) || senderId() == repInfo.myId() || !repInfo.isIdOfReplica(senderId()) ||
      (b()->epochNum != EpochManager::instance().getSelfEpochNumber()) ||
      size() < (sizeof(Header) + thresholSignatureLength() + spanContextSize()))
    throw std::runtime_error(__PRETTY_FUNCTION__);

  // TODO(GG): TBD - check something about the collectors identity (and in other similar messages)
}

}  // namespace impl
}  // namespace bftEngine
