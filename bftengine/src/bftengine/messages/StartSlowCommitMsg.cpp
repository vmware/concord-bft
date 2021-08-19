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

#include "StartSlowCommitMsg.hpp"
#include <cstring>
#include "assertUtils.hpp"
#include "EpochManager.hpp"

namespace bftEngine {
namespace impl {

StartSlowCommitMsg::StartSlowCommitMsg(ReplicaId senderId,
                                       ViewNum v,
                                       SeqNum s,
                                       const concordUtils::SpanContext& spanContext)
    : MessageBase(senderId, MsgCode::StartSlowCommit, spanContext.data().size(), sizeof(Header)) {
  b()->viewNum = v;
  b()->seqNum = s;
  b()->epochNum = EpochManager::instance().getSelfEpochNumber();
  std::memcpy(body() + sizeof(Header), spanContext.data().data(), spanContext.data().size());
}

void StartSlowCommitMsg::validate(const ReplicasInfo& repInfo) const {
  if (size() < sizeof(Header) + spanContextSize() || b()->epochNum != EpochManager::instance().getSelfEpochNumber() ||
      repInfo.primaryOfView(viewNumber()) != senderId())
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

}  // namespace impl
}  // namespace bftEngine
