// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <cstring>

#include <bftengine/ClientMsgs.hpp>
#include "OpenTracing.hpp"
#include "ReplicaAsksToLeaveViewMsg.hpp"
#include "SysConsts.hpp"
#include "Crypto.hpp"
#include "ViewsManager.hpp"

namespace bftEngine {
namespace impl {

ReplicaAsksToLeaveViewMsg::ReplicaAsksToLeaveViewMsg(
    ReplicaId srcReplicaId, ViewNum v, Reason r, uint16_t sigLen, const concordUtils::SpanContext& spanContext)
    : MessageBase(srcReplicaId, MsgCode::ReplicaAsksToLeaveView, spanContext.data().size(), sizeof(Header) + sigLen) {
  b()->genReplicaId = srcReplicaId;
  b()->viewNum = v;
  b()->reason = r;
  b()->sigLength = sigLen;
  std::memcpy(body() + sizeof(Header), spanContext.data().data(), spanContext.data().size());
}

ReplicaAsksToLeaveViewMsg* ReplicaAsksToLeaveViewMsg::create(ReplicaId senderId,
                                                             ViewNum v,
                                                             Reason r,
                                                             const concordUtils::SpanContext& spanContext) {
  auto sigManager = SigManager::getInstance();
  const size_t sigLen = sigManager->getMySigLength();

  ReplicaAsksToLeaveViewMsg* m = new ReplicaAsksToLeaveViewMsg(senderId, v, r, sigLen, spanContext);

  auto position = m->body() + sizeof(Header);
  std::memcpy(position, spanContext.data().data(), spanContext.data().size());
  position += spanContext.data().size();

  sigManager->sign(m->body(), sizeof(Header), position, sigLen);

  return m;
}

void ReplicaAsksToLeaveViewMsg::validate(const ReplicasInfo& repInfo) const {
  auto sigManager = SigManager::getInstance();
  auto totalSize = sizeof(Header) + spanContextSize();
  if (size() < totalSize || !repInfo.isIdOfReplica(idOfGeneratedReplica()))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": basic validations"));

  uint16_t sigLen = sigManager->getSigLength(idOfGeneratedReplica());
  if (size() < totalSize + sigLen) throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": size"));

  if (!sigManager->verifySig(idOfGeneratedReplica(), body(), sizeof(Header), body() + totalSize, sigLen))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": verifySig"));
}

}  // namespace impl
}  // namespace bftEngine
