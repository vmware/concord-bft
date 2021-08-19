// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <cstring>
#include "OpenTracing.hpp"
#include "ReplicaRestartReadyMsg.hpp"
#include "SysConsts.hpp"
#include "SigManager.hpp"
#include "EpochManager.hpp"

namespace bftEngine {
namespace impl {

ReplicaRestartReadyMsg::ReplicaRestartReadyMsg(ReplicaId srcReplicaId,
                                               SeqNum s,
                                               uint16_t sigLen,
                                               const concordUtils::SpanContext& spanContext)
    : MessageBase(srcReplicaId, MsgCode::ReplicaRestartReady, spanContext.data().size(), sizeof(Header) + sigLen) {
  b()->genReplicaId = srcReplicaId;
  b()->seqNum = s;
  b()->epochNum = EpochManager::instance().getSelfEpochNumber();
  b()->sigLength = sigLen;
  std::memcpy(body() + sizeof(Header), spanContext.data().data(), spanContext.data().size());
}

ReplicaRestartReadyMsg* ReplicaRestartReadyMsg::create(ReplicaId senderId,
                                                       SeqNum s,
                                                       const concordUtils::SpanContext& spanContext) {
  auto sigManager = SigManager::instance();
  const size_t sigLen = sigManager->getMySigLength();

  ReplicaRestartReadyMsg* m = new ReplicaRestartReadyMsg(senderId, s, sigLen, spanContext);

  auto position = m->body() + sizeof(Header);
  std::memcpy(position, spanContext.data().data(), spanContext.data().size());
  position += spanContext.data().size();

  sigManager->sign(m->body(), sizeof(Header), position, sigLen);

  return m;
}

void ReplicaRestartReadyMsg::validate(const ReplicasInfo& repInfo) const {
  auto idOfSenderReplica = idOfGeneratedReplica();
  auto sigManager = SigManager::instance();
  auto dataSize = sizeof(Header) + spanContextSize();
  if (size() < dataSize || !repInfo.isIdOfReplica(idOfSenderReplica) ||
      b()->epochNum != EpochManager::instance().getSelfEpochNumber())
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": basic validations"));

  uint16_t sigLen = sigManager->getSigLength(idOfSenderReplica);
  if (size() < dataSize + sigLen) throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": size"));

  if (!sigManager->verifySig(idOfSenderReplica, body(), sizeof(Header), body() + dataSize, sigLen))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": verifySig"));
}

}  // namespace impl
}  // namespace bftEngine
