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
#include "InstallReadyMsg.hpp"
#include "SysConsts.hpp"
#include "Crypto.hpp"
#include "SigManager.hpp"
#include "EpochManager.hpp"

namespace bftEngine {
namespace impl {

InstallReadyMsg::InstallReadyMsg(
    ReplicaId srcReplicaId, SeqNum s, uint16_t verStrLen, uint16_t sigLen, const concordUtils::SpanContext& spanContext)
    : MessageBase(srcReplicaId, MsgCode::InstallReady, spanContext.data().size(), sizeof(Header) + sigLen + verStrLen) {
  b()->genReplicaId = srcReplicaId;
  b()->seqNum = s;
  b()->epochNum = EpochManager::instance().getSelfEpochNumber();
  b()->versionStrLen = verStrLen;
  b()->sigLength = sigLen;
}

InstallReadyMsg* InstallReadyMsg::create(ReplicaId senderId,
                                         SeqNum s,
                                         const std::string& version,
                                         const concordUtils::SpanContext& spanContext) {
  auto sigManager = SigManager::instance();
  const size_t sigLen = sigManager->getMySigLength();
  InstallReadyMsg* m = new InstallReadyMsg(senderId, s, version.size(), sigLen, spanContext);
  auto position = m->body() + sizeof(Header);
  std::memcpy(position, spanContext.data().data(), spanContext.data().size());
  position += spanContext.data().size();
  std::memcpy(position, version.data(), version.size());
  position += version.size();
  sigManager->sign(m->body(), sizeof(Header) + spanContext.data().size() + version.size(), position, sigLen);
  return m;
}

std::string InstallReadyMsg::getVersion() const {
  return std::string(body() + sizeof(Header) + spanContextSize(), versionStrLen());
}

void InstallReadyMsg::validate(const ReplicasInfo& repInfo) const {
  auto idOfSenderReplica = idOfGeneratedReplica();
  auto sigManager = SigManager::instance();
  auto dataSize = sizeof(Header) + spanContextSize() + b()->versionStrLen;
  if (size() < dataSize || !repInfo.isIdOfReplica(idOfSenderReplica) || b()->versionStrLen == 0 ||
      b()->epochNum != EpochManager::instance().getSelfEpochNumber())
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": basic validations"));

  uint16_t sigLen = sigManager->getSigLength(idOfSenderReplica);
  if (size() < dataSize + sigLen) throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": size"));

  if (!sigManager->verifySig(idOfSenderReplica, body(), dataSize, body() + dataSize, sigLen))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": verifySig"));
}

}  // namespace impl
}  // namespace bftEngine
