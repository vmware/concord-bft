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

#include "SignedShareMsgs.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "kvstream.h"
#include "EpochManager.hpp"
#include "threshsign/IThresholdSigner.h"

namespace bftEngine {
namespace impl {

///////////////////////////////////////////////////////////////////////////////
// SignedShareBase
///////////////////////////////////////////////////////////////////////////////

SignedShareBase::SignedShareBase(ReplicaId sender,
                                 int16_t type,
                                 const concordUtils::SpanContext& spanContext,
                                 size_t msgSize)
    : MessageBase(sender, type, spanContext.data().size(), msgSize) {}

SignedShareBase* SignedShareBase::create(int16_t type,
                                         ViewNum v,
                                         SeqNum s,
                                         ReplicaId senderId,
                                         Digest& digest,
                                         std::shared_ptr<IThresholdSigner> thresholdSigner,
                                         const concordUtils::SpanContext& spanContext) {
  LOG_TRACE(THRESHSIGN_LOG, KVLOG(type, v, s, senderId));
  const size_t sigLen = thresholdSigner->requiredLengthForSignedData();
  size_t size = sizeof(Header) + sigLen;

  SignedShareBase* m = new SignedShareBase(senderId, type, spanContext, size);

  m->b()->seqNumber = s;
  m->b()->viewNumber = v;
  m->b()->epochNum = EpochManager::instance().getSelfEpochNumber();
  m->b()->thresSigLength = (uint16_t)sigLen;

  Digest tmpDigest;
  Digest::calcCombination(digest, v, s, tmpDigest);

  auto position = m->body() + sizeof(Header);
  std::memcpy(position, spanContext.data().data(), spanContext.data().size());
  position += spanContext.data().size();

  thresholdSigner->signData((const char*)(&(tmpDigest)), sizeof(Digest), position, sigLen);

  return m;
}

SignedShareBase* SignedShareBase::create(int16_t type,
                                         ViewNum v,
                                         SeqNum s,
                                         ReplicaId senderId,
                                         const char* sig,
                                         uint16_t sigLen,
                                         const concordUtils::SpanContext& spanContext) {
  size_t size = sizeof(Header) + sigLen;

  SignedShareBase* m = new SignedShareBase(senderId, type, spanContext, size);

  m->b()->seqNumber = s;
  m->b()->viewNumber = v;
  m->b()->epochNum = EpochManager::instance().getSelfEpochNumber();
  m->b()->thresSigLength = sigLen;

  auto position = m->body() + sizeof(Header);
  std::memcpy(position, spanContext.data().data(), spanContext.data().size());
  position += spanContext.data().size();
  memcpy(position, sig, sigLen);

  return m;
}

void SignedShareBase::_validate(const ReplicasInfo& repInfo, int16_t type_) const {
  ConcordAssert(type() == type_);
  if (size() < sizeof(Header) + spanContextSize() ||
      size() < sizeof(Header) + signatureLen() + spanContextSize() ||  // size
      senderId() == repInfo.myId() ||                                  // sent from another replica
      b()->epochNum != EpochManager::instance().getSelfEpochNumber() || !repInfo.isIdOfReplica(senderId()))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

///////////////////////////////////////////////////////////////////////////////
// PreparePartialMsg
///////////////////////////////////////////////////////////////////////////////

PreparePartialMsg* PreparePartialMsg::create(ViewNum v,
                                             SeqNum s,
                                             ReplicaId senderId,
                                             Digest& ppDigest,
                                             std::shared_ptr<IThresholdSigner> thresholdSigner,
                                             const concordUtils::SpanContext& spanContext) {
  return (PreparePartialMsg*)SignedShareBase::create(
      MsgCode::PreparePartial, v, s, senderId, ppDigest, thresholdSigner, spanContext);
}

void PreparePartialMsg::validate(const ReplicasInfo& repInfo) const {
  SignedShareBase::_validate(repInfo, MsgCode::PreparePartial);

  if (repInfo.myId() != repInfo.primaryOfView(viewNumber()))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": the primary is the collector of PreparePartialMsg"));
}

///////////////////////////////////////////////////////////////////////////////
// PrepareFullMsg
///////////////////////////////////////////////////////////////////////////////

PrepareFullMsg* PrepareFullMsg::create(ViewNum v,
                                       SeqNum s,
                                       ReplicaId senderId,
                                       const char* sig,
                                       uint16_t sigLen,
                                       const concordUtils::SpanContext& spanContext) {
  return (PrepareFullMsg*)SignedShareBase::create(MsgCode::PrepareFull, v, s, senderId, sig, sigLen, spanContext);
}

void PrepareFullMsg::validate(const ReplicasInfo& repInfo) const {
  SignedShareBase::_validate(repInfo, MsgCode::PrepareFull);
}

///////////////////////////////////////////////////////////////////////////////
// CommitPartialMsg
///////////////////////////////////////////////////////////////////////////////

CommitPartialMsg* CommitPartialMsg::create(ViewNum v,
                                           SeqNum s,
                                           ReplicaId senderId,
                                           Digest& ppDoubleDigest,
                                           std::shared_ptr<IThresholdSigner> thresholdSigner,
                                           const concordUtils::SpanContext& spanContext) {
  return (CommitPartialMsg*)SignedShareBase::create(
      MsgCode::CommitPartial, v, s, senderId, ppDoubleDigest, thresholdSigner, spanContext);
}

void CommitPartialMsg::validate(const ReplicasInfo& repInfo) const {
  SignedShareBase::_validate(repInfo, MsgCode::CommitPartial);

  if (repInfo.myId() != repInfo.primaryOfView(viewNumber()))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": the primary is the collector of CommitPartialMsg"));
}

///////////////////////////////////////////////////////////////////////////////
// CommitFullMsg
///////////////////////////////////////////////////////////////////////////////

CommitFullMsg* CommitFullMsg::create(ViewNum v,
                                     SeqNum s,
                                     ReplicaId senderId,
                                     const char* sig,
                                     uint16_t sigLen,
                                     const concordUtils::SpanContext& spanContext) {
  return (CommitFullMsg*)SignedShareBase::create(MsgCode::CommitFull, v, s, senderId, sig, sigLen, spanContext);
}

void CommitFullMsg::validate(const ReplicasInfo& repInfo) const {
  SignedShareBase::_validate(repInfo, MsgCode::CommitFull);
}

}  // namespace impl
}  // namespace bftEngine
