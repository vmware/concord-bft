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
#include "Crypto.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

///////////////////////////////////////////////////////////////////////////////
// SignedShareBase
///////////////////////////////////////////////////////////////////////////////

SignedShareBase::SignedShareBase(ReplicaId sender, int16_t type, const std::string& spanContext, size_t msgSize)
    : MessageBase(sender, type, spanContext.size(), msgSize) {}

SignedShareBase* SignedShareBase::create(int16_t type,
                                         ViewNum v,
                                         SeqNum s,
                                         ReplicaId senderId,
                                         Digest& digest,
                                         IThresholdSigner* thresholdSigner,
                                         const std::string& spanContext) {
  const size_t sigLen = thresholdSigner->requiredLengthForSignedData();
  size_t size = sizeof(SignedShareBaseHeader) + sigLen;

  SignedShareBase* m = new SignedShareBase(senderId, type, spanContext, size);

  m->b()->seqNumber = s;
  m->b()->viewNumber = v;
  m->b()->thresSigLength = (uint16_t)sigLen;

  Digest tmpDigest;
  Digest::calcCombination(digest, v, s, tmpDigest);

  auto position = m->body() + sizeof(SignedShareBaseHeader);
  std::memcpy(position, spanContext.data(), spanContext.size());
  position += spanContext.size();

  thresholdSigner->signData((const char*)(&(tmpDigest)), sizeof(Digest), position, sigLen);

  return m;
}

SignedShareBase* SignedShareBase::create(int16_t type,
                                         ViewNum v,
                                         SeqNum s,
                                         ReplicaId senderId,
                                         const char* sig,
                                         uint16_t sigLen,
                                         const std::string& spanContext) {
  size_t size = sizeof(SignedShareBaseHeader) + sigLen;

  SignedShareBase* m = new SignedShareBase(senderId, type, spanContext, size);

  m->b()->seqNumber = s;
  m->b()->viewNumber = v;
  m->b()->thresSigLength = sigLen;

  auto position = m->body() + sizeof(SignedShareBaseHeader);
  std::memcpy(position, spanContext.data(), spanContext.size());
  position += spanContext.size();
  memcpy(position, sig, sigLen);

  return m;
}

void SignedShareBase::_validate(const ReplicasInfo& repInfo, int16_t type_) const {
  Assert(type() == type_);
  if (size() < sizeof(SignedShareBaseHeader) ||
      size() < sizeof(SignedShareBaseHeader) + signatureLen() + spanContextSize() ||  // size
      senderId() == repInfo.myId() ||                                                 // sent from another replica
      !repInfo.isIdOfReplica(senderId()))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

std::string SignedShareBase::spanContext() const {
  return std::string(body() + sizeof(SignedShareBaseHeader), b()->header.spanContextSize);
}
///////////////////////////////////////////////////////////////////////////////
// PreparePartialMsg
///////////////////////////////////////////////////////////////////////////////

PreparePartialMsg* PreparePartialMsg::create(ViewNum v,
                                             SeqNum s,
                                             ReplicaId senderId,
                                             Digest& ppDigest,
                                             IThresholdSigner* thresholdSigner,
                                             const std::string& spanContext) {
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

MsgSize PrepareFullMsg::maxSizeOfPrepareFull() {
  return sizeof(SignedShareBaseHeader) + maxSizeOfCombinedSignature + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

MsgSize PrepareFullMsg::maxSizeOfPrepareFullInLocalBuffer() {
  return maxSizeOfPrepareFull() + sizeof(RawHeaderOfObjAndMsg);
}

PrepareFullMsg* PrepareFullMsg::create(
    ViewNum v, SeqNum s, ReplicaId senderId, const char* sig, uint16_t sigLen, const std::string& spanContext) {
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
                                           IThresholdSigner* thresholdSigner,
                                           const std::string& spanContext) {
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

MsgSize CommitFullMsg::maxSizeOfCommitFull() { return sizeof(SignedShareBaseHeader) + maxSizeOfCombinedSignature; }

MsgSize CommitFullMsg::maxSizeOfCommitFullInLocalBuffer() {
  return maxSizeOfCommitFull() + sizeof(RawHeaderOfObjAndMsg);
}

CommitFullMsg* CommitFullMsg::create(
    ViewNum v, SeqNum s, ReplicaId senderId, const char* sig, uint16_t sigLen, const std::string& spanContext) {
  return (CommitFullMsg*)SignedShareBase::create(MsgCode::CommitFull, v, s, senderId, sig, sigLen, spanContext);
}

void CommitFullMsg::validate(const ReplicasInfo& repInfo) const {
  SignedShareBase::_validate(repInfo, MsgCode::CommitFull);
}

}  // namespace impl
}  // namespace bftEngine
