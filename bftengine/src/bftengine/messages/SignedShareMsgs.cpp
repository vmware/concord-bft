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

SignedShareBase::SignedShareBase(ReplicaId sender, int16_t type, size_t msgSize) : MessageBase(sender, type, msgSize) {}

SignedShareBase* SignedShareBase::create(
    int16_t type, ViewNum v, SeqNum s, ReplicaId senderId, Digest& digest, IThresholdSigner* thresholdSigner) {
  const size_t sigLen = thresholdSigner->requiredLengthForSignedData();
  size_t size = sizeof(SignedShareBaseHeader) + sigLen;

  SignedShareBase* m = new SignedShareBase(senderId, type, size);

  m->b()->seqNumber = s;
  m->b()->viewNumber = v;
  m->b()->thresSigLength = (uint16_t)sigLen;

  Digest tmpDigest;
  Digest::calcCombination(digest, v, s, tmpDigest);

  thresholdSigner->signData(
      (const char*)(&(tmpDigest)), sizeof(Digest), m->body() + sizeof(SignedShareBaseHeader), sigLen);

  return m;
}

SignedShareBase* SignedShareBase::create(
    int16_t type, ViewNum v, SeqNum s, ReplicaId senderId, const char* sig, uint16_t sigLen) {
  size_t size = sizeof(SignedShareBaseHeader) + sigLen;

  SignedShareBase* m = new SignedShareBase(senderId, type, size);

  m->b()->seqNumber = s;
  m->b()->viewNumber = v;
  m->b()->thresSigLength = sigLen;

  memcpy(m->body() + sizeof(SignedShareBaseHeader), sig, sigLen);

  return m;
}

void SignedShareBase::validate(const ReplicasInfo& repInfo, int16_t type_) {
  Assert(type() == type_);
  if (size() < sizeof(SignedShareBaseHeader) || size() < sizeof(SignedShareBaseHeader) + signatureLen() ||  // size
      senderId() == repInfo.myId() ||  // sent from another replica
      !repInfo.isIdOfReplica(senderId()))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

///////////////////////////////////////////////////////////////////////////////
// PreparePartialMsg
///////////////////////////////////////////////////////////////////////////////

PreparePartialMsg* PreparePartialMsg::create(
    ViewNum v, SeqNum s, ReplicaId senderId, Digest& ppDigest, IThresholdSigner* thresholdSigner) {
  return (PreparePartialMsg*)SignedShareBase::create(
      MsgCode::PreparePartial, v, s, senderId, ppDigest, thresholdSigner);
}

void PreparePartialMsg::validate(const ReplicasInfo& repInfo) {
  SignedShareBase::validate(repInfo, MsgCode::PreparePartial);

  if (repInfo.myId() != repInfo.primaryOfView(viewNumber()))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": the primary is the collector of PreparePartialMsg"));
}

///////////////////////////////////////////////////////////////////////////////
// PrepareFullMsg
///////////////////////////////////////////////////////////////////////////////

MsgSize PrepareFullMsg::maxSizeOfPrepareFull() { return sizeof(SignedShareBaseHeader) + maxSizeOfCombinedSignature; }

MsgSize PrepareFullMsg::maxSizeOfPrepareFullInLocalBuffer() {
  return maxSizeOfPrepareFull() + sizeof(RawHeaderOfObjAndMsg);
}

PrepareFullMsg* PrepareFullMsg::create(ViewNum v, SeqNum s, ReplicaId senderId, const char* sig, uint16_t sigLen) {
  return (PrepareFullMsg*)SignedShareBase::create(MsgCode::PrepareFull, v, s, senderId, sig, sigLen);
}

void PrepareFullMsg::validate(const ReplicasInfo& repInfo) { SignedShareBase::validate(repInfo, MsgCode::PrepareFull); }

///////////////////////////////////////////////////////////////////////////////
// CommitPartialMsg
///////////////////////////////////////////////////////////////////////////////

CommitPartialMsg* CommitPartialMsg::create(
    ViewNum v, SeqNum s, ReplicaId senderId, Digest& ppDoubleDigest, IThresholdSigner* thresholdSigner) {
  return (CommitPartialMsg*)SignedShareBase::create(
      MsgCode::CommitPartial, v, s, senderId, ppDoubleDigest, thresholdSigner);
}

void CommitPartialMsg::validate(const ReplicasInfo& repInfo) {
  SignedShareBase::validate(repInfo, MsgCode::CommitPartial);

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

CommitFullMsg* CommitFullMsg::create(ViewNum v, SeqNum s, int16_t senderId, const char* sig, uint16_t sigLen) {
  return (CommitFullMsg*)SignedShareBase::create(MsgCode::CommitFull, v, s, senderId, sig, sigLen);
}

void CommitFullMsg::validate(const ReplicasInfo& repInfo) { SignedShareBase::validate(repInfo, MsgCode::CommitFull); }

}  // namespace impl
}  // namespace bftEngine
