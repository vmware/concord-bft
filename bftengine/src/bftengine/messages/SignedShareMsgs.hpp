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

#pragma once

#include "Digest.hpp"
#include "MessageBase.hpp"

class IThresholdSigner;

namespace bftEngine {
namespace impl {

///////////////////////////////////////////////////////////////////////////////
// SignedShareBase
///////////////////////////////////////////////////////////////////////////////

class SignedShareBase : public MessageBase {
 public:
  ViewNum viewNumber() const { return b()->viewNumber; }

  SeqNum seqNumber() const { return b()->seqNumber; }

  uint16_t signatureLen() const { return b()->thresSigLength; }

  char* signatureBody() const { return body() + sizeof(Header) + spanContextSize(); }

  virtual std::string spanContext() const;

 protected:
#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    ViewNum viewNumber;
    SeqNum seqNumber;
    uint16_t thresSigLength;
    // Followed by threshold signature of <viewNumber, seqNumber, and the preprepare digest>
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 8 + 8 + 2), "Header is 62B");

  static SignedShareBase* create(int16_t type,
                                 ViewNum v,
                                 SeqNum s,
                                 ReplicaId senderId,
                                 Digest& digest,
                                 IThresholdSigner* thresholdSigner,
                                 const std::string& spanContext = "");
  static SignedShareBase* create(int16_t type,
                                 ViewNum v,
                                 SeqNum s,
                                 ReplicaId senderId,
                                 const char* sig,
                                 uint16_t sigLen,
                                 const std::string& spanContext = "");
  void _validate(const ReplicasInfo& repInfo, int16_t type) const;

  SignedShareBase(ReplicaId sender, int16_t type, const std::string& spanContext, size_t msgSize);

  Header* b() const { return (Header*)msgBody_; }
};

///////////////////////////////////////////////////////////////////////////////
// PreparePartialMsg
///////////////////////////////////////////////////////////////////////////////

class PreparePartialMsg : public SignedShareBase {
 public:
  static PreparePartialMsg* create(ViewNum v,
                                   SeqNum s,
                                   ReplicaId senderId,
                                   Digest& ppDigest,
                                   IThresholdSigner* thresholdSigner,
                                   const std::string& spanContext = "");
  void validate(const ReplicasInfo&) const override;
};

///////////////////////////////////////////////////////////////////////////////
// PrepareFullMsg
///////////////////////////////////////////////////////////////////////////////

class PrepareFullMsg : public SignedShareBase {
  template <typename MessageT>
  friend MsgSize maxMessageSize();

 public:
  static MsgSize maxSizeOfPrepareFullInLocalBuffer();
  static PrepareFullMsg* create(
      ViewNum v, SeqNum s, ReplicaId senderId, const char* sig, uint16_t sigLen, const std::string& spanContext = "");
  void validate(const ReplicasInfo&) const override;
};

template <>
inline MsgSize maxMessageSize<PrepareFullMsg>() {
  return sizeof(PrepareFullMsg::Header) + maxSizeOfCombinedSignature + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

///////////////////////////////////////////////////////////////////////////////
// CommitPartialMsg
///////////////////////////////////////////////////////////////////////////////

class CommitPartialMsg : public SignedShareBase {
 public:
  static CommitPartialMsg* create(ViewNum v,
                                  SeqNum s,
                                  ReplicaId senderId,
                                  Digest& ppDoubleDigest,
                                  IThresholdSigner* thresholdSigner,
                                  const std::string& spanContext = "");
  void validate(const ReplicasInfo&) const override;
};

///////////////////////////////////////////////////////////////////////////////
// CommitFullMsg
///////////////////////////////////////////////////////////////////////////////

class CommitFullMsg : public SignedShareBase {
  template <typename MessageT>
  friend MsgSize maxMessageSize();

 public:
  static MsgSize maxSizeOfCommitFullInLocalBuffer();
  static CommitFullMsg* create(
      ViewNum v, SeqNum s, ReplicaId senderId, const char* sig, uint16_t sigLen, const std::string& spanContext = "");
  void validate(const ReplicasInfo&) const override;
};

template <>
inline MsgSize maxMessageSize<CommitFullMsg>() {
  return sizeof(CommitFullMsg::Header) + maxSizeOfCombinedSignature + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

}  // namespace impl
}  // namespace bftEngine
