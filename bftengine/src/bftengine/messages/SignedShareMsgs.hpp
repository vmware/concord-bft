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

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

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
                                 std::shared_ptr<IThresholdSigner> thresholdSigner,
                                 const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});
  static SignedShareBase* create(int16_t type,
                                 ViewNum v,
                                 SeqNum s,
                                 ReplicaId senderId,
                                 const char* sig,
                                 uint16_t sigLen,
                                 const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});
  void _validate(const ReplicasInfo& repInfo, int16_t type) const;

  SignedShareBase(ReplicaId sender, int16_t type, const concordUtils::SpanContext& spanContext, size_t msgSize);

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(SignedShareBase)

  Header* b() const { return (Header*)msgBody_; }
};

///////////////////////////////////////////////////////////////////////////////
// PreparePartialMsg
///////////////////////////////////////////////////////////////////////////////

class PreparePartialMsg : public SignedShareBase {
  template <typename MessageT>
  friend size_t sizeOfHeader();

 public:
  static PreparePartialMsg* create(ViewNum v,
                                   SeqNum s,
                                   ReplicaId senderId,
                                   Digest& ppDigest,
                                   std::shared_ptr<IThresholdSigner> thresholdSigner,
                                   const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  PreparePartialMsg(MessageBase* msgBase) : SignedShareBase(msgBase) {}

  void validate(const ReplicasInfo&) const override;
};

///////////////////////////////////////////////////////////////////////////////
// PrepareFullMsg
///////////////////////////////////////////////////////////////////////////////

class PrepareFullMsg : public SignedShareBase {
  template <typename MessageT>
  friend size_t sizeOfHeader();

  template <typename MessageT>
  friend MsgSize maxMessageSize();

 public:
  static PrepareFullMsg* create(ViewNum v,
                                SeqNum s,
                                ReplicaId senderId,
                                const char* sig,
                                uint16_t sigLen,
                                const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  PrepareFullMsg(MessageBase* msgBase) : SignedShareBase(msgBase) {}

  void validate(const ReplicasInfo&) const override;
};

template <>
inline MsgSize maxMessageSize<PrepareFullMsg>() {
  return sizeOfHeader<PrepareFullMsg>() + maxSizeOfCombinedSignature + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

///////////////////////////////////////////////////////////////////////////////
// CommitPartialMsg
///////////////////////////////////////////////////////////////////////////////

class CommitPartialMsg : public SignedShareBase {
  template <typename MessageT>
  friend size_t sizeOfHeader();

 public:
  static CommitPartialMsg* create(ViewNum v,
                                  SeqNum s,
                                  ReplicaId senderId,
                                  Digest& ppDoubleDigest,
                                  std::shared_ptr<IThresholdSigner> thresholdSigner,
                                  const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  CommitPartialMsg(MessageBase* msgBase) : SignedShareBase(msgBase) {}

  void validate(const ReplicasInfo&) const override;
};

///////////////////////////////////////////////////////////////////////////////
// CommitFullMsg
///////////////////////////////////////////////////////////////////////////////

class CommitFullMsg : public SignedShareBase {
  template <typename MessageT>
  friend size_t sizeOfHeader();

  template <typename MessageT>
  friend MsgSize maxMessageSize();

 public:
  static CommitFullMsg* create(ViewNum v,
                               SeqNum s,
                               ReplicaId senderId,
                               const char* sig,
                               uint16_t sigLen,
                               const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  CommitFullMsg(MessageBase* msgBase) : SignedShareBase(msgBase) {}

  void validate(const ReplicasInfo&) const override;
};

template <>
inline MsgSize maxMessageSize<CommitFullMsg>() {
  return sizeOfHeader<CommitFullMsg>() + maxSizeOfCombinedSignature + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

}  // namespace impl
}  // namespace bftEngine
