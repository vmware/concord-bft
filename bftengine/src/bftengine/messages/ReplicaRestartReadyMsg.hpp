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

#pragma once

#include <cstdint>
#include "assert.h"
#include "assertUtils.hpp"
#include "ReplicaConfig.hpp"
#include "MessageBase.hpp"
#include "OpenTracing.hpp"

namespace bftEngine {
namespace impl {
class ReplicasRestartReadyProofMsg;

class ReplicaRestartReadyMsg : public MessageBase {
 public:
  ReplicaRestartReadyMsg(ReplicaId srcReplicaId,
                         SeqNum seqNum,
                         EpochNum e,
                         uint16_t sigLen,
                         const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(ReplicaRestartReadyMsg)

  uint16_t idOfGeneratedReplica() const { return b()->genReplicaId; }

  uint16_t signatureLen() const { return b()->sigLength; }

  SeqNum seqNum() const { return b()->seqNum; }

  char* signatureBody() const { return body() + sizeof(Header) + spanContextSize(); }

  static ReplicaRestartReadyMsg* create(ReplicaId senderId,
                                        SeqNum s,
                                        EpochNum e,
                                        const concordUtils::SpanContext& spanContext = {});

  void validate(const ReplicasInfo&) const override;

  static uint32_t sizeOfHeader() { return sizeof(Header); }

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();
  friend ReplicasRestartReadyProofMsg;

#pragma pack(push, 1)

  struct Header : public MessageBase::Header {
    ReplicaId genReplicaId;
    SeqNum seqNum;
    EpochNum epochNum;
    uint16_t sigLength;
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 2 + 8 + 8 + 2), "Header is 26B");

  Header* b() const { return (Header*)msgBody_; }
};

template <>
inline MsgSize maxMessageSize<ReplicaRestartReadyMsg>() {
  return ReplicaConfig::instance().getmaxExternalMessageSize() + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

}  // namespace impl
}  // namespace bftEngine
