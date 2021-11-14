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

#pragma once

#include <cstdint>

#include "assertUtils.hpp"
#include "Digest.hpp"
#include "ReplicaConfig.hpp"
#include "MessageBase.hpp"
#include "OpenTracing.hpp"

namespace bftEngine {
namespace impl {

class ReplicaAsksToLeaveViewMsg : public MessageBase {
 public:
  enum class Reason : uint8_t { ClientRequestTimeout, NewPrimaryGetInChargeTimeout, PrimarySentBadPreProcessResult };

  ReplicaAsksToLeaveViewMsg(ReplicaId srcReplicaId,
                            ViewNum v,
                            Reason r,
                            uint16_t SignLen,
                            const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(ReplicaAsksToLeaveViewMsg)

  uint16_t idOfGeneratedReplica() const { return b()->genReplicaId; }

  ViewNum viewNumber() const { return b()->viewNum; }

  uint16_t signatureLen() const { return b()->sigLength; }

  Reason reason() const { return b()->reason; }

  char* signatureBody() const { return body() + sizeof(Header) + spanContextSize(); }

  static ReplicaAsksToLeaveViewMsg* create(ReplicaId senderId,
                                           ViewNum v,
                                           Reason r,
                                           const concordUtils::SpanContext& spanContext = {});

  void validate(const ReplicasInfo&) const override;

  bool shouldValidateAsync() const override { return true; }

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)

  struct Header : public MessageBase::Header {
    ReplicaId genReplicaId;
    ViewNum viewNum;
    EpochNum epochNum;
    Reason reason;
    uint16_t sigLength;
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 2 + 8 + 8 + 1 + 2), "Header is 27B");

  Header* b() const { return (Header*)msgBody_; }
};

template <>
inline MsgSize maxMessageSize<ReplicaAsksToLeaveViewMsg>() {
  return ReplicaConfig::instance().getmaxExternalMessageSize() + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

}  // namespace impl
}  // namespace bftEngine
