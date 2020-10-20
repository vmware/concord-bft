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

class ReplicaAsksToLeaveView : public MessageBase {
 public:
  enum class Reason : uint8_t { ClientRequestTimeout };

  ReplicaAsksToLeaveView(ReplicaId srcReplicaId,
                         ViewNum v,
                         Reason r,
                         uint16_t SignLen,
                         const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(ReplicaAsksToLeaveView)

  uint16_t idOfGeneratedReplica() const { return b()->genReplicaId; }

  ViewNum viewNumber() const { return b()->viewNum; }

  uint16_t signatureLen() const { return b()->sigLength; }

  char* signatureBody() const { return body() + sizeof(Header) + spanContextSize(); }

  static ReplicaAsksToLeaveView* create(ReplicaId senderId,
                                        ViewNum v,
                                        Reason r,
                                        const concordUtils::SpanContext& spanContext = {});

  void validate(const ReplicasInfo&) const override;

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)

  struct Header : public MessageBase::Header {
    ReplicaId genReplicaId;
    ViewNum viewNum;
    uint8_t reason;
    uint16_t sigLength;
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 2 + 8 + 1 + 2), "Header is 19B");

  Header* b() const { return (Header*)msgBody_; }
};

template <>
inline MsgSize maxMessageSize<ReplicaAsksToLeaveView>() {
  return ReplicaConfigSingleton::GetInstance().GetMaxExternalMessageSize() + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

}  // namespace impl
}  // namespace bftEngine
