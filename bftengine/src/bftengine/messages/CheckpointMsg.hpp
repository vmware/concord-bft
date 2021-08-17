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

#include "MessageBase.hpp"
#include "Digest.hpp"
#include "ReplicaConfig.hpp"

namespace bftEngine {
namespace impl {

class CheckpointMsg : public MessageBase {
 public:
  CheckpointMsg(ReplicaId genReplica,
                SeqNum seqNum,
                const Digest& stateDigest,
                bool stateIsStable,
                const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(CheckpointMsg)

  SeqNum seqNumber() const { return b()->seqNum; }

  EpochNum epochNumber() const { return b()->epochNum; }

  Digest& digestOfState() const { return b()->stateDigest; }

  uint16_t idOfGeneratedReplica() const { return b()->genReplicaId; }

  bool isStableState() const { return (b()->flags & 0x1) != 0; }

  void setStateAsStable() { b()->flags |= 0x1; }

  void setEpochNumber(const EpochNum& e) { b()->epochNum = e; }

  void validate(const ReplicasInfo& repInfo) const override;

  void sign();

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    SeqNum seqNum;
    EpochNum epochNum;
    Digest stateDigest;
    ReplicaId genReplicaId;  // the replica that originally generated this message
    uint8_t flags;           // followed by a signature (by genReplicaId)
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 8 + 8 + DIGEST_SIZE + 2 + 1), "Header is 57B");

  Header* b() const { return (Header*)msgBody_; }
};

template <>
inline MsgSize maxMessageSize<CheckpointMsg>() {
  return ReplicaConfig::instance().getmaxExternalMessageSize() + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}
}  // namespace impl
}  // namespace bftEngine
