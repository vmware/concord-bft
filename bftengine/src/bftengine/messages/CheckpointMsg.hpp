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

namespace bftEngine {
namespace impl {

class CheckpointMsg : public MessageBase {
 public:
  CheckpointMsg(ReplicaId senderId,
                SeqNum seqNum,
                const Digest& stateDigest,
                bool stateIsStable,
                const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(CheckpointMsg)

  SeqNum seqNumber() const { return b()->seqNum; }

  Digest& digestOfState() const { return b()->stateDigest; }

  bool isStableState() const { return (b()->flags & 0x1) != 0; }

  void setStateAsStable() { b()->flags |= 0x1; }

  void validate(const ReplicasInfo& repInfo) const override;

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    SeqNum seqNum;
    Digest stateDigest;
    uint8_t flags;
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 8 + DIGEST_SIZE + 1), "Header is 47B");

  Header* b() const { return (Header*)msgBody_; }
};
}  // namespace impl
}  // namespace bftEngine
