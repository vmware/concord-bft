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

#include "MessageBase.hpp"
#include "ReplicaRestartReadyMsg.hpp"
#include "ReplicaConfig.hpp"

namespace bftEngine {
namespace impl {
struct NewViewElement;
class ReplicasRestartReadyProofMsg : public MessageBase {
 public:
  ReplicasRestartReadyProofMsg(ReplicaId senderId,
                               SeqNum seqNum,
                               EpochNum epochNum,
                               const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(ReplicasRestartReadyProofMsg)

  static ReplicasRestartReadyProofMsg* create(ReplicaId id,
                                              SeqNum s,
                                              EpochNum e,
                                              const concordUtils::SpanContext& spanContext = {});

  uint16_t idOfGeneratedReplica() const { return b()->genReplicaId; }

  void addElement(std::unique_ptr<ReplicaRestartReadyMsg>&);

  SeqNum seqNum() const { return b()->seqNum; }

  void finalizeMessage();

  const uint16_t elementsCount() const { return b()->elementsCount; }

  const uint32_t getBodySize() const;

  void validate(const ReplicasInfo&) const override;

  bool checkElements(const ReplicasInfo& repInfo, uint16_t sigSize) const;

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    uint16_t genReplicaId;
    SeqNum seqNum;
    EpochNum epochNum;
    uint16_t elementsCount;
    uint32_t locationAfterLast;  // if(elementsCount > 0) then it holds the location after last element
                                 // followed by the signature
                                 // TODO(NK): add epoch when support is added
  };
#pragma pack(pop)

  static_assert(sizeof(Header) == (6 + 2 + 8 + 8 + 2 + 4), "Header is 30B");
  Header* b() const { return (Header*)msgBody_; }
};
}  // namespace impl
}  // namespace bftEngine
