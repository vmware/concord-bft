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

class NewViewMsg : public MessageBase {
 public:
  NewViewMsg(ReplicaId senderId,
             ViewNum newView,
             const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(NewViewMsg)

  void addElement(ReplicaId replicaId, Digest& viewChangeDigest);

  ViewNum newView() const { return b()->newViewNum; }

  void finalizeMessage(const ReplicasInfo& repInfo);

  void validate(const ReplicasInfo&) const override;

  bool includesViewChangeFromReplica(ReplicaId replicaId, const Digest& viewChangeReplica) const;

  const uint16_t elementsCount() const;

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;

    ViewNum newViewNum;  // the new view

    uint16_t elementsCount;  // TODO(GG): remove from header
                             // followed by a sequnce of 2f+2c+1 instances of Header
  };

  struct NewViewElement {
    ReplicaId replicaId;
    Digest viewChangeDigest;
  };
#pragma pack(pop)

  static_assert(sizeof(Header) == (6 + 8 + 2), "Header is 16B");
  static_assert(sizeof(NewViewElement) == (2 + DIGEST_SIZE), "Header is 34B");

  Header* b() const { return (Header*)msgBody_; }
  NewViewElement* elementsArray() const;
};

template <>
inline MsgSize maxMessageSize<NewViewMsg>() {
  return ReplicaConfig::instance().getmaxExternalMessageSize() + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

}  // namespace impl
}  // namespace bftEngine
