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
#include "digest.hpp"
#include "OpenTracing.hpp"
#include "assertUtils.hpp"

namespace bftEngine::impl {

class AskForCheckpointMsg : public MessageBase {
 public:
  AskForCheckpointMsg(ReplicaId senderId, const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{})
      : MessageBase(senderId, MsgCode::AskForCheckpoint, spanContext.data().size(), sizeof(Header)) {
    char* position = body() + sizeof(Header);
    memcpy(position, spanContext.data().data(), spanContext.data().size());
  }

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(AskForCheckpointMsg)

  AskForCheckpointMsg* clone() { return new AskForCheckpointMsg(*this); }

  void validate(const ReplicasInfo& repInfo) const override {
    ConcordAssert(type() == MsgCode::AskForCheckpoint);
    ConcordAssert(senderId() != repInfo.myId());

    if (size() > sizeof(Header) + spanContextSize()) {
      throw std::runtime_error(__PRETTY_FUNCTION__);
    }
  }

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == sizeof(MessageBase::Header), "Header is 2B");

  Header* b() const { return (Header*)msgBody_; }
};
}  // namespace bftEngine::impl
