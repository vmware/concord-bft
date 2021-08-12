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

namespace bftEngine {
namespace impl {

///////////////////////////////////////////////////////////////////////////////
// SimpleAckMsg
///////////////////////////////////////////////////////////////////////////////

class SimpleAckMsg : public MessageBase {
 public:
  SimpleAckMsg(SeqNum s, ViewNum v, ReplicaId senderId, uint64_t ackData);

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(SimpleAckMsg)

  ViewNum viewNumber() const { return b()->viewNum; }

  SeqNum seqNumber() const { return b()->seqNum; }

  uint64_t ackData() const { return b()->ackData; }

  void validate(const ReplicasInfo&) const override;

 protected:
#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    SeqNum seqNum;
    ViewNum viewNum;
    EpochNum epochNum;
    uint64_t ackData;
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 8 + 8 + 8 + 8), "Header is 38B");

  Header* b() const { return (Header*)msgBody_; }
};

}  // namespace impl
}  // namespace bftEngine
