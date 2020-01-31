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

class AskForCheckpointMsg : public MessageBase {
 public:
  static MsgSize maxSizeOfAskForCheckpointMsg() { return sizeof(AskForCheckpointMsgHeader); }

  static MsgSize maxSizeOfAskForCheckpointMsgInLocalBuffer() {
    return maxSizeOfAskForCheckpointMsg() + sizeof(RawHeaderOfObjAndMsg);
  }

  AskForCheckpointMsg(ReplicaId senderId)
      : MessageBase(senderId, MsgCode::AskForCheckpoint, sizeof(AskForCheckpointMsgHeader)) {}

  AskForCheckpointMsg* clone() { return new AskForCheckpointMsg(*this); }

  void validate(const ReplicasInfo& repInfo) override {
    Assert(type() == MsgCode::AskForCheckpoint);
    Assert(senderId() != repInfo.myId());

    if (size() > sizeof(AskForCheckpointMsgHeader)) throw std::runtime_error(__PRETTY_FUNCTION__);
  }

 protected:
#pragma pack(push, 1)
  struct AskForCheckpointMsgHeader {
    MessageBase::Header header;
  };
#pragma pack(pop)
  static_assert(sizeof(AskForCheckpointMsgHeader) == sizeof(MessageBase::Header), "AskForCheckpointMsgHeader is 2B");

  AskForCheckpointMsgHeader* b() const { return (AskForCheckpointMsgHeader*)msgBody_; }
};
}  // namespace impl
}  // namespace bftEngine
