// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "messages/MessageBase.hpp"
#include "ClientPreProcessRequestMsg.hpp"

namespace preprocessor {

#pragma pack(push, 1)
struct PreProcessRequestMsgHeader {
  MessageBase::Header header;
  ViewNum viewNum;
  SeqNum reqSeqNum;
  NodeIdType senderId;
  uint32_t requestLength;
};
#pragma pack(pop)

class PreProcessRequestMsg : public MessageBase {
 public:
  PreProcessRequestMsg(NodeIdType sender, uint64_t reqSeqNum, ViewNum view, uint32_t reqLength, const char* request);

  PreProcessRequestMsg(const ClientPreProcessReqMsgSharedPtr& msg, ViewNum currentView);

  void setParams(NodeIdType senderId, ReqId reqSeqNum, ViewNum view, uint32_t requestLength);

  static bool ToActualMsgType(MessageBase* inMsg, PreProcessRequestMsg*& outMsg);

 private:
  PreProcessRequestMsgHeader* msgBody() const { return ((PreProcessRequestMsgHeader*)msgBody_); }
};

}  // namespace preprocessor
