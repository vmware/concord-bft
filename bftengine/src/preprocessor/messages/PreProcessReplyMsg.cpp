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

#include "PreProcessReplyMsg.hpp"
#include "assertUtils.hpp"
#include <cstring>

namespace preprocessor {

PreProcessReplyMsg::PreProcessReplyMsg(
    NodeIdType senderId, uint64_t reqSeqNum, ViewNum viewNum, uint32_t replyLength, const char* reply)
    : MessageBase(senderId, MsgCode::PreProcessReply, (sizeof(PreProcessReplyMsgHeader) + replyLength)) {
  setParams(senderId, reqSeqNum, viewNum, replyLength);
  memcpy(body() + sizeof(PreProcessReplyMsgHeader), reply, replyLength);
}

void PreProcessReplyMsg::validate(const ReplicasInfo& repInfo) const {
  Assert(type() == MsgCode::PreProcessReply);
  Assert(senderId() != repInfo.myId());

  if (size() < sizeof(PreProcessReplyMsgHeader) + msgBody()->requestLength)
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

void PreProcessReplyMsg::setParams(NodeIdType senderId, ReqId reqSeqNum, ViewNum view, uint32_t replyLength) {
  msgBody()->senderId = senderId;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->viewNum = view;
  msgBody()->requestLength = replyLength;
}

}  // namespace preprocessor
