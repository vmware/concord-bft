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

#include "PreProcessRequestMsg.hpp"
#include "assertUtils.hpp"
#include <cstring>

namespace preprocessor {

PreProcessRequestMsg::PreProcessRequestMsg(
    NodeIdType senderId, uint64_t reqSeqNum, ViewNum currentView, uint32_t requestLength, const char* request)
    : MessageBase(senderId, MsgCode::PreProcessRequest, (sizeof(PreProcessRequestMsgHeader) + requestLength)) {
  setParams(senderId, reqSeqNum, currentView, requestLength);
  memcpy(body() + sizeof(PreProcessRequestMsgHeader), request, requestLength);
}

PreProcessRequestMsg::PreProcessRequestMsg(const ClientPreProcessReqMsgSharedPtr& msg, ViewNum currentView)
    : MessageBase(msg->senderId(), (Header*)msg->body(), msg->size(), false) {
  msgBody()->header.msgType = bftEngine::impl::MsgCode::PreProcessRequest;
  setParams(msg->senderId(), msg->requestSeqNum(), currentView, msg->size());
}

bool PreProcessRequestMsg::ToActualMsgType(MessageBase* inMsg, PreProcessRequestMsg*& outMsg) {
  Assert(inMsg->type() == MsgCode::PreProcessRequest);
  if (inMsg->size() < sizeof(PreProcessRequestMsgHeader)) return false;

  auto* msg = (PreProcessRequestMsg*)inMsg;
  if (msg->size() < (sizeof(PreProcessRequestMsgHeader) + msg->msgBody()->requestLength)) return false;

  outMsg = msg;
  return true;
}

void PreProcessRequestMsg::setParams(NodeIdType senderId, ReqId reqSeqNum, ViewNum view, uint32_t requestLength) {
  msgBody()->senderId = senderId;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->viewNum = view;
  msgBody()->requestLength = requestLength;
}

}  // namespace preprocessor
