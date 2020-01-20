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

#include <cstring>
#include <bftengine/SimpleClient.hpp>
#include "ClientRequestMsg.hpp"
#include "assertUtils.hpp"
#include "ReplicaConfig.hpp"

namespace bftEngine::impl {

// local helper functions

static uint16_t getSender(const ClientRequestMsgHeader* r) { return r->idOfClientProxy; }

static int32_t compRequestMsgSize(const ClientRequestMsgHeader* r) {
  return (sizeof(ClientRequestMsgHeader) + r->requestLength);
}

uint32_t getRequestSizeTemp(const char* request)  // TODO(GG): change - TBD
{
  const ClientRequestMsgHeader* r = (ClientRequestMsgHeader*)request;
  return compRequestMsgSize(r);
}

// class ClientRequestMsg

ClientRequestMsg::ClientRequestMsg(
    NodeIdType sender, bool isReadOnly, uint64_t reqSeqNum, uint32_t requestLength, const char* request)
    : MessageBase(sender, MsgCode::ClientRequest, (sizeof(ClientRequestMsgHeader) + requestLength)) {
  setParams(sender, reqSeqNum, requestLength, isReadOnly);
  memcpy(body() + sizeof(ClientRequestMsgHeader), request, requestLength);
}

ClientRequestMsg::ClientRequestMsg(NodeIdType sender)
    : MessageBase(sender, MsgCode::ClientRequest, ReplicaConfigSingleton::GetInstance().GetMaxExternalMessageSize()) {
  setParams(sender, 0, 0, false);
  setMsgSize(sizeof(ClientRequestMsgHeader));
}

ClientRequestMsg::ClientRequestMsg(ClientRequestMsgHeader* body)
    : MessageBase(getSender(body), (MessageBase::Header*)body, compRequestMsgSize(body), false) {}

void ClientRequestMsg::set(ReqId reqSeqNum, uint32_t requestLength, bool isReadOnly) {
  Assert(requestLength > 0);
  Assert(requestLength <= (internalStorageSize() - sizeof(ClientRequestMsgHeader)));

  setParams(reqSeqNum, requestLength, isReadOnly);
  setMsgSize(sizeof(ClientRequestMsgHeader) + requestLength);
}

void ClientRequestMsg::validate(const ReplicasInfo& repInfo) {
  if (size() < (sizeof(ClientRequestMsgHeader) + msgBody()->requestLength))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

void ClientRequestMsg::setParams(ReqId reqSeqNum, uint32_t requestLength, bool isReadOnly) {
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->requestLength = requestLength;
  if (isReadOnly)
    msgBody()->flags |= READ_ONLY_REQ;
  else
    msgBody()->flags = 0;
}

void ClientRequestMsg::setParams(NodeIdType sender, ReqId reqSeqNum, uint32_t requestLength, bool isReadOnly) {
  msgBody()->idOfClientProxy = sender;
  setParams(reqSeqNum, requestLength, isReadOnly);
}

}  // namespace bftEngine::impl
