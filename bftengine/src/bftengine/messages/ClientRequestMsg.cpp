// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
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
  return (sizeof(ClientRequestMsgHeader) + r->spanContextSize + r->requestLength + r->cid_length);
}

uint32_t getRequestSizeTemp(const char* request)  // TODO(GG): change - TBD
{
  const ClientRequestMsgHeader* r = (ClientRequestMsgHeader*)request;
  return compRequestMsgSize(r);
}

// class ClientRequestMsg
ClientRequestMsg::ClientRequestMsg(NodeIdType sender,
                                   uint8_t flags,
                                   uint64_t reqSeqNum,
                                   uint32_t requestLength,
                                   const char* request,
                                   uint64_t reqTimeoutMilli,
                                   const std::string& cid,
                                   const concordUtils::SpanContext& spanContext)
    : MessageBase(sender,
                  MsgCode::ClientRequest,
                  spanContext.data().size(),
                  (sizeof(ClientRequestMsgHeader) + requestLength + cid.size())) {
  setParams(sender, reqSeqNum, requestLength, flags, reqTimeoutMilli, cid);
  char* position = body() + sizeof(ClientRequestMsgHeader);
  memcpy(position, spanContext.data().data(), spanContext.data().size());
  position += spanContext.data().size();
  memcpy(position, request, requestLength);
  position += requestLength;
  memcpy(position, cid.data(), cid.size());
}

ClientRequestMsg::ClientRequestMsg(NodeIdType sender)
    : MessageBase(sender, MsgCode::ClientRequest, 0, (sizeof(ClientRequestMsgHeader))) {
  msgBody()->flags &= EMPTY_CLIENT_REQ;
}

ClientRequestMsg::ClientRequestMsg(ClientRequestMsgHeader* body)
    : MessageBase(getSender(body), (MessageBase::Header*)body, compRequestMsgSize(body), false) {}

bool ClientRequestMsg::isReadOnly() const { return (msgBody()->flags & READ_ONLY_REQ) != 0; }

void ClientRequestMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(senderId() != repInfo.myId());
  if (size() < sizeof(ClientRequestMsgHeader) ||
      size() < (sizeof(ClientRequestMsgHeader) + msgBody()->requestLength + msgBody()->cid_length + spanContextSize()))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

void ClientRequestMsg::setParams(NodeIdType sender,
                                 ReqId reqSeqNum,
                                 uint32_t requestLength,
                                 uint8_t flags,
                                 uint64_t reqTimeoutMilli,
                                 const std::string& cid) {
  msgBody()->idOfClientProxy = sender;
  msgBody()->timeoutMilli = reqTimeoutMilli;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->requestLength = requestLength;
  msgBody()->flags = flags;
  msgBody()->cid_length = cid.size();
}

std::string ClientRequestMsg::getCid() const {
  return std::string(body() + sizeof(ClientRequestMsgHeader) + msgBody()->requestLength + spanContextSize(),
                     msgBody()->cid_length);
}

}  // namespace bftEngine::impl
