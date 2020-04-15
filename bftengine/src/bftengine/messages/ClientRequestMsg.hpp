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

#pragma once

#include "MessageBase.hpp"
#include "ReplicasInfo.hpp"
#include "ClientMsgs.hpp"

namespace bftEngine::impl {

class ClientRequestMsg : public MessageBase {
  // TODO(GG): requests should always be verified by the application layer !!!

  static_assert((uint16_t)REQUEST_MSG_TYPE == (uint16_t)MsgCode::ClientRequest, "");
  static_assert(sizeof(ClientRequestMsgHeader::msgType) == sizeof(MessageBase::Header::msgType), "");
  static_assert(sizeof(ClientRequestMsgHeader::idOfClientProxy) == sizeof(NodeIdType), "");
  static_assert(sizeof(ClientRequestMsgHeader::reqSeqNum) == sizeof(ReqId), "");
  static_assert(sizeof(ClientRequestMsgHeader) == 33, "ClientRequestMsgHeader size is 21B");

  // TODO(GG): more asserts

 public:
  ClientRequestMsg(NodeIdType sender,
                   uint8_t flags,
                   uint64_t reqSeqNum,
                   uint32_t requestLength,
                   const char* request,
                   uint64_t reqTimeoutMilli,
                   const std::string& cid = "",
                   const std::string& spanContext = "");

  ClientRequestMsg(ClientRequestMsgHeader* body);

  uint16_t clientProxyId() const { return msgBody()->idOfClientProxy; }

  bool isReadOnly() const;

  uint8_t flags() const { return msgBody()->flags; }

  ReqId requestSeqNum() const { return msgBody()->reqSeqNum; }

  uint32_t requestLength() const { return msgBody()->requestLength; }

  char* requestBuf() const { return body() + sizeof(ClientRequestMsgHeader) + spanContextSize(); }

  uint64_t requestTimeoutMilli() const { return msgBody()->timeoutMilli; }

  std::string getCid() const;
  void validate(const ReplicasInfo&) const override;

 protected:
  ClientRequestMsgHeader* msgBody() const { return ((ClientRequestMsgHeader*)msgBody_); }

 private:
  void setParams(NodeIdType sender,
                 ReqId reqSeqNum,
                 uint32_t requestLength,
                 uint8_t flags,
                 uint64_t reqTimeoutMilli,
                 const std::string& cid,
                 const std::string& spanContext);
};  // namespace bftEngine::impl

template <>
inline size_t sizeOfHeader<ClientRequestMsg>() {
  return sizeof(ClientRequestMsgHeader);
}

}  // namespace bftEngine::impl
