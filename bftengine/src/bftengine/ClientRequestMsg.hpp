// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "MessageBase.hpp"
#include "ReplicasInfo.hpp"
#include "ClientMsgs.hpp"

namespace bftEngine {
namespace impl {

class ClientRequestMsg : public MessageBase {
  // TODO(GG): requests should always be verified by the application layer !!!

  static_assert((uint16_t)REQUEST_MSG_TYPE == (uint16_t)MsgCode::Request, "");
  static_assert(sizeof(ClientRequestMsgHeader::msgType) ==
                    sizeof(MessageBase::Header),
                "");
  static_assert(sizeof(ClientRequestMsgHeader::idOfClientProxy) ==
                    sizeof(NodeIdType),
                "");
  static_assert(sizeof(ClientRequestMsgHeader::reqSeqNum) == sizeof(ReqId), "");
  static_assert(sizeof(ClientRequestMsgHeader) == 17,
                "ClientRequestMsgHeader is 17B");

  // TODO(GG): more asserts

 public:
  ClientRequestMsg(NodeIdType sender,
                   bool isReadOnly,
                   uint64_t reqSeqNum,
                   uint32_t requestLength,
                   const char* request);

  ClientRequestMsg(NodeIdType sender);

  ClientRequestMsg(ClientRequestMsgHeader* body);

  uint32_t maxRequestLength() const {
    return internalStorageSize() - sizeof(ClientRequestMsgHeader);
  }

  uint16_t clientProxyId() const { return b()->idOfClientProxy; }

  bool isReadOnly() const { return (b()->flags & 0x1) != 0; }

  ReqId requestSeqNum() const { return b()->reqSeqNum; }

  uint32_t requestLength() const { return b()->requestLength; }

  char* requestBuf() const { return body() + sizeof(ClientRequestMsgHeader); }

  void set(ReqId reqSeqNum, uint32_t requestLength, bool isReadOnly);

  void setAsReadWrite();

  static bool ToActualMsgType(const ReplicasInfo& repInfo,
                              MessageBase* inMsg,
                              ClientRequestMsg*& outMsg);

 protected:
  ClientRequestMsgHeader* b() const {
    return ((ClientRequestMsgHeader*)msgBody_);
  }
};
}  // namespace impl
}  // namespace bftEngine
