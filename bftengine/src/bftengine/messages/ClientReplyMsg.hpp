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
#include "ClientMsgs.hpp"

namespace bftEngine {
namespace impl {

// A ClientReplyMsg is simply a ClientReplyMessageHeader: header, followed by an opaque body of
// length header.replyLength.
class ClientReplyMsg : public MessageBase {
  static_assert((uint16_t)REPLY_MSG_TYPE == (uint16_t)MsgCode::ClientReply, "");
  static_assert(sizeof(ClientReplyMsgHeader::msgType) == sizeof(MessageBase::Header::msgType), "");
  static_assert(sizeof(ClientReplyMsgHeader::reqSeqNum) == sizeof(ReqId), "");
  static_assert(sizeof(ClientReplyMsgHeader::currentPrimaryId) == sizeof(ReplicaId), "");
  static_assert(sizeof(ClientReplyMsgHeader::result) == sizeof(uint32_t), "");
  static_assert(sizeof(ClientReplyMsgHeader) == 28, "ClientRequestMsgHeader is 28B");

 public:
  ClientReplyMsg(ReplicaId primaryId, ReqId reqSeqNum, ReplicaId replicaId);

  ClientReplyMsg(ReplicaId replicaId, ReqId reqSeqNum, char* reply, uint32_t replyLength);

  ClientReplyMsg(ReplicaId primaryId, ReqId reqSeqNum, ReplicaId replicaId, uint32_t result);

  ClientReplyMsg(ReplicaId replicaId, uint32_t replyLength);

  uint32_t maxReplyLength() const { return internalStorageSize() - sizeof(ClientReplyMsgHeader); }

  ReqId reqSeqNum() const { return b()->reqSeqNum; }

  ReplicaId currentPrimaryId() const { return b()->currentPrimaryId; }

  uint32_t replyLength() const { return b()->replyLength; }

  char* replyBuf() const { return body() + sizeof(ClientReplyMsgHeader); }

  void setReplyLength(uint32_t replyLength);

  void setReplicaSpecificInfoLength(uint32_t length);

  void setPrimaryId(ReplicaId primaryId);

  uint64_t debugHash() const;

  void validate(const ReplicasInfo&) const override;

  void setMsgSize(MsgSize size) { MessageBase::setMsgSize(size); }

  ClientReplyMsgHeader* b() const { return (ClientReplyMsgHeader*)msgBody_; }

 private:
  void setHeaderParameters(ReplicaId primaryId, ReqId reqSeqNum, uint32_t replyLength, uint32_t result);
};

}  // namespace impl
}  // namespace bftEngine
