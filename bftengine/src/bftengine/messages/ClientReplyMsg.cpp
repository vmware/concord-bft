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

#include <string.h>
#include "ClientReplyMsg.hpp"
#include "assertUtils.hpp"
#include "ReplicaConfig.hpp"

namespace bftEngine {
namespace impl {

ClientReplyMsg::ClientReplyMsg(ReplicaId primaryId, ReqId reqSeqNum, ReplicaId replicaId)
    : MessageBase(replicaId, MsgCode::ClientReply, ReplicaConfig::instance().getmaxExternalMessageSize()) {
  b()->reqSeqNum = reqSeqNum;
  b()->currentPrimaryId = primaryId;
  b()->replyLength = 0;
  b()->replicaSpecificInfoLength = 0;
  setMsgSize(sizeof(ClientReplyMsgHeader));
}

ClientReplyMsg::ClientReplyMsg(ReplicaId replicaId, ReqId reqSeqNum, char* reply, uint32_t replyLength)
    : MessageBase(replicaId, MsgCode::ClientReply, sizeof(ClientReplyMsgHeader) + replyLength) {
  b()->reqSeqNum = reqSeqNum;
  b()->currentPrimaryId = 0;
  b()->replyLength = replyLength;

  memcpy(body() + sizeof(ClientReplyMsgHeader), reply, replyLength);
  setMsgSize(sizeof(ClientReplyMsgHeader) + replyLength);
}

ClientReplyMsg::ClientReplyMsg(ReplicaId replicaId, uint32_t replyLength)
    : MessageBase(replicaId, MsgCode::ClientReply, sizeof(ClientReplyMsgHeader) + replyLength) {
  b()->reqSeqNum = 0;
  b()->currentPrimaryId = 0;
  b()->replyLength = replyLength;

  setMsgSize(sizeof(ClientReplyMsgHeader) + replyLength);
}

void ClientReplyMsg::setReplyLength(uint32_t replyLength) {
  ConcordAssert(replyLength <= maxReplyLength());
  b()->replyLength = replyLength;
  setMsgSize(sizeof(ClientReplyMsgHeader) + replyLength);
}

void ClientReplyMsg::setReplicaSpecificInfoLength(uint32_t length) {
  ConcordAssert(length <= maxReplyLength());
  b()->replicaSpecificInfoLength = length;
}

void ClientReplyMsg::setPrimaryId(ReplicaId primaryId) { b()->currentPrimaryId = primaryId; }

void ClientReplyMsg::validate(const ReplicasInfo&) const {
  if (size() < ((int)sizeof(ClientReplyMsgHeader) + replyLength())) throw std::runtime_error(__PRETTY_FUNCTION__);

  // TODO(GG): the client should make sure that the message was actually sent by a valid replica
}

uint64_t ClientReplyMsg::debugHash() const {
  uint64_t retVal = 0;

  uint32_t replyLen = replyLength();
  ConcordAssert(replyLen > 0);

  uint32_t firstWordLen = replyLen % sizeof(uint64_t);
  if (firstWordLen == 0) firstWordLen = sizeof(uint64_t);

  ConcordAssert(((replyLen - firstWordLen) % sizeof(uint64_t)) == 0);
  uint32_t numberOfWords = ((replyLen - firstWordLen) / sizeof(uint64_t)) + 1;

  char* repBuf = replyBuf();

  // copy first word
  {
    char* p = (char*)&retVal;
    for (uint32_t i = 0; i < firstWordLen; i++) {
      p[i] = repBuf[i];
    }
  }

  if (numberOfWords > 1) {
    uint64_t* p = (uint64_t*)(repBuf + firstWordLen);
    for (uint32_t i = 0; i < numberOfWords - 1; i++) {
      retVal = retVal ^ p[i];
    }
  }

  return retVal;
}

}  // namespace impl
}  // namespace bftEngine
