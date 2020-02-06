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
#include "ReplicaConfig.hpp"
#include "assertUtils.hpp"

namespace preprocessor {

using namespace std;
using namespace concord::util;
using namespace bftEngine;

uint16_t PreProcessReplyMsg::maxReplyMsgSize_ = 512;  // Actually, it is sizeof(PreProcessReplyMsgHeader) = 50 + 256

PreProcessReplyMsg::PreProcessReplyMsg(SigManagerSharedPtr sigManager,
                                       NodeIdType senderId,
                                       uint16_t clientId,
                                       uint64_t reqSeqNum)
    : MessageBase(senderId, MsgCode::PreProcessReply, maxReplyMsgSize_), sigManager_(sigManager) {
  setParams(senderId, clientId, reqSeqNum);
}

void PreProcessReplyMsg::validate(const ReplicasInfo& repInfo) const {
  Assert(type() == MsgCode::PreProcessReply);
  Assert(senderId() != repInfo.myId());

  const uint64_t headerSize = sizeof(PreProcessReplyMsgHeader);
  if (size() < headerSize || size() < headerSize + msgBody()->replyLength) throw runtime_error(__PRETTY_FUNCTION__);

  auto& msgHeader = *msgBody();
  uint16_t sigLen = sigManager_->getSigLength(msgHeader.senderId);

  if (size() < (sizeof(PreProcessReplyMsgHeader) + sigLen)) throw runtime_error(__PRETTY_FUNCTION__ + string(": size"));

  if (!sigManager_->verifySig(
          msgHeader.senderId, (char*)msgBody()->resultsHash, SHA3_256::SIZE_IN_BYTES, body() + headerSize, sigLen))
    throw runtime_error(__PRETTY_FUNCTION__ + string(": verifySig"));
}

void PreProcessReplyMsg::setParams(NodeIdType senderId, uint16_t clientId, ReqId reqSeqNum) {
  msgBody()->senderId = senderId;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->clientId = clientId;
  LOG_DEBUG(GL, "senderId=" << senderId << " clientId=" << clientId << " reqSeqNum=" << reqSeqNum);
}

void PreProcessReplyMsg::setupMsgBody(const char* buf, uint32_t bufLen) {
  const uint16_t sigSize = sigManager_->getMySigLength();
  const uint16_t headerSize = sizeof(PreProcessReplyMsgHeader);

  // Calculate pre-process result hash
  auto hash = SHA3_256().digest(buf, bufLen);
  memcpy(msgBody()->resultsHash, hash.data(), SHA3_256::SIZE_IN_BYTES);

  // Sign hash
  sigManager_->sign((char*)hash.data(), SHA3_256::SIZE_IN_BYTES, body() + headerSize, sigSize);
  msgSize_ = headerSize + sigSize;
  msgBody()->replyLength = sigSize;
}

}  // namespace preprocessor
