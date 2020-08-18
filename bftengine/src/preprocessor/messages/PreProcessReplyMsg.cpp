// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
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

// maxReplyMsgSize_ = sizeof(Header) + sizeof(signature) + cid.size(), i.e 58 + 256 + up to 710 bytes of cid
uint16_t PreProcessReplyMsg::maxReplyMsgSize_ = 1024;

PreProcessReplyMsg::PreProcessReplyMsg(SigManagerSharedPtr sigManager,
                                       NodeIdType senderId,
                                       uint16_t clientId,
                                       uint64_t reqSeqNum)
    : MessageBase(senderId, MsgCode::PreProcessReply, 0, maxReplyMsgSize_), sigManager_(sigManager) {
  setParams(senderId, clientId, reqSeqNum);
}

void PreProcessReplyMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(type() == MsgCode::PreProcessReply);

  const uint64_t headerSize = sizeof(Header);
  if (size() < headerSize || size() < headerSize + msgBody()->replyLength) throw runtime_error(__PRETTY_FUNCTION__);

  auto& msgHeader = *msgBody();
  ConcordAssert(msgHeader.senderId != repInfo.myId());

  uint16_t sigLen = sigManager_->getSigLength(msgHeader.senderId);
  if (size() < (sizeof(Header) + sigLen)) throw runtime_error(__PRETTY_FUNCTION__ + string(": size"));

  if (!sigManager_->verifySig(msgHeader.senderId,
                              (char*)msgBody()->resultsHash,
                              SHA3_256::SIZE_IN_BYTES,
                              (char*)msgBody() + headerSize,
                              sigLen))
    throw runtime_error(__PRETTY_FUNCTION__ + string(": verifySig"));
}

void PreProcessReplyMsg::setParams(NodeIdType senderId, uint16_t clientId, ReqId reqSeqNum) {
  msgBody()->senderId = senderId;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->clientId = clientId;
  LOG_DEBUG(logger(), "senderId=" << senderId << " clientId=" << clientId << " reqSeqNum=" << reqSeqNum);
}

void PreProcessReplyMsg::setupMsgBody(const char* buf, uint32_t bufLen, const std::string& cid) {
  const uint16_t sigSize = sigManager_->getMySigLength();
  const uint16_t headerSize = sizeof(Header);

  // Calculate pre-process result hash
  auto hash = SHA3_256().digest(buf, bufLen);
  memcpy(msgBody()->resultsHash, hash.data(), SHA3_256::SIZE_IN_BYTES);

  // Sign hash
  sigManager_->sign((char*)hash.data(), SHA3_256::SIZE_IN_BYTES, body() + headerSize, sigSize);
  memcpy(body() + headerSize + sigSize, cid.c_str(), cid.size());
  msgBody()->cidLength = cid.size();
  msgSize_ = headerSize + sigSize + msgBody()->cidLength;
  msgBody()->replyLength = sigSize;
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(),
            "senderId=" << msgBody()->senderId << " clientId=" << msgBody()->clientId
                        << " reqSeqNum=" << msgBody()->reqSeqNum << " headerSize=" << headerSize
                        << " sigSize=" << sigSize << " cidSize=" << cid.size() << " msgSize_=" << msgSize_);
}
std::string PreProcessReplyMsg::getCid() const {
  return std::string(body() + msgSize_ - msgBody()->cidLength, msgBody()->cidLength);
}

}  // namespace preprocessor
