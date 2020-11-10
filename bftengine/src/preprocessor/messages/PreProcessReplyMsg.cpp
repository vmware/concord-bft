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

PreProcessReplyMsg::PreProcessReplyMsg(
    SigManagerSharedPtr sigManager, NodeIdType senderId, uint16_t clientId, uint64_t reqSeqNum, uint64_t reqRetryId)
    : MessageBase(senderId, MsgCode::PreProcessReply, 0, maxReplyMsgSize_), sigManager_(sigManager) {
  setParams(senderId, clientId, reqSeqNum, reqRetryId);
}

void PreProcessReplyMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(type() == MsgCode::PreProcessReply);

  const uint64_t headerSize = sizeof(Header);
  if (size() < headerSize || size() < headerSize + msgBody()->replyLength) throw runtime_error(__PRETTY_FUNCTION__);

  auto& msgHeader = *msgBody();
  ConcordAssert(msgHeader.senderId != repInfo.myId());

  if (sigManager_ == nullptr) throw runtime_error(__PRETTY_FUNCTION__ + string(": verifySig"));

  uint16_t sigLen = sigManager_->getSigLength(msgHeader.senderId);
  if (size() < (sizeof(Header) + sigLen)) throw runtime_error(__PRETTY_FUNCTION__ + string(": size"));

  if (!sigManager_->verifySig(msgHeader.senderId,
                              (char*)msgBody()->resultsHash,
                              SHA3_256::SIZE_IN_BYTES,
                              (char*)msgBody() + headerSize,
                              sigLen))
    throw runtime_error(__PRETTY_FUNCTION__ + string(": verifySig"));
}

void PreProcessReplyMsg::setParams(NodeIdType senderId, uint16_t clientId, ReqId reqSeqNum, uint64_t reqRetryId) {
  msgBody()->senderId = senderId;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->clientId = clientId;
  msgBody()->reqRetryId = reqRetryId;
  LOG_DEBUG(logger(), KVLOG(senderId, clientId, reqSeqNum, reqRetryId));
}

void PreProcessReplyMsg::setupMsgBody(const char* buf, uint32_t bufLen, const std::string& cid, ReplyStatus status) {
  const uint16_t headerSize = sizeof(Header);
  uint16_t sigSize = 0;
  if (status == STATUS_GOOD) {
    sigSize = sigManager_->getMySigLength();

    // Calculate pre-process result hash
    const auto& hash = SHA3_256().digest(buf, bufLen);
    memcpy(msgBody()->resultsHash, hash.data(), SHA3_256::SIZE_IN_BYTES);

    // Sign hash
    sigManager_->sign((char*)hash.data(), SHA3_256::SIZE_IN_BYTES, body() + headerSize, sigSize);
  }
  memcpy(body() + headerSize + sigSize, cid.c_str(), cid.size());
  msgBody()->status = status;
  msgBody()->cidLength = cid.size();
  msgBody()->replyLength = sigSize;
  msgSize_ = headerSize + sigSize + msgBody()->cidLength;

  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(),
            KVLOG(status,
                  msgBody()->senderId,
                  msgBody()->clientId,
                  msgBody()->reqSeqNum,
                  headerSize,
                  sigSize,
                  cid.size(),
                  msgSize_));
}

std::string PreProcessReplyMsg::getCid() const {
  return std::string(body() + msgSize_ - msgBody()->cidLength, msgBody()->cidLength);
}

}  // namespace preprocessor
