// Concord
//
// Copyright (c) 2019-2021 VMware, Inc. All Rights Reserved.
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
#include "SigManager.hpp"

namespace preprocessor {

using namespace std;
using namespace concord::util;
using namespace bftEngine;
using namespace bftEngine::impl;

// maxReplyMsgSize_ = sizeof(Header) + sizeof(signature) + cid.size(), i.e 58 + 256 + up to 710 bytes of cid
uint16_t PreProcessReplyMsg::maxReplyMsgSize_ = 1024;

PreProcessReplyMsg::PreProcessReplyMsg(preprocessor::PreProcessorRecorder* histograms,
                                       NodeIdType senderId,
                                       uint16_t clientId,
                                       uint16_t reqOffsetInBatch,
                                       uint64_t reqSeqNum,
                                       uint64_t reqRetryId)
    : MessageBase(senderId, MsgCode::PreProcessReply, 0, maxReplyMsgSize_) {
  setPreProcessorHistograms(histograms);
  setParams(senderId, clientId, reqOffsetInBatch, reqSeqNum, reqRetryId);
}

void PreProcessReplyMsg::validate(const ReplicasInfo& repInfo) const {
  const uint64_t headerSize = sizeof(Header);
  if (size() < headerSize || size() < headerSize + msgBody()->replyLength) throw runtime_error(__PRETTY_FUNCTION__);

  if (type() != MsgCode::PreProcessReply) {
    LOG_ERROR(logger(), "Message type is incorrect" << KVLOG(type()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }

  auto& msgHeader = *msgBody();
  if (msgHeader.senderId == repInfo.myId()) {
    LOG_ERROR(logger(), "Message sender is invalid" << KVLOG(senderId()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }

  auto sigManager = SigManager::instance();
  uint16_t sigLen = sigManager->getSigLength(msgHeader.senderId);
  if (msgHeader.status == STATUS_GOOD) {
    if (size() < (sizeof(Header) + sigLen)) {
      LOG_ERROR(logger(),
                "Message size is too small" << KVLOG(
                    msgHeader.senderId, msgHeader.clientId, msgHeader.reqSeqNum, size(), sizeof(Header) + sigLen));
      throw runtime_error(__PRETTY_FUNCTION__ + string(": Message size is too small"));
    }
    concord::diagnostics::TimeRecorder scoped_timer(*preProcessorHistograms_->verifyPreProcessReplySig);
    if (!sigManager->verifySig(msgHeader.senderId,
                               (char*)msgBody()->resultsHash,
                               SHA3_256::SIZE_IN_BYTES,
                               (char*)msgBody() + headerSize,
                               sigLen))
      throw runtime_error(__PRETTY_FUNCTION__ + string(": verifySig failed"));
  }
}  // namespace preprocessor

void PreProcessReplyMsg::setParams(
    NodeIdType senderId, uint16_t clientId, uint16_t reqOffsetInBatch, ReqId reqSeqNum, uint64_t reqRetryId) {
  msgBody()->senderId = senderId;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->clientId = clientId;
  msgBody()->reqOffsetInBatch = reqOffsetInBatch;
  msgBody()->reqRetryId = reqRetryId;
  LOG_DEBUG(logger(), KVLOG(senderId, clientId, reqSeqNum, reqRetryId));
}

void PreProcessReplyMsg::setupMsgBody(const char* preProcessResultBuf,
                                      uint32_t preProcessResultBufLen,
                                      const std::string& cid,
                                      ReplyStatus status) {
  const uint16_t headerSize = sizeof(Header);
  uint16_t sigSize = 0;
  if (status == STATUS_GOOD) {
    auto sigManager = SigManager::instance();
    sigSize = sigManager->getMySigLength();
    SHA3_256::Digest hash;
    // Calculate pre-process result hash
    hash = SHA3_256().digest(preProcessResultBuf, preProcessResultBufLen);
    memcpy(msgBody()->resultsHash, hash.data(), SHA3_256::SIZE_IN_BYTES);
    {
      concord::diagnostics::TimeRecorder scoped_timer(*preProcessorHistograms_->signPreProcessReplyHash);
      sigManager->sign((char*)hash.data(), SHA3_256::SIZE_IN_BYTES, body() + headerSize, sigSize);
    }
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
                  preProcessResultBufLen,
                  msgSize_));
}

std::string PreProcessReplyMsg::getCid() const {
  return std::string(body() + msgSize_ - msgBody()->cidLength, msgBody()->cidLength);
}

}  // namespace preprocessor
