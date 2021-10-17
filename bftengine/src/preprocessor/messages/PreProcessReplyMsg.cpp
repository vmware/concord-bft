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
#include "endianness.hpp"

namespace preprocessor {

using namespace std;
using namespace concord::util;
using namespace bftEngine;
using namespace bftEngine::impl;

// maxReplyMsgSize_ = sizeof(Header) + sizeof(signature) + cid.size(), i.e 77 + 256 + 256 + rest is for cid
uint16_t PreProcessReplyMsg::maxReplyMsgSize_ = 1280;

PreProcessReplyMsg::PreProcessReplyMsg(NodeIdType senderId,
                                       uint16_t clientId,
                                       uint16_t reqOffsetInBatch,
                                       uint64_t reqSeqNum,
                                       uint64_t reqRetryId,
                                       const char* preProcessResultBuf,
                                       uint32_t preProcessResultBufLen,
                                       const std::string& cid,
                                       ReplyStatus status,
                                       uint64_t blockId)
    : MessageBase(senderId, MsgCode::PreProcessReply, 0, maxReplyMsgSize_) {
  setParams(senderId, clientId, reqOffsetInBatch, reqSeqNum, reqRetryId, status, blockId);
  setupMsgBody(preProcessResultBuf, preProcessResultBufLen, cid, status);
}

// Used by PreProcessBatchReplyMsg while retrieving PreProcessReplyMsgs from the batch
PreProcessReplyMsg::PreProcessReplyMsg(NodeIdType senderId,
                                       uint16_t clientId,
                                       uint16_t reqOffsetInBatch,
                                       uint64_t reqSeqNum,
                                       uint64_t reqRetryId,
                                       const uint8_t* resultsHash,
                                       const char* signature,
                                       const std::string& cid,
                                       ReplyStatus status,
                                       uint64_t blockId,
                                       const char* blockSignature)
    : MessageBase(senderId, MsgCode::PreProcessReply, 0, maxReplyMsgSize_) {
  setParams(senderId, clientId, reqOffsetInBatch, reqSeqNum, reqRetryId, status, blockId);
  setupMsgBody(resultsHash, signature, cid, blockSignature);
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
    auto blockIdStr = concordUtils::toBigEndianStringBuffer(msgBody()->blockId);
    if (!sigManager->verifySig(
            msgHeader.senderId, blockIdStr.data(), blockIdStr.size(), (char*)msgBody() + headerSize + sigLen, sigLen))
      throw runtime_error(__PRETTY_FUNCTION__ + string(": verifySig of blockid failed"));
  }
}

std::vector<char> PreProcessReplyMsg::getResultHashSignature() const { return getSignature(0); }

std::vector<char> PreProcessReplyMsg::getBlockIdSignature() const {
  const auto& msgHeader = *msgBody();
  auto sigManager = SigManager::instance();
  uint16_t sigLen = sigManager->getSigLength(msgHeader.senderId);
  return getSignature(sigLen);
}

std::vector<char> PreProcessReplyMsg::getSignature(uint64_t offset) const {
  const uint64_t headerSize = sizeof(Header);
  const auto& msgHeader = *msgBody();
  auto sigManager = SigManager::instance();
  uint16_t sigLen = sigManager->getSigLength(msgHeader.senderId);
  return std::vector<char>((char*)msgBody() + headerSize + offset, (char*)msgBody() + headerSize + offset + sigLen);
}

void PreProcessReplyMsg::setParams(NodeIdType senderId,
                                   uint16_t clientId,
                                   uint16_t reqOffsetInBatch,
                                   ReqId reqSeqNum,
                                   uint64_t reqRetryId,
                                   ReplyStatus status,
                                   uint64_t blockId) {
  msgBody()->senderId = senderId;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->clientId = clientId;
  msgBody()->reqOffsetInBatch = reqOffsetInBatch;
  msgBody()->reqRetryId = reqRetryId;
  msgBody()->status = status;
  msgBody()->blockId = blockId;
  LOG_DEBUG(logger(), KVLOG(senderId, clientId, reqSeqNum, reqRetryId, status, blockId));
}

void PreProcessReplyMsg::setLeftMsgParams(const string& cid, uint16_t sigSize) {
  const uint16_t headerSize = sizeof(Header);
  msgBody()->cidLength = cid.size();
  // Place the cid after the result_signature and blockid_signature
  memcpy(body() + headerSize + 2 * sigSize, cid.c_str(), cid.size());
  msgBody()->replyLength = 2 * sigSize;
  msgSize_ = headerSize + 2 * sigSize + msgBody()->cidLength;
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), KVLOG(msgBody()->senderId, msgBody()->clientId, msgBody()->reqSeqNum, sigSize, cid, msgSize_));
}

void PreProcessReplyMsg::setupMsgBody(const char* preProcessResultBuf,
                                      uint32_t preProcessResultBufLen,
                                      const string& cid,
                                      ReplyStatus status) {
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
      sigManager->sign((char*)hash.data(), SHA3_256::SIZE_IN_BYTES, body() + sizeof(Header), sigSize);
    }
    {
      concord::diagnostics::TimeRecorder scoped_timer(*preProcessorHistograms_->signBlockId);
      auto blockIdStr = concordUtils::toBigEndianStringBuffer(msgBody()->blockId);
      sigManager->sign(blockIdStr.data(), blockIdStr.size(), body() + sizeof(Header) + sigSize, sigSize);
    }
  }
  setLeftMsgParams(cid, sigSize);
}

// Used by PreProcessBatchReplyMsg while retrieving PreProcessReplyMsgs from the batch
void PreProcessReplyMsg::setupMsgBody(const uint8_t* resultsHash,
                                      const char* signature,
                                      const string& cid,
                                      const char* blockSignature) {
  memcpy(msgBody()->resultsHash, resultsHash, SHA3_256::SIZE_IN_BYTES);
  const uint16_t sigLen = SigManager::instance()->getMySigLength();
  memcpy(body() + sizeof(Header), signature, sigLen);
  memcpy(body() + sizeof(Header) + sigLen, blockSignature, sigLen);
  setLeftMsgParams(cid, sigLen);
}

std::string PreProcessReplyMsg::getCid() const {
  return std::string(body() + msgSize_ - msgBody()->cidLength, msgBody()->cidLength);
}

}  // namespace preprocessor
