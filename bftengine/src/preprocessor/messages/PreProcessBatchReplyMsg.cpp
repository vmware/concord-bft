// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "PreProcessBatchReplyMsg.hpp"
#include "SigManager.hpp"
#include "assertUtils.hpp"

namespace preprocessor {

using namespace std;
using namespace bftEngine;
using namespace bftEngine::impl;

PreProcessBatchReplyMsg::PreProcessBatchReplyMsg(uint16_t clientId,
                                                 NodeIdType senderId,
                                                 const PreProcessReplyMsgsList& batch,
                                                 const std::string& cid,
                                                 uint32_t repliesSize)
    : MessageBase(senderId, MsgCode::PreProcessBatchReply, 0, sizeof(Header) + repliesSize + cid.size()) {
  const uint32_t numOfMessagesInBatch = batch.size();
  setParams(senderId, clientId, numOfMessagesInBatch, repliesSize);
  msgBody()->cidLength = cid.size();
  auto position = body() + sizeof(Header);
  if (cid.size()) {
    memcpy(position, cid.c_str(), cid.size());
    position += cid.size();
  }
  for (auto const& reply : batch) {
    memcpy(position, reply->body(), reply->size());
    position += reply->size();
  }
  const uint64_t msgLength = sizeof(Header) + cid.size() + repliesSize;
  LOG_DEBUG(logger(), KVLOG(cid, clientId, senderId, numOfMessagesInBatch, repliesSize, msgLength));
}

void PreProcessBatchReplyMsg::validate(const ReplicasInfo& repInfo) const {
  if (size() < sizeof(Header) || size() < (sizeof(Header) + msgBody()->repliesSize))
    throw std::runtime_error(__PRETTY_FUNCTION__);

  if (type() != MsgCode::PreProcessBatchReply) {
    LOG_WARN(logger(), "Message type is incorrect" << KVLOG(type()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }

  if (senderId() == repInfo.myId()) {
    LOG_WARN(logger(), "Message sender is invalid" << KVLOG(senderId()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }
}

void PreProcessBatchReplyMsg::setParams(NodeIdType senderId,
                                        uint16_t clientId,
                                        uint32_t numOfMessagesInBatch,
                                        uint32_t repliesSize) {
  msgBody()->senderId = senderId;
  msgBody()->clientId = clientId;
  msgBody()->numOfMessagesInBatch = numOfMessagesInBatch;
  msgBody()->repliesSize = repliesSize;
}

std::string PreProcessBatchReplyMsg::getCid() const { return string(body() + sizeof(Header), msgBody()->cidLength); }

PreProcessReplyMsgsList& PreProcessBatchReplyMsg::getPreProcessReplyMsgs() {
  if (!preProcessReplyMsgsList_.empty()) return preProcessReplyMsgsList_;

  const auto& numOfMessagesInBatch = msgBody()->numOfMessagesInBatch;
  const string& batchCid = getCid();
  const auto& clientId = msgBody()->clientId;
  const auto& senderId = msgBody()->senderId;
  const auto sigLen = SigManager::instance()->getSigLength(msgBody()->senderId);
  char* dataPosition = body() + sizeof(Header) + msgBody()->cidLength;
  for (uint32_t i = 0; i < numOfMessagesInBatch; i++) {
    const auto& singleMsgHeader = *(PreProcessReplyMsg::Header*)dataPosition;
    const auto& reqSeqNum = singleMsgHeader.reqSeqNum;
    const char* sigPosition = dataPosition + sizeof(PreProcessReplyMsg::Header);
    const char* cidPosition = sigPosition + sigLen;
    const string cid(cidPosition, singleMsgHeader.cidLength);
    auto preProcessReplyMsg =
        make_unique<preprocessor::PreProcessReplyMsg>(senderId,
                                                      clientId,
                                                      singleMsgHeader.reqOffsetInBatch,
                                                      reqSeqNum,
                                                      singleMsgHeader.reqRetryId,
                                                      (const uint8_t*)&singleMsgHeader.resultsHash,
                                                      sigPosition,
                                                      cid,
                                                      (ReplyStatus)singleMsgHeader.status);
    preProcessReplyMsgsList_.push_back(move(preProcessReplyMsg));
    dataPosition += sizeof(PreProcessReplyMsg::Header) + sigLen + singleMsgHeader.cidLength;
  }
  LOG_DEBUG(logger(), KVLOG(batchCid, clientId, senderId, numOfMessagesInBatch, preProcessReplyMsgsList_.size()));
  return preProcessReplyMsgsList_;
}

}  // namespace preprocessor
