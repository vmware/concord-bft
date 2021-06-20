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

#include "PreProcessBatchRequestMsg.hpp"
#include "assertUtils.hpp"

namespace preprocessor {

using namespace std;

PreProcessBatchRequestMsg::PreProcessBatchRequestMsg(RequestType reqType,
                                                     NodeIdType clientId,
                                                     NodeIdType senderId,
                                                     const PreProcessReqMsgsList& batch,
                                                     const string& cid,
                                                     uint32_t requestsSize)
    : MessageBase(senderId, MsgCode::PreProcessBatchRequest, 0, sizeof(Header) + requestsSize + cid.size()) {
  const uint32_t numOfMessagesInBatch = batch.size();
  setParams(clientId, senderId, reqType, numOfMessagesInBatch);
  msgBody()->cidLength = cid.size();
  auto position = body() + sizeof(Header);
  if (cid.size()) {
    memcpy(position, cid.c_str(), cid.size());
    position += cid.size();
  }
  for (auto const& req : batch) {
    memcpy(position, req->body(), req->size());
    position += req->size();
  }
  const uint64_t msgLength = sizeof(Header) + requestsSize + cid.size();
  LOG_DEBUG(logger(), KVLOG(reqType, cid, clientId, senderId, numOfMessagesInBatch, requestsSize, msgLength));
}

void PreProcessBatchRequestMsg::validate(const ReplicasInfo& repInfo) const {
  if (size() < sizeof(Header) || size() < (sizeof(Header) + msgBody()->requestsSize))
    throw std::runtime_error(__PRETTY_FUNCTION__);

  if (type() != MsgCode::PreProcessBatchRequest) {
    LOG_ERROR(logger(), "Message type is incorrect" << KVLOG(type()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }

  if (senderId() == repInfo.myId()) {
    LOG_ERROR(logger(), "Message sender is invalid" << KVLOG(senderId()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }
}

void PreProcessBatchRequestMsg::setParams(uint16_t clientId,
                                          NodeIdType senderId,
                                          RequestType reqType,
                                          uint32_t numOfMessagesInBatch) {
  auto* header = msgBody();
  header->reqType = reqType;
  header->clientId = clientId;
  header->senderId = senderId;
  header->numOfMessagesInBatch = numOfMessagesInBatch;
}

string PreProcessBatchRequestMsg::getCid() const { return string(body() + sizeof(Header), msgBody()->cidLength); }

PreProcessReqMsgsList& PreProcessBatchRequestMsg::getPreProcessRequestMsgs() {
  if (!preProcessReqMsgsList_.empty()) return preProcessReqMsgsList_;

  const auto& numOfMessagesInBatch = msgBody()->numOfMessagesInBatch;
  const string& batchCid = getCid();
  char* dataPosition = body() + sizeof(Header) + msgBody()->cidLength;
  for (uint32_t i = 0; i < numOfMessagesInBatch; i++) {
    const auto& singleMsgHeader = *(PreProcessRequestMsg::Header*)dataPosition;
    const char* spanDataPosition = dataPosition + sizeof(PreProcessRequestMsg::Header);
    const char* requestDataPosition = spanDataPosition + singleMsgHeader.spanContextSize;
    const char* cidPosition = requestDataPosition + singleMsgHeader.requestLength;
    const char* requestSignaturePosition =
        (singleMsgHeader.reqSignatureLength > 0) ? (cidPosition + singleMsgHeader.cidLength) : nullptr;
    const concordUtils::SpanContext spanContext(string(spanDataPosition, singleMsgHeader.spanContextSize));
    const string cid(cidPosition, singleMsgHeader.cidLength);
    auto preProcessReqMsg = make_unique<preprocessor::PreProcessRequestMsg>(singleMsgHeader.reqType,
                                                                            senderId(),
                                                                            clientId(),
                                                                            singleMsgHeader.reqOffsetInBatch,
                                                                            singleMsgHeader.reqSeqNum,
                                                                            singleMsgHeader.reqRetryId,
                                                                            singleMsgHeader.requestLength,
                                                                            requestDataPosition,
                                                                            cid,
                                                                            requestSignaturePosition,
                                                                            singleMsgHeader.reqSignatureLength,
                                                                            spanContext);

    LOG_DEBUG(logger(),
              "Single request info:" << KVLOG(
                  batchCid, preProcessReqMsg->clientId(), preProcessReqMsg->getCid(), preProcessReqMsg->reqSeqNum()));
    preProcessReqMsgsList_.push_back(move(preProcessReqMsg));
    dataPosition += sizeof(PreProcessRequestMsg::Header) + singleMsgHeader.spanContextSize +
                    singleMsgHeader.requestLength + singleMsgHeader.cidLength + singleMsgHeader.reqSignatureLength;
  }
  LOG_DEBUG(logger(), KVLOG(batchCid, msgBody()->clientId, preProcessReqMsgsList_.size(), numOfMessagesInBatch));
  return preProcessReqMsgsList_;
}

}  // namespace preprocessor
