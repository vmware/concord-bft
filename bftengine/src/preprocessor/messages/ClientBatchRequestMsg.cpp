// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ClientBatchRequestMsg.hpp"
#include "assertUtils.hpp"
#include "SigManager.hpp"

namespace bftEngine::impl {

using namespace std;

ClientBatchRequestMsg::ClientBatchRequestMsg(NodeIdType clientId,
                                             const std::deque<ClientRequestMsg*>& batch,
                                             uint32_t batchBufSize,
                                             const string& cid)
    : MessageBase(
          clientId, MsgCode::ClientBatchRequest, 0, sizeof(ClientBatchRequestMsgHeader) + cid.size() + batchBufSize) {
  const auto& numOfMessagesInBatch = batch.size();
  msgBody()->msgType = MsgCode::ClientBatchRequest;
  msgBody()->cidSize = cid.size();
  msgBody()->clientId = clientId;
  msgBody()->numOfMessagesInBatch = numOfMessagesInBatch;
  msgBody()->dataSize = batchBufSize;
  char* data = body() + sizeof(ClientBatchRequestMsgHeader);
  if (cid.size()) {
    memcpy(data, cid.c_str(), cid.size());
    data += cid.size();
  }
  for (auto const& msg : batch) {
    memcpy(data, msg->body(), msg->size());
    data += msg->size();
  }
  LOG_DEBUG(logger(), KVLOG(cid, clientId, numOfMessagesInBatch, batchBufSize));
}

const string& ClientBatchRequestMsg::getCid() {
  if (cid_.empty()) cid_ = string(body() + sizeof(ClientBatchRequestMsgHeader), msgBody()->cidSize);
  return cid_;
}

void ClientBatchRequestMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(senderId() != repInfo.myId());
  if (size() < sizeof(ClientBatchRequestMsgHeader) ||
      size() < (sizeof(ClientBatchRequestMsgHeader) + msgBody()->dataSize))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

ClientMsgsList& ClientBatchRequestMsg::getClientPreProcessRequestMsgs() {
  if (!clientMsgsList_.empty()) return clientMsgsList_;

  const auto& numOfMessagesInBatch = msgBody()->numOfMessagesInBatch;
  char* dataPosition = body() + sizeof(ClientBatchRequestMsgHeader) + msgBody()->cidSize;
  auto sigManager = SigManager::getInstance();
  bool isClientTransactionSigningEnabled = sigManager->isClientTransactionSigningEnabled();
  for (uint32_t i = 0; i < numOfMessagesInBatch; i++) {
    const auto& singleMsgHeader = *(ClientRequestMsgHeader*)dataPosition;
    const char* spanDataPosition = dataPosition + sizeof(ClientRequestMsgHeader);
    const char* requestDataPosition = spanDataPosition + singleMsgHeader.spanContextSize;
    const char* cidPosition = requestDataPosition + singleMsgHeader.requestLength;
    const concordUtils::SpanContext spanContext(string(spanDataPosition, singleMsgHeader.spanContextSize));
    const char* requestSignaturePosition =
        isClientTransactionSigningEnabled ? (cidPosition + singleMsgHeader.cidLength) : nullptr;
    uint32_t requestSignatureLength =
        isClientTransactionSigningEnabled ? sigManager->getSigLength(singleMsgHeader.idOfClientProxy) : 0;
    auto msg = make_unique<preprocessor::ClientPreProcessRequestMsg>(singleMsgHeader.idOfClientProxy,
                                                                     singleMsgHeader.reqSeqNum,
                                                                     singleMsgHeader.requestLength,
                                                                     requestDataPosition,
                                                                     singleMsgHeader.timeoutMilli,
                                                                     string(cidPosition, singleMsgHeader.cidLength),
                                                                     spanContext,
                                                                     requestSignaturePosition,
                                                                     requestSignatureLength);
    clientMsgsList_.push_back(move(msg));
    dataPosition += sizeof(ClientRequestMsgHeader) + singleMsgHeader.spanContextSize + singleMsgHeader.requestLength +
                    singleMsgHeader.cidLength;
  }
  return clientMsgsList_;
}

}  // namespace bftEngine::impl
