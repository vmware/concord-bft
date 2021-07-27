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
  if (size() < sizeof(ClientBatchRequestMsgHeader) ||
      size() < (sizeof(ClientBatchRequestMsgHeader) + msgBody()->dataSize))
    throw std::runtime_error(__PRETTY_FUNCTION__);

  if (type() != MsgCode::ClientBatchRequest) {
    LOG_ERROR(logger(), "Message type is incorrect" << KVLOG(type()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }

  if (senderId() == repInfo.myId()) {
    LOG_ERROR(logger(), "Message sender is invalid" << KVLOG(senderId()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }
}

ClientMsgsList& ClientBatchRequestMsg::getClientPreProcessRequestMsgs() {
  if (!clientMsgsList_.empty()) return clientMsgsList_;

  const auto& numOfMessagesInBatch = msgBody()->numOfMessagesInBatch;
  const string& batchCid = getCid();
  char* dataPosition = body() + sizeof(ClientBatchRequestMsgHeader) + msgBody()->cidSize;
  auto sigManager = SigManager::instance();
  bool isClientTransactionSigningEnabled = sigManager->isClientTransactionSigningEnabled();
  for (uint32_t i = 0; i < numOfMessagesInBatch; i++) {
    const auto& singleMsgHeader = *(ClientRequestMsgHeader*)dataPosition;
    const char* spanDataPosition = dataPosition + sizeof(ClientRequestMsgHeader);
    const char* requestDataPosition = spanDataPosition + singleMsgHeader.spanContextSize;
    const char* cidPosition = requestDataPosition + singleMsgHeader.requestLength;
    const char* requestSignaturePosition =
        (isClientTransactionSigningEnabled && (singleMsgHeader.reqSignatureLength > 0))
            ? (cidPosition + singleMsgHeader.cidLength)
            : nullptr;
    const concordUtils::SpanContext spanContext(string(spanDataPosition, singleMsgHeader.spanContextSize));
    auto const cid = string(cidPosition, singleMsgHeader.cidLength);
    auto msg = make_unique<preprocessor::ClientPreProcessRequestMsg>(singleMsgHeader.idOfClientProxy,
                                                                     singleMsgHeader.reqSeqNum,
                                                                     singleMsgHeader.requestLength,
                                                                     requestDataPosition,
                                                                     singleMsgHeader.timeoutMilli,
                                                                     cid,
                                                                     spanContext,
                                                                     requestSignaturePosition,
                                                                     singleMsgHeader.reqSignatureLength);
    clientMsgsList_.push_back(move(msg));
    dataPosition += sizeof(ClientRequestMsgHeader) + singleMsgHeader.spanContextSize + singleMsgHeader.requestLength +
                    singleMsgHeader.cidLength + singleMsgHeader.reqSignatureLength;
  }
  LOG_DEBUG(logger(), KVLOG(batchCid, msgBody()->clientId, clientMsgsList_.size(), numOfMessagesInBatch));
  return clientMsgsList_;
}

}  // namespace bftEngine::impl
