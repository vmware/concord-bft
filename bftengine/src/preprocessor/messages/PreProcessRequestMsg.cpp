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

#include "PreProcessRequestMsg.hpp"
#include "assertUtils.hpp"
#include "SigManager.hpp"

#include <cstring>

namespace preprocessor {

PreProcessRequestMsg::PreProcessRequestMsg(RequestType reqType,
                                           NodeIdType senderId,
                                           uint16_t clientId,
                                           uint16_t reqOffsetInBatch,
                                           uint64_t reqSeqNum,
                                           uint64_t reqRetryId,
                                           uint32_t reqLength,
                                           const char* request,
                                           const std::string& cid,
                                           const char* requestSignature,
                                           uint16_t requestSignatureLength,
                                           uint64_t blockid,
                                           const concordUtils::SpanContext& span_context)
    : MessageBase(senderId,
                  MsgCode::PreProcessRequest,
                  span_context.data().size(),
                  sizeof(Header) + reqLength + cid.size() + requestSignatureLength) {
  ConcordAssert((requestSignatureLength > 0) == (nullptr != requestSignature));
  setParams(reqType,
            senderId,
            clientId,
            reqOffsetInBatch,
            reqSeqNum,
            cid.size(),
            span_context.data().size(),
            reqRetryId,
            reqLength,
            requestSignatureLength,
            blockid);
  auto position = body() + sizeof(Header);
  memcpy(position, span_context.data().data(), span_context.data().size());
  position += span_context.data().size();
  memcpy(position, request, reqLength);
  position += reqLength;
  memcpy(position, cid.c_str(), cid.size());
  uint64_t msgLength = sizeof(Header) + span_context.data().size() + reqLength + cid.size();
  if (requestSignatureLength) {
    position += cid.size();
    memcpy(position, requestSignature, requestSignatureLength);
    msgLength += requestSignatureLength;
  }
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(),
            KVLOG(reqType,
                  senderId,
                  clientId,
                  reqSeqNum,
                  reqRetryId,
                  reqLength,
                  cid.size(),
                  span_context.data().size(),
                  requestSignatureLength,
                  msgLength,
                  blockid));
}

void PreProcessRequestMsg::validate(const ReplicasInfo& repInfo) const {
  if (size() < (sizeof(Header))) throw std::runtime_error(__PRETTY_FUNCTION__);

  auto* header = msgBody();
  auto* requestSignature = this->requestSignature();
  auto* sigManager = SigManager::instance();
  auto expectedMsgSize = (sizeof(Header) + header->spanContextSize + header->requestLength + header->cidLength +
                          header->reqSignatureLength);
  if (size() != expectedMsgSize) throw std::runtime_error(__PRETTY_FUNCTION__);

  if (type() != MsgCode::PreProcessRequest) {
    LOG_ERROR(logger(), "Message type is incorrect" << KVLOG(type()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }

  if (senderId() == repInfo.myId()) {
    LOG_ERROR(logger(), "Message sender is ivalid" << KVLOG(senderId(), repInfo.myId()));
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }

  if (requestSignature) {
    ConcordAssert(sigManager->isClientTransactionSigningEnabled());
    if (!sigManager->verifySig(
            header->clientId, requestBuf(), header->requestLength, requestSignature, header->reqSignatureLength)) {
      std::stringstream msg;
      LOG_ERROR(logger(),
                "Signature verification failed for " << KVLOG(header->reqSeqNum, header->clientId, this->senderId()));
      msg << "Signature verification failed for: "
          << KVLOG(header->clientId, header->reqSeqNum, header->requestLength, header->reqSignatureLength);
      throw std::runtime_error(msg.str());
    }
    LOG_TRACE(GL, "Signature verified for " << KVLOG(header->reqSeqNum, this->senderId(), header->clientId));
  }
}

void PreProcessRequestMsg::setParams(RequestType reqType,
                                     NodeIdType senderId,
                                     uint16_t clientId,
                                     uint16_t reqOffsetInBatch,
                                     ReqId reqSeqNum,
                                     uint32_t cidLength,
                                     uint32_t spanContextSize,
                                     uint64_t reqRetryId,
                                     uint32_t reqLength,
                                     uint16_t reqSignatureLength,
                                     uint64_t blockId) {
  auto* header = msgBody();
  header->reqType = reqType;
  header->senderId = senderId;
  header->clientId = clientId;
  header->reqOffsetInBatch = reqOffsetInBatch;
  header->reqSeqNum = reqSeqNum;
  header->cidLength = cidLength;
  header->spanContextSize = spanContextSize;
  header->reqRetryId = reqRetryId;
  header->requestLength = reqLength;
  header->reqSignatureLength = reqSignatureLength;
  header->primaryBlockId = blockId;
}

std::string PreProcessRequestMsg::getCid() const {
  return std::string(body() + sizeof(Header) + msgBody()->spanContextSize + msgBody()->requestLength,
                     msgBody()->cidLength);
}

}  // namespace preprocessor
