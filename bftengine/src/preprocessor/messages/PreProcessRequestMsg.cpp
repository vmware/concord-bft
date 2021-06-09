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
                                           const concordUtils::SpanContext& span_context)
    : MessageBase(senderId,
                  MsgCode::PreProcessRequest,
                  span_context.data().size(),
                  sizeof(Header) + reqLength + cid.size() + requestSignatureLength) {
  ConcordAssert((requestSignatureLength > 0) == (nullptr != requestSignature));
  setParams(reqType, senderId, clientId, reqOffsetInBatch, reqSeqNum, reqRetryId, reqLength, requestSignatureLength);
  msgBody()->cidLength = cid.size();
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
                  sizeof(Header),
                  reqLength,
                  cid.size(),
                  requestSignatureLength,
                  msgLength));
}

void PreProcessRequestMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(type() == MsgCode::PreProcessRequest);
  ConcordAssert(senderId() != repInfo.myId());
  auto* header = msgBody();
  auto* requestSignature = this->requestSignature();
  auto* sigManager = SigManager::instance();
  auto expectedMsgSize =
      (sizeof(Header) + spanContextSize() + header->requestLength + header->cidLength + header->reqSignatureLength);

  if (size() < (sizeof(Header)) || size() != expectedMsgSize) {
    throw std::runtime_error(__PRETTY_FUNCTION__);
  }

  if (requestSignature) {
    ConcordAssert(sigManager->isClientTransactionSigningEnabled());
    ConcordAssert(header->reqType == REQ_TYPE_PRE_PROCESS);
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
                                     uint64_t reqRetryId,
                                     uint32_t reqLength,
                                     uint16_t reqSignatureLength) {
  auto* header = msgBody();
  header->reqType = reqType;
  header->senderId = senderId;
  header->clientId = clientId;
  header->reqOffsetInBatch = reqOffsetInBatch;
  header->reqSeqNum = reqSeqNum;
  header->reqRetryId = reqRetryId;
  header->requestLength = reqLength;
  header->reqSignatureLength = reqSignatureLength;
}

std::string PreProcessRequestMsg::getCid() const {
  return std::string(body() + sizeof(Header) + spanContextSize() + msgBody()->requestLength, msgBody()->cidLength);
}

}  // namespace preprocessor
