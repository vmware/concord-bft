// Concord
//
// Copyright (c) 2018-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ClientRequestMsg.hpp"
#include "assertUtils.hpp"
#include "SigManager.hpp"
#include "Replica.hpp"
#include <cstring>

namespace bftEngine::impl {

uint32_t ClientRequestMsg::compRequestMsgSize(const ClientRequestMsgHeader* r) {
  return (sizeof(ClientRequestMsgHeader) + r->spanContextSize + r->requestLength + r->cidLength +
          r->reqSignatureLength + r->extraDataLength);
}

// class ClientRequestMsg
ClientRequestMsg::ClientRequestMsg(NodeIdType sender,
                                   uint64_t flags,
                                   uint64_t reqSeqNum,
                                   uint32_t requestLength,
                                   const char* request,
                                   uint64_t reqTimeoutMilli,
                                   const std::string& cid,
                                   uint32_t result,
                                   const concordUtils::SpanContext& spanContext,
                                   const char* requestSignature,
                                   uint32_t requestSignatureLen,
                                   uint32_t extraBufSize,
                                   uint16_t indexInBatch)
    : MessageBase(sender,
                  MsgCode::ClientRequest,
                  spanContext.data().size(),
                  sizeof(ClientRequestMsgHeader) + requestLength + cid.size() + requestSignatureLen + extraBufSize) {
  // logical XOR - if requestSignatureLen is zero requestSignature must be null and vise versa
  ConcordAssert((requestSignature == nullptr) == (requestSignatureLen == 0));
  // set header
  setParams(sender,
            reqSeqNum,
            requestLength,
            flags,
            reqTimeoutMilli,
            result,
            cid,
            requestSignatureLen,
            extraBufSize,
            indexInBatch,
            spanContext.data().size());

  // set span context
  char* position = body().data() + sizeof(ClientRequestMsgHeader);
  memcpy(position, spanContext.data().data(), spanContext.data().size());

  // set request data
  position += spanContext.data().size();
  memcpy(position, request, requestLength);

  // set correlation ID
  position += requestLength;
  memcpy(position, cid.data(), cid.size());

  // set signature
  if (requestSignature) {
    position += cid.size();
    memcpy(position, requestSignature, requestSignatureLen);
  }
}

bool ClientRequestMsg::shouldValidateAsync() const {
  // Reconfiguration messages are validated in their own handler, so we should not do its validation in an asynchronous
  // manner, as that will lead to overhead. Similarly, key exchanges should happen rarely, and thus we should validate
  // as quick as possible, in sync.
  const auto* header = msgBody();
  if (((header->flags & RECONFIG_FLAG) != 0) || ((header->flags & KEY_EXCHANGE_FLAG) != 0) ||
      (header->flags & INTERNAL_FLAG) != 0) {
    return false;
  }
  return true;
}

void ClientRequestMsg::validateMsg(const ReplicasInfo& repInfo,
                                   const ClientRequestMsgHeader* header,
                                   uint32_t msgSize,
                                   uint32_t senderId,
                                   const char* requestBuf,
                                   const char* requestSignature,
                                   const std::string& cid,
                                   uint32_t spanContextSize) {
  std::stringstream msg;
  // Check message size is greater than minimum header size
  if (msgSize < sizeof(ClientRequestMsgHeader) + spanContextSize) {
    msg << "Invalid Message Size " << KVLOG(msgSize, sizeof(ClientRequestMsgHeader), spanContextSize);
    LOG_ERROR(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }

  PrincipalId clientId = header->idOfClientProxy;
  if ((header->flags & RECONFIG_FLAG) == 0 && (header->flags & INTERNAL_FLAG) == 0)
    ConcordAssert(senderId != repInfo.myId());

  /// to do - should it be just the header?
  auto minMsgSize = sizeof(ClientRequestMsgHeader) + header->cidLength + spanContextSize + header->reqSignatureLength;
  if (msgSize < minMsgSize) {
    msg << "Invalid msgSize: " << KVLOG(msgSize, minMsgSize);
    LOG_WARN(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }

  uint16_t expectedSigLen = 0;
  auto sigManager = SigManager::instance();
  bool isClientTransactionSigningEnabled = sigManager->isClientTransactionSigningEnabled();
  bool isIdOfExternalClient = repInfo.isIdOfExternalClient(clientId);
  bool doSigVerify = false;
  bool emptyReq = (header->requestLength == 0);
  if (((header->flags & RECONFIG_FLAG) != 0 || (header->flags & INTERNAL_FLAG) != 0) &&
      (repInfo.isIdOfReplica(clientId) || repInfo.isIdOfPeerRoReplica(clientId))) {
    // Allow every reconfiguration/internal message from replicas (it will be verified in the reconfiguration handler)
    return;
  }
  if (!repInfo.isValidPrincipalId(clientId)) {
    msg << "Invalid clientId " << clientId;
    LOG_WARN(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }
  if (!repInfo.isValidPrincipalId(senderId)) {
    msg << "Invalid senderId " << senderId;
    LOG_WARN(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }
  if (isIdOfExternalClient) {
    if ((header->flags & RECONFIG_FLAG) != 0) {
      // This message arrived from operator/cre - no need at this stage to verify the request, since operator/cre
      // verifies its own signatures on requests in the reconfiguration handler.
      expectedSigLen = header->reqSignatureLength;
      doSigVerify = false;
    } else if (isClientTransactionSigningEnabled) {
      // Skip signature validation if:
      // 1) request has been pre-processed (validation done already on pre-processor) or
      // 2) request is empty. empty requests are sent from pre-processor in some cases - skip signature
      if (emptyReq) {
        expectedSigLen = 0;
      } else {
        expectedSigLen = sigManager->getSigLength(clientId);
        if (0 == expectedSigLen) {
          msg << "Invalid expectedSigLen" << KVLOG(clientId, senderId);
          LOG_ERROR(GL, msg.str());
          throw std::runtime_error(msg.str());
        }
        if ((header->flags & HAS_PRE_PROCESSED_FLAG) == 0) {
          doSigVerify = true;
        }
      }
    }
  }
  if (expectedSigLen != header->reqSignatureLength) {
    msg << "Unexpected request signature length:"
        << KVLOG(clientId,
                 senderId,
                 header->reqSeqNum,
                 expectedSigLen,
                 header->reqSignatureLength,
                 isIdOfExternalClient,
                 isClientTransactionSigningEnabled);
    LOG_ERROR(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }
  auto expectedMsgSize = sizeof(ClientRequestMsgHeader) + header->requestLength + header->cidLength + spanContextSize +
                         expectedSigLen + header->extraDataLength;

  if ((msgSize < minMsgSize) || (msgSize != expectedMsgSize)) {
    msg << "Invalid msgSize:"
        << KVLOG(msgSize,
                 minMsgSize,
                 expectedMsgSize,
                 sizeof(ClientRequestMsgHeader),
                 header->requestLength,
                 header->cidLength,
                 expectedSigLen,
                 spanContextSize,
                 header->extraDataLength);
    LOG_ERROR(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }
  if (doSigVerify) {
    if (!sigManager->verifySig(clientId,
                               std::string_view{requestBuf, header->requestLength},
                               std::string_view{requestSignature, header->reqSignatureLength})) {
      LOG_WARN(CNSUS, "Signature verification failed for" << KVLOG(header->reqSeqNum, senderId, clientId));
      msg << "Signature verification failed for: "
          << KVLOG(clientId,
                   senderId,
                   header->reqSeqNum,
                   header->requestLength,
                   header->reqSignatureLength,
                   cid,
                   senderId);
      throw std::runtime_error(msg.str());
    }
    LOG_TRACE(CNSUS, "Signature verified for" << KVLOG(header->reqSeqNum, senderId, clientId));
  }
}

void ClientRequestMsg::validateImp(const ReplicasInfo& repInfo) const {
  validateMsg(repInfo, msgBody(), size(), senderId(), requestBuf(), requestSignature(), getCid(), spanContextSize());
}

void ClientRequestMsg::setParams(NodeIdType sender,
                                 ReqId reqSeqNum,
                                 uint32_t requestLength,
                                 uint64_t flags,
                                 uint64_t reqTimeoutMilli,
                                 uint32_t result,
                                 const std::string& cid,
                                 uint32_t requestSignatureLen,
                                 uint32_t extraBufSize,
                                 uint16_t indexInBatch,
                                 SpanContextSize spanContextSize) {
  auto* header = msgBody();
  header->idOfClientProxy = sender;
  header->timeoutMilli = reqTimeoutMilli;
  header->reqSeqNum = reqSeqNum;
  header->requestLength = requestLength;
  header->flags = flags;
  header->result = result;
  header->cidLength = cid.size();
  header->reqSignatureLength = requestSignatureLen;
  header->extraDataLength = extraBufSize;
  header->indexInBatch = indexInBatch;
  header->spanContextSize = spanContextSize;
}

std::string ClientRequestMsg::getCid() const {
  return std::string(body().data() + sizeof(ClientRequestMsgHeader) + msgBody()->requestLength + spanContextSize(),
                     msgBody()->cidLength);
}

char* ClientRequestMsg::requestSignature() const {
  const auto* header = msgBody();
  if (header->reqSignatureLength > 0) {
    return body().data() + sizeof(ClientRequestMsgHeader) + spanContextSize() + header->requestLength +
           header->cidLength;
  }
  return nullptr;
}

}  // namespace bftEngine::impl
