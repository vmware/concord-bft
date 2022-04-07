// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "bftengine/SimpleClient.hpp"
#include "ClientRequestMsg.hpp"
#include "assertUtils.hpp"
#include "ReplicaConfig.hpp"
#include "SigManager.hpp"
#include "Replica.hpp"
#include <cstring>

namespace bftEngine::impl {

// local helper functions

static uint16_t getSender(const ClientRequestMsgHeader* r) { return r->idOfClientProxy; }

static int32_t compRequestMsgSize(const ClientRequestMsgHeader* r) {
  return (sizeof(ClientRequestMsgHeader) + r->spanContextSize + r->requestLength + r->cidLength +
          r->participantIdLength + r->reqSignatureLength + r->extraDataLength);
}

uint32_t getRequestSizeTemp(const char* request)  // TODO(GG): change - TBD
{
  const ClientRequestMsgHeader* r = (ClientRequestMsgHeader*)request;
  return compRequestMsgSize(r);
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
                                   const std::string& participantId,
                                   const char* requestSignature,
                                   uint32_t requestSignatureLen,
                                   const uint32_t extraBufSize)
    : MessageBase(sender,
                  MsgCode::ClientRequest,
                  spanContext.data().size(),
                  sizeof(ClientRequestMsgHeader) + requestLength + cid.size() + participantId.size() +
                      requestSignatureLen + extraBufSize) {
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
            participantId,
            requestSignatureLen,
            extraBufSize);

  // set span context
  char* position = body() + sizeof(ClientRequestMsgHeader);

  memcpy(position, spanContext.data().data(), spanContext.data().size());

  // set request data
  position += spanContext.data().size();
  memcpy(position, request, requestLength);

  // set correlation ID
  position += requestLength;
  memcpy(position, cid.data(), cid.size());

  position += cid.size();

  // set participant ID
  int participantIdLen = participantId.size();
  if (participantIdLen > 0) {
    memcpy(position, participantId.data(), participantIdLen);
  }

  // set signature
  if (requestSignature) {
    if (participantIdLen > 0) {
      position += participantIdLen;
    }
    memcpy(position, requestSignature, requestSignatureLen);
  }
}

ClientRequestMsg::ClientRequestMsg(NodeIdType sender)
    : MessageBase(sender, MsgCode::ClientRequest, 0, (sizeof(ClientRequestMsgHeader))) {
  msgBody()->flags &= EMPTY_CLIENT_REQ;
}

ClientRequestMsg::ClientRequestMsg(ClientRequestMsgHeader* body)
    : MessageBase(getSender(body), (MessageBase::Header*)body, compRequestMsgSize(body), false) {}

bool ClientRequestMsg::isReadOnly() const { return (msgBody()->flags & READ_ONLY_REQ) != 0; }

bool ClientRequestMsg::shouldValidateAsync() const {
  // Reconfiguration messages are validated in their own handler, so we should not do its validation in an asynchronous
  // manner, as that will lead to overhead. Similarly, key exchanges should happen rarely, and thus we should validate
  // as quick as possible, in sync.
  const auto* header = msgBody();
  if (((header->flags & RECONFIG_FLAG) != 0) || ((header->flags & KEY_EXCHANGE_FLAG) != 0)) {
    return false;
  }
  return true;
}

void ClientRequestMsg::validateImp(const ReplicasInfo& repInfo) const {
  const auto* header = msgBody();
  const auto msgSize = size();

  std::stringstream msg;
  // Check message size is greater than minimum header size
  if (msgSize < sizeof(ClientRequestMsgHeader) + spanContextSize()) {
    msg << "Invalid Message Size " << KVLOG(msgSize, sizeof(ClientRequestMsgHeader), spanContextSize());
    LOG_ERROR(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }

  PrincipalId clientId = header->idOfClientProxy;
  if ((header->flags & RECONFIG_FLAG) == 0) ConcordAssert(this->senderId() != repInfo.myId());

  /// to do - should it be just the header?
  auto minMsgSize = sizeof(ClientRequestMsgHeader) + header->cidLength + spanContextSize() + header->reqSignatureLength;
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
  if ((header->flags & RECONFIG_FLAG) != 0 &&
      (repInfo.isIdOfReplica(clientId) || repInfo.isIdOfPeerRoReplica(clientId))) {
    // Allow every reconfiguration message from replicas (it will be verified in the reconfiguration handler)
    return;
  }
  if (!repInfo.isValidPrincipalId(clientId)) {
    msg << "Invalid clientId " << clientId;
    LOG_WARN(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }
  if (!repInfo.isValidPrincipalId(this->senderId())) {
    msg << "Invalid senderId " << this->senderId();
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
          msg << "Invalid expectedSigLen" << KVLOG(clientId, this->senderId());
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
                 this->senderId(),
                 header->reqSeqNum,
                 expectedSigLen,
                 header->reqSignatureLength,
                 isIdOfExternalClient,
                 isClientTransactionSigningEnabled);
    LOG_ERROR(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }
  auto expectedMsgSize = sizeof(ClientRequestMsgHeader) + header->requestLength + header->cidLength +
                         spanContextSize() + header->participantIdLength + expectedSigLen + header->extraDataLength;

  if ((msgSize < minMsgSize) || (msgSize != expectedMsgSize)) {
    msg << "Invalid msgSize:"
        << KVLOG(msgSize,
                 minMsgSize,
                 expectedMsgSize,
                 sizeof(ClientRequestMsgHeader),
                 header->requestLength,
                 header->cidLength,
                 expectedSigLen,
                 spanContextSize(),
                 header->participantIdLength,
                 header->extraDataLength);
    LOG_ERROR(CNSUS, msg.str());
    throw std::runtime_error(msg.str());
  }

  if (doSigVerify) {
    if (!sigManager->verifySig(
            clientId, requestBuf(), header->requestLength, requestSignature(), header->reqSignatureLength)) {
      std::stringstream msg;
      LOG_WARN(CNSUS, "Signature verification failed for" << KVLOG(header->reqSeqNum, this->senderId(), clientId));
      msg << "Signature verification failed for: "
          << KVLOG(clientId,
                   this->senderId(),
                   header->reqSeqNum,
                   header->requestLength,
                   header->reqSignatureLength,
                   getCid(),
                   getParticipantId(),
                   this->senderId());
      throw std::runtime_error(msg.str());
    }
    LOG_TRACE(CNSUS, "Signature verified for" << KVLOG(header->reqSeqNum, this->senderId(), clientId));
  }
}

void ClientRequestMsg::setParams(NodeIdType sender,
                                 ReqId reqSeqNum,
                                 uint32_t requestLength,
                                 uint64_t flags,
                                 uint64_t reqTimeoutMilli,
                                 uint32_t result,
                                 const std::string& cid,
                                 const std::string& participant_id,
                                 uint32_t requestSignatureLen,
                                 uint32_t extraBufSize) {
  auto* header = msgBody();
  header->idOfClientProxy = sender;
  header->timeoutMilli = reqTimeoutMilli;
  header->reqSeqNum = reqSeqNum;
  header->requestLength = requestLength;
  header->flags = flags;
  header->result = result;
  header->cidLength = cid.size();
  header->participantIdLength = participant_id.size();
  header->reqSignatureLength = requestSignatureLen;
  header->extraDataLength = extraBufSize;
}

std::string ClientRequestMsg::getCid() const {
  return std::string(body() + sizeof(ClientRequestMsgHeader) + msgBody()->requestLength + spanContextSize(),
                     msgBody()->cidLength);
}

std::string ClientRequestMsg::getParticipantId() const {
  if (msgBody()->participantIdLength > 0) {
    return std::string(
        body() + sizeof(ClientRequestMsgHeader) + msgBody()->requestLength + spanContextSize() + msgBody()->cidLength,
        msgBody()->participantIdLength);
  } else
    return "";
}

char* ClientRequestMsg::requestSignature() const {
  const auto* header = msgBody();
  if (header->reqSignatureLength > 0) {
    return body() + sizeof(ClientRequestMsgHeader) + spanContextSize() + header->requestLength + header->cidLength +
           header->participantIdLength;
  }
  return nullptr;
}

}  // namespace bftEngine::impl
