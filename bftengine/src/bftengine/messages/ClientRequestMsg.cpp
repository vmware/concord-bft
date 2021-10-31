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
          r->reqSignatureLength + r->extraDataLength);
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
                                   const concordUtils::SpanContext& spanContext,
                                   const char* requestSignature,
                                   uint32_t requestSignatureLen,
                                   const uint32_t extraBufSize)
    : MessageBase(sender,
                  MsgCode::ClientRequest,
                  spanContext.data().size(),
                  sizeof(ClientRequestMsgHeader) + requestLength + cid.size() + requestSignatureLen + extraBufSize) {
  // logical XOR - if requestSignatureLen is zero requestSignature must be null and vise versa
  ConcordAssert((requestSignature == nullptr) == (requestSignatureLen == 0));
  // set header
  setParams(sender, reqSeqNum, requestLength, flags, reqTimeoutMilli, cid, requestSignatureLen, extraBufSize);

  // set span context
  char* position = body() + sizeof(ClientRequestMsgHeader);
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

ClientRequestMsg::ClientRequestMsg(NodeIdType sender)
    : MessageBase(sender, MsgCode::ClientRequest, 0, (sizeof(ClientRequestMsgHeader))) {
  msgBody()->flags &= EMPTY_CLIENT_REQ;
}

ClientRequestMsg::ClientRequestMsg(ClientRequestMsgHeader* body)
    : MessageBase(getSender(body), (MessageBase::Header*)body, compRequestMsgSize(body), false) {}

bool ClientRequestMsg::isReadOnly() const { return (msgBody()->flags & READ_ONLY_REQ) != 0; }

void ClientRequestMsg::validateImp(const ReplicasInfo& repInfo) const {
  const auto* header = msgBody();
  PrincipalId clientId = header->idOfClientProxy;
  ConcordAssert(this->senderId() != repInfo.myId());
  /// to do - should it be just the header?
  auto minMsgSize = sizeof(ClientRequestMsgHeader) + header->cidLength + spanContextSize() + header->reqSignatureLength;
  const auto msgSize = size();
  uint16_t expectedSigLen = 0;
  std::stringstream msg;
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
    LOG_WARN(GL, msg.str());
    throw std::runtime_error(msg.str());
  }
  if (!repInfo.isValidPrincipalId(this->senderId())) {
    msg << "Invalid senderId " << this->senderId();
    LOG_WARN(GL, msg.str());
    throw std::runtime_error(msg.str());
  }
  if (isIdOfExternalClient && isClientTransactionSigningEnabled) {
    // Skip signature validation if:
    // 1) request has been pre-processed (validation done already on pre-processor) or
    // 2) request is empty. empty requests are sent from pre-processor in some cases - skip signature
    if (emptyReq) {
      expectedSigLen = 0;
    } else if ((header->flags & RECONFIG_FLAG) != 0) {
      expectedSigLen = header->reqSignatureLength;
      // This message arrived from operator - no need at this stage to verifiy the request, since operator
      // verifies it's own signatures on requests in the reconfiguration handler
      doSigVerify = false;
    } else {
      expectedSigLen = sigManager->getSigLength(clientId);
      if (0 == expectedSigLen) {
        msg << "Invalid expectedSigLen " << KVLOG(clientId, this->senderId());
        LOG_ERROR(GL, msg.str());
        throw std::runtime_error(msg.str());
      }
      if ((header->flags & HAS_PRE_PROCESSED_FLAG) == 0) {
        doSigVerify = true;
      }
    }
  }

  if (expectedSigLen != header->reqSignatureLength) {
    msg << "Unexpected request signature length: "
        << KVLOG(clientId,
                 this->senderId(),
                 header->reqSeqNum,
                 expectedSigLen,
                 header->reqSignatureLength,
                 isIdOfExternalClient,
                 isClientTransactionSigningEnabled);
    LOG_WARN(GL, msg.str());
    throw std::runtime_error(msg.str());
  }
  auto expectedMsgSize = sizeof(ClientRequestMsgHeader) + header->requestLength + header->cidLength +
                         spanContextSize() + expectedSigLen + header->extraDataLength;

  if ((msgSize < minMsgSize) || (msgSize != expectedMsgSize)) {
    msg << "Invalid msgSize: " << KVLOG(msgSize, minMsgSize, expectedMsgSize);
    LOG_WARN(GL, msg.str());
    throw std::runtime_error(msg.str());
  }
  if (doSigVerify) {
    if (!sigManager->verifySig(
            clientId, requestBuf(), header->requestLength, requestSignature(), header->reqSignatureLength)) {
      std::stringstream msg;
      LOG_WARN(GL, "Signature verification failed for " << KVLOG(header->reqSeqNum, this->senderId(), clientId));
      msg << "Signature verification failed for: "
          << KVLOG(clientId,
                   this->senderId(),
                   header->reqSeqNum,
                   header->requestLength,
                   header->reqSignatureLength,
                   getCid(),
                   this->senderId());
      throw std::runtime_error(msg.str());
    }
    LOG_TRACE(GL, "Signature verified for " << KVLOG(header->reqSeqNum, this->senderId(), clientId));
  }
}

void ClientRequestMsg::setParams(NodeIdType sender,
                                 ReqId reqSeqNum,
                                 uint32_t requestLength,
                                 uint64_t flags,
                                 uint64_t reqTimeoutMilli,
                                 const std::string& cid,
                                 uint32_t requestSignatureLen,
                                 uint32_t extraBufSize) {
  auto* header = msgBody();
  header->idOfClientProxy = sender;
  header->timeoutMilli = reqTimeoutMilli;
  header->reqSeqNum = reqSeqNum;
  header->requestLength = requestLength;
  header->flags = flags;
  header->cidLength = cid.size();
  header->reqSignatureLength = requestSignatureLen;
  header->extraDataLength = extraBufSize;
}

std::string ClientRequestMsg::getCid() const {
  return std::string(body() + sizeof(ClientRequestMsgHeader) + msgBody()->requestLength + spanContextSize(),
                     msgBody()->cidLength);
}

char* ClientRequestMsg::requestSignature() const {
  const auto* header = msgBody();
  if (header->reqSignatureLength > 0) {
    return body() + sizeof(ClientRequestMsgHeader) + spanContextSize() + header->requestLength + header->cidLength;
  }
  return nullptr;
}

}  // namespace bftEngine::impl
