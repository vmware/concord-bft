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

#include <cstring>

namespace bftEngine::impl {

// local helper functions

static uint16_t getSender(const ClientRequestMsgHeader* r) { return r->idOfClientProxy; }

static int32_t compRequestMsgSize(const ClientRequestMsgHeader* r) {
  return (sizeof(ClientRequestMsgHeader) + r->spanContextSize + r->requestLength + r->cidLength +
          r->reqSignatureLength);
}

uint32_t getRequestSizeTemp(const char* request)  // TODO(GG): change - TBD
{
  const ClientRequestMsgHeader* r = (ClientRequestMsgHeader*)request;
  return compRequestMsgSize(r);
}

ClientRequestMsg::Recorders ClientRequestMsg::histograms_;

// class ClientRequestMsg
ClientRequestMsg::ClientRequestMsg(NodeIdType sender,
                                   uint8_t flags,
                                   uint64_t reqSeqNum,
                                   uint32_t requestLength,
                                   const char* request,
                                   uint64_t reqTimeoutMilli,
                                   const std::string& cid,
                                   const concordUtils::SpanContext& spanContext,
                                   const char* requestSignature,
                                   uint32_t requestSignatureLen)
    : MessageBase(sender,
                  MsgCode::ClientRequest,
                  spanContext.data().size(),
                  sizeof(ClientRequestMsgHeader) + requestLength + cid.size() + requestSignatureLen) {
  // logical XOR - if requestSignatureLen is zero requestSignature must be null and vise versa
  ConcordAssert((requestSignature == nullptr) == (requestSignatureLen == 0));
  // set header
  setParams(sender, reqSeqNum, requestLength, flags, reqTimeoutMilli, cid, requestSignatureLen);

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

void ClientRequestMsg::validate(const ReplicasInfo& repInfo) const {
  uint16_t expectedSigLen = getExpectedSignatureLength();
  validateRequest(repInfo, expectedSigLen);
  if (expectedSigLen > 0) {
    validateRequestSignature(repInfo);
  }
}

uint16_t ClientRequestMsg::getExpectedSignatureLength() const {
  std::stringstream msg;
  auto sigManager = SigManager::getInstance();
  uint16_t expectedSigLen{0};

  if (sigManager->isClientTransactionSigningEnabled()) {
    expectedSigLen = sigManager->getSigLength(senderId());
    if (expectedSigLen == 0) {
      std::stringstream msg;
      msg << "Failed to get signature length for senderid " << senderId();
      LOG_ERROR(GL, msg.str());
      throw ClientSignatureVerificationFailedException(msg.str());
    }
  }
  return expectedSigLen;
}

void ClientRequestMsg::validateRequest(const ReplicasInfo& repInfo, uint16_t expectedSigLen) const {
  PrincipalId senderId = this->senderId();
  ConcordAssert(senderId != repInfo.myId());
  const auto* header = msgBody();
  auto minMsgSize = sizeof(ClientRequestMsgHeader) + header->cidLength + spanContextSize() + header->reqSignatureLength;
  const auto msgSize = size();
  auto expectedMsgSize =
      sizeof(ClientRequestMsgHeader) + header->requestLength + header->cidLength + spanContextSize() + expectedSigLen;
  std::stringstream msg;

  // TODO - uncomment after fixing Apollo infra, which is not compatible with this requirement
  // See: BC-8149
  //
  // if (!repInfo.isValidParticipantId(senderId)) {
  //   msg << "Invalid senderId " << senderId;
  //   throw std::runtime_error(msg.str());
  // }

  if ((msgSize < minMsgSize) || (msgSize != expectedMsgSize)) {
    msg << "Invalid msgSize: " << KVLOG(msgSize, minMsgSize, expectedMsgSize);
    LOG_ERROR(GL, msg.str());
    throw std::runtime_error(msg.str());
  }
}

void ClientRequestMsg::validateRequestSignature(const ReplicasInfo& repInfo) const {
  // Mesure the time takes for a signature validation
  concord::diagnostics::TimeRecorder<true> scoped_timer(*histograms_.signatureVerificationduration);
  PrincipalId senderId = this->senderId();
  auto sigManager = SigManager::getInstance();
  std::stringstream msg;

  // We validate request's signature only from external clients
  if (repInfo.isIdOfExternalClient(senderId)) {
    msg << "Signature is given but senderId " << senderId << " is not a valid id of an externa lbft client!";
    LOG_ERROR(GL, msg.str());
    throw ClientSignatureVerificationFailedException(msg.str());
  }

  if (!sigManager->verifySig(
          senderId, requestBuf(), msgBody()->requestLength, requestSignature(), requestSignatureLength())) {
    msg << "Signature verification failed for senderid " << senderId << " and requestLength "
        << msgBody()->requestLength;
    LOG_ERROR(GL, msg.str());
    throw ClientSignatureVerificationFailedException(msg.str());
  }
}

void ClientRequestMsg::setParams(NodeIdType sender,
                                 ReqId reqSeqNum,
                                 uint32_t requestLength,
                                 uint8_t flags,
                                 uint64_t reqTimeoutMilli,
                                 const std::string& cid,
                                 uint32_t requestSignatureLen) {
  auto* header = msgBody();
  header->idOfClientProxy = sender;
  header->timeoutMilli = reqTimeoutMilli;
  header->reqSeqNum = reqSeqNum;
  header->requestLength = requestLength;
  header->flags = flags;
  header->cidLength = cid.size();
  header->reqSignatureLength = requestSignatureLen;
}

std::string ClientRequestMsg::getCid() const {
  return std::string(body() + sizeof(ClientRequestMsgHeader) + msgBody()->requestLength + spanContextSize(),
                     msgBody()->cidLength);
}

const char* ClientRequestMsg::requestSignature() const {
  const auto* header = msgBody();
  if (header->reqSignatureLength > 0) {
    return body() + sizeof(ClientRequestMsgHeader) + spanContextSize() + header->requestLength + header->cidLength;
  }
  return nullptr;
}

}  // namespace bftEngine::impl
