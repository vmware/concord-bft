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

#pragma once

#include "MessageBase.hpp"
#include "ReplicasInfo.hpp"
#include "ClientMsgs.hpp"
#include "diagnostics.h"
#include "performance_handler.h"

namespace bftEngine::impl {

class ClientRequestMsg : public MessageBase {
  // TODO(GG): requests should always be verified by the application layer !!!

  static_assert((uint16_t)REQUEST_MSG_TYPE == (uint16_t)MsgCode::ClientRequest, "");
  static_assert(sizeof(ClientRequestMsgHeader::msgType) == sizeof(MessageBase::Header::msgType), "");
  static_assert(sizeof(ClientRequestMsgHeader::idOfClientProxy) == sizeof(NodeIdType), "");
  static_assert(sizeof(ClientRequestMsgHeader::reqSeqNum) == sizeof(ReqId), "");
  static_assert(sizeof(ClientRequestMsgHeader) == 35, "ClientRequestMsgHeader size is 35B");
  static concord::diagnostics::Recorder sigNatureVerificationRecorder;
  // TODO(GG): more asserts

 public:
  ClientRequestMsg(NodeIdType sender,
                   uint8_t flags,
                   uint64_t reqSeqNum,
                   uint32_t requestLength,
                   const char* request,
                   uint64_t reqTimeoutMilli,
                   const std::string& cid = "",
                   const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{},
                   const char* requestSignature = nullptr,
                   uint32_t requestSignatureLen = 0);

  ClientRequestMsg(NodeIdType sender);

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(ClientRequestMsg)

  ClientRequestMsg(ClientRequestMsgHeader* body);

  uint16_t clientProxyId() const { return msgBody()->idOfClientProxy; }

  bool isReadOnly() const;

  uint8_t flags() const { return msgBody()->flags; }

  ReqId requestSeqNum() const { return msgBody()->reqSeqNum; }

  uint32_t requestLength() const { return msgBody()->requestLength; }

  char* requestBuf() const { return body() + sizeof(ClientRequestMsgHeader) + spanContextSize(); }

  uint32_t requestSignatureLength() const { return msgBody()->reqSignatureLength; }

  const char* requestSignature() const;

  uint64_t requestTimeoutMilli() const { return msgBody()->timeoutMilli; }

  std::string getCid() const;

  void validate(const ReplicasInfo&) const override;
  void validateRequest(const ReplicasInfo&, uint16_t) const;
  void validateRequestSignature(const ReplicasInfo&) const;
  uint16_t getExpectedSignatureLength() const;

 protected:
  ClientRequestMsgHeader* msgBody() const { return ((ClientRequestMsgHeader*)msgBody_); }

  struct Recorders {
    Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.registerComponent("client_request", {signatureVerificationduration});
    }
    DEFINE_SHARED_RECORDER(signatureVerificationduration, 1, 10000, 3, concord::diagnostics::Unit::MICROSECONDS);
  };

  static Recorders histograms_;

 private:
  void setParams(NodeIdType sender,
                 ReqId reqSeqNum,
                 uint32_t requestLength,
                 uint8_t flags,
                 uint64_t reqTimeoutMilli,
                 const std::string& cid,
                 uint32_t requestSignatureLen);
};

template <>
inline size_t sizeOfHeader<ClientRequestMsg>() {
  return sizeof(ClientRequestMsgHeader);
}

}  // namespace bftEngine::impl
