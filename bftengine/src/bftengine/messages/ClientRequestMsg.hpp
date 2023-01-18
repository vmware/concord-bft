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

#pragma once

#include "MessageBase.hpp"
#include "ReplicasInfo.hpp"
#include "ClientMsgs.hpp"
#include "SimpleClient.hpp"
#include "diagnostics.h"
#include "performance_handler.h"

namespace bftEngine::impl {

class ClientRequestMsg : public MessageBase {
  static_assert((uint16_t)REQUEST_MSG_TYPE == (uint16_t)MsgCode::ClientRequest, "");
  static_assert(sizeof(ClientRequestMsgHeader::msgType) == sizeof(MessageBase::Header::msgType), "");
  static_assert(sizeof(ClientRequestMsgHeader::idOfClientProxy) == sizeof(NodeIdType), "");
  static_assert(sizeof(ClientRequestMsgHeader::reqSeqNum) == sizeof(ReqId), "");
  static_assert(sizeof(ClientRequestMsgHeader) == 52, "ClientRequestMsgHeader size is 52B");
  static concord::diagnostics::Recorder sigNatureVerificationRecorder;

 public:
  ClientRequestMsg(NodeIdType sender,
                   uint64_t flags,
                   uint64_t reqSeqNum,
                   uint32_t requestLength,
                   const char* request,
                   uint64_t reqTimeoutMilli,
                   const std::string& cid = "",
                   uint32_t result = 1,  // UNKNOWN
                   const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{},
                   const char* requestSignature = nullptr,
                   uint32_t requestSignatureLen = 0,
                   uint32_t extraBufSize = 0,
                   uint16_t indexInBatch = 0);

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(ClientRequestMsg)

  uint16_t clientProxyId() const { return msgBody()->idOfClientProxy; }
  bool isReadOnly() const { return (msgBody()->flags & READ_ONLY_REQ) != 0; }
  uint64_t flags() const { return msgBody()->flags; }
  uint32_t result() const { return msgBody()->result; }
  ReqId requestSeqNum() const { return msgBody()->reqSeqNum; }
  uint32_t requestLength() const { return msgBody()->requestLength; }
  uint32_t requestSignatureLength() const { return msgBody()->reqSignatureLength; }
  uint64_t requestTimeoutMilli() const { return msgBody()->timeoutMilli; }
  uint16_t requestIndexInBatch() const { return msgBody()->indexInBatch; }

  char* requestBuf() const { return body().data() + sizeof(ClientRequestMsgHeader) + spanContextSize(); }
  char* requestSignature() const;
  std::string getCid() const;

  void validate(const ReplicasInfo& repInfo) const override { validateImp(repInfo); }
  bool shouldValidateAsync() const override;

  static uint32_t compRequestMsgSize(const ClientRequestMsgHeader* r);
  static void validateMsg(const ReplicasInfo& repInfo,
                          const ClientRequestMsgHeader* header,
                          uint32_t msgSize,
                          uint32_t senderId,
                          const char* requestBuf,
                          const char* requestSignature,
                          const std::string& cid,
                          uint32_t spanContextSize);
  static std::pair<char*, uint32_t> getExtraBufData(char* msg, uint32_t msgSize, uint32_t extraDataLength) {
    return std::make_pair(msg + msgSize - extraDataLength, extraDataLength);
  }

 protected:
  ClientRequestMsgHeader* msgBody() const { return ((ClientRequestMsgHeader*)msgBody_->data()); }

  void validateImp(const ReplicasInfo& repInfo) const;

  // Returns a pair of pointer and size to the extra buffer which was allocated during initialisation
  std::pair<char*, uint32_t> getExtraBufPtr() {
    return getExtraBufData(body().data(), internalStorageSize(), msgBody()->extraDataLength);
  }

 private:
  void setParams(NodeIdType sender,
                 ReqId reqSeqNum,
                 uint32_t requestLength,
                 uint64_t flags,
                 uint64_t reqTimeoutMilli,
                 uint32_t result,
                 const std::string& cid,
                 uint32_t requestSignatureLen,
                 uint32_t extraBufSize,
                 uint16_t offsetInBatch,
                 SpanContextSize spanContextSize);
};

// Helper class to retrieve info from the ClientRequestMsgs stored internally in PrePrepareMsg.
// The class does not own the memory it looks at.
class ClientRequestMsgView {
 public:
  ClientRequestMsgView(ClientRequestMsgHeader* msg) : msg_(msg), msgSize_(ClientRequestMsg::compRequestMsgSize(msg)) {}

  void validate(const ReplicasInfo& repInfo) const {
    ClientRequestMsg::validateMsg(repInfo,
                                  msg_,
                                  msgSize_,
                                  msg_->idOfClientProxy,
                                  requestBuf(),
                                  requestSignature(),
                                  getCid(),
                                  msg_->spanContextSize);
  }

  std::pair<char*, uint32_t> getExtraBufPtr() {
    return ClientRequestMsg::getExtraBufData(reinterpret_cast<char*>(msg_), msgSize_, msg_->extraDataLength);
  }

  uint16_t clientProxyId() const { return msg_->idOfClientProxy; }
  uint64_t flags() const { return msg_->flags; }
  uint32_t result() const { return msg_->result; }
  ReqId requestSeqNum() const { return msg_->reqSeqNum; }
  uint32_t requestLength() const { return msg_->requestLength; }
  uint32_t requestSignatureLength() const { return msg_->reqSignatureLength; }
  uint16_t requestIndexInBatch() const { return msg_->indexInBatch; }
  uint32_t getMsgSize() const { return msgSize_; }
  const char* getRawData() const { return reinterpret_cast<char*>(msg_); }

  char* requestSignature() const {
    if (msg_->reqSignatureLength > 0) {
      return reinterpret_cast<char*>(msg_) + sizeof(ClientRequestMsgHeader) + msg_->spanContextSize +
             msg_->requestLength + msg_->cidLength;
    }
    return nullptr;
  }
  std::string getCid() const {
    return std::string(reinterpret_cast<const char*>(msg_) + sizeof(ClientRequestMsgHeader) + msg_->requestLength +
                           msg_->spanContextSize,
                       msg_->cidLength);
  }
  char* requestBuf() const {
    return reinterpret_cast<char*>(msg_) + sizeof(ClientRequestMsgHeader) + msg_->spanContextSize;
  }

 protected:
  ClientRequestMsgHeader* msg_;
  uint32_t msgSize_;
};

template <>
inline size_t sizeOfHeader<ClientRequestMsg>() {
  return sizeof(ClientRequestMsgHeader);
}

}  // namespace bftEngine::impl
