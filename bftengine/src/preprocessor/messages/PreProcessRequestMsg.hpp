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

#pragma once

#include "OpenTracing.hpp"
#include "messages/MessageBase.hpp"
#include "Logger.hpp"
#include <memory>

namespace preprocessor {

typedef enum { REQ_TYPE_PRE_PROCESS, REQ_TYPE_CANCEL } RequestType;

class PreProcessRequestMsg : public MessageBase {
 public:
  PreProcessRequestMsg(RequestType reqType,
                       NodeIdType senderId,
                       uint16_t clientId,
                       uint16_t reqOffsetInBatch,
                       uint64_t reqSeqNum,
                       uint64_t reqRetryId,
                       uint32_t reqLength,
                       const char* request,
                       const std::string& reqCid,
                       const char* requestSignature,
                       uint16_t requestSignatureLength,
                       uint64_t blockid,
                       ViewNum viewNum,
                       const std::string& participant_id,
                       const concordUtils::SpanContext& span_context = concordUtils::SpanContext{},
                       uint32_t result = 1);  // UNKNOWN

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(PreProcessRequestMsg)

  void validate(const bftEngine::impl::ReplicasInfo&) const override;
  char* requestBuf() const { return body() + sizeof(Header) + spanContextSize(); }
  const RequestType reqType() const { return msgBody()->reqType; }
  const uint32_t requestLength() const { return msgBody()->requestLength; }
  const uint16_t clientId() const { return msgBody()->clientId; }
  const uint16_t reqOffsetInBatch() const { return msgBody()->reqOffsetInBatch; }
  const SeqNum reqSeqNum() const { return msgBody()->reqSeqNum; }
  const uint64_t reqRetryId() const { return msgBody()->reqRetryId; }
  const uint64_t primaryBlockId() const { return msgBody()->primaryBlockId; }
  const uint32_t requestSignatureLength() const { return msgBody()->reqSignatureLength; }
  const ViewNum viewNum() const { return msgBody()->viewNum; }
  std::string getCid() const;
  std::string getParticipantid() const;
  inline char* requestSignature() const {
    auto* header = msgBody();
    if (header->reqSignatureLength > 0)
      return body() + sizeof(Header) + spanContextSize() + header->requestLength + header->cidLength +
             header->participantidLength;
    return nullptr;
  }
  const uint32_t result() const { return msgBody()->result; }

 public:
#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    RequestType reqType;
    SeqNum reqSeqNum;
    uint16_t clientId;
    uint16_t reqOffsetInBatch;
    NodeIdType senderId;
    uint32_t requestLength;
    uint32_t cidLength;
    uint32_t spanContextSize;
    uint64_t reqRetryId;
    uint16_t reqSignatureLength;
    uint64_t primaryBlockId;
    uint32_t result;
    ViewNum viewNum;
    uint32_t participantidLength;
  };
#pragma pack(pop)

 protected:
  template <typename MessageT>
  friend size_t bftEngine::impl::sizeOfHeader();

 private:
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }
  void setParams(RequestType reqType,
                 NodeIdType senderId,
                 uint16_t clientId,
                 uint16_t reqOffsetInBatch,
                 ReqId reqSeqNum,
                 uint32_t cidLength,
                 uint32_t spanContextSize,
                 uint64_t reqRetryId,
                 uint32_t reqLength,
                 uint16_t reqSignatureLength,
                 uint64_t blockId,
                 uint32_t result,
                 ViewNum viewNum,
                 uint32_t participantidLength);
  Header* msgBody() const { return ((Header*)msgBody_); }
};

typedef std::shared_ptr<PreProcessRequestMsg> PreProcessRequestMsgSharedPtr;

}  // namespace preprocessor
