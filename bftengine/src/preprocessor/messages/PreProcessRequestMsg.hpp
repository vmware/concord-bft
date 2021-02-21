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

class PreProcessRequestMsg : public MessageBase {
 public:
  PreProcessRequestMsg(NodeIdType senderId,
                       uint16_t clientId,
                       uint16_t reqOffsetInBatch,
                       uint64_t reqSeqNum,
                       uint64_t reqRetryId,
                       uint32_t reqLength,
                       const char* request,
                       const std::string& cid,
                       const concordUtils::SpanContext& span_context = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(PreProcessRequestMsg)

  void validate(const bftEngine::impl::ReplicasInfo&) const override;
  char* requestBuf() const { return body() + sizeof(Header) + spanContextSize(); }
  const uint32_t requestLength() const { return msgBody()->requestLength; }
  const uint16_t clientId() const { return msgBody()->clientId; }
  const uint16_t reqOffsetInBatch() const { return msgBody()->reqOffsetInBatch; }
  const SeqNum reqSeqNum() const { return msgBody()->reqSeqNum; }
  const uint64_t reqRetryId() const { return msgBody()->reqRetryId; }
  std::string getCid() const;

 protected:
  template <typename MessageT>
  friend size_t bftEngine::impl::sizeOfHeader();
#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    SeqNum reqSeqNum;
    uint16_t clientId;
    uint16_t reqOffsetInBatch;
    NodeIdType senderId;
    uint32_t requestLength;
    uint32_t cidLength;
    uint64_t reqRetryId;
  };
#pragma pack(pop)

 private:
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }
  void setParams(NodeIdType senderId,
                 uint16_t clientId,
                 uint16_t reqOffsetInBatch,
                 ReqId reqSeqNum,
                 uint64_t reqRetryId,
                 uint32_t reqLength);

 private:
  Header* msgBody() const { return ((Header*)msgBody_); }
};

typedef std::shared_ptr<PreProcessRequestMsg> PreProcessRequestMsgSharedPtr;

}  // namespace preprocessor
