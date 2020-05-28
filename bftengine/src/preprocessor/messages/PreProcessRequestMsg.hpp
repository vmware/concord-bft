// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "messages/MessageBase.hpp"
#include <memory>

namespace preprocessor {

class PreProcessRequestMsg : public MessageBase {
 public:
  PreProcessRequestMsg(NodeIdType senderId,
                       uint16_t clientId,
                       uint64_t reqSeqNum,
                       uint32_t reqLength,
                       const char* request,
                       const std::string& span_context,
                       const std::string& cid);

  void validate(const bftEngine::impl::ReplicasInfo&) const override;
  char* requestBuf() const { return body() + sizeof(Header) + spanContextSize(); }
  const uint32_t requestLength() const { return msgBody()->requestLength; }
  const uint16_t clientId() const { return msgBody()->clientId; }
  const SeqNum reqSeqNum() const { return msgBody()->reqSeqNum; }
  std::string getCid() const;

 protected:
  template <typename MessageT>
  friend size_t bftEngine::impl::sizeOfHeader();
#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    SeqNum reqSeqNum;
    uint16_t clientId;
    NodeIdType senderId;
    uint32_t requestLength;
    uint32_t cidLength;
  };
#pragma pack(pop)

 private:
  void setParams(NodeIdType senderId, uint16_t clientId, ReqId reqSeqNum, uint32_t reqLength);

 private:
  Header* msgBody() const { return ((Header*)msgBody_); }
};

typedef std::shared_ptr<PreProcessRequestMsg> PreProcessRequestMsgSharedPtr;

}  // namespace preprocessor
