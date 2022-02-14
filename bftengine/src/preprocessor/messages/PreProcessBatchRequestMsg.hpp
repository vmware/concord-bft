// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "PreProcessRequestMsg.hpp"
#include <list>

namespace preprocessor {

using PreProcessReqMsgsList = std::list<preprocessor::PreProcessRequestMsgSharedPtr>;

class PreProcessBatchRequestMsg : public MessageBase {
 public:
  PreProcessBatchRequestMsg(RequestType reqType,
                            NodeIdType clientId,
                            NodeIdType senderId,
                            const PreProcessReqMsgsList& batch,
                            const std::string& cid,
                            uint32_t requestsSize,
                            ViewNum viewNum);

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(PreProcessBatchRequestMsg)

  std::string getCid() const;
  uint16_t clientId() const { return msgBody()->clientId; }
  uint32_t numOfMessagesInBatch() const { return msgBody()->numOfMessagesInBatch; }
  const RequestType reqType() const { return msgBody()->reqType; }
  const ViewNum viewNum() const { return msgBody()->viewNum; }
  PreProcessReqMsgsList& getPreProcessRequestMsgs();
  void validate(const ReplicasInfo&) const override;

 protected:
  template <typename MessageT>
  friend size_t bftEngine::impl::sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    RequestType reqType;
    uint16_t clientId;
    NodeIdType senderId;
    uint32_t cidLength;
    uint32_t numOfMessagesInBatch;
    uint32_t requestsSize;
    ViewNum viewNum;
  };
#pragma pack(pop)

 private:
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }
  void setParams(uint16_t clientId,
                 NodeIdType senderId,
                 RequestType reqType,
                 uint32_t numOfMessagesInBatch,
                 uint32_t requestsSize,
                 ViewNum viewNum);
  Header* msgBody() const { return ((Header*)msgBody_); }

 private:
  bool checkElements() const;
  std::string cid_;
  PreProcessReqMsgsList preProcessReqMsgsList_;
};

using PreProcessBatchReqMsgSharedPtr = std::shared_ptr<PreProcessBatchRequestMsg>;

}  // namespace preprocessor
