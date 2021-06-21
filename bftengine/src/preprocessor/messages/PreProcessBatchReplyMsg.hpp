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

#include "PreProcessReplyMsg.hpp"
#include "../PreProcessorRecorder.hpp"
#include <list>

namespace preprocessor {

typedef std::list<preprocessor::PreProcessReplyMsgSharedPtr> PreProcessReplyMsgsList;

class PreProcessBatchReplyMsg : public MessageBase {
 public:
  PreProcessBatchReplyMsg(uint16_t clientId,
                          NodeIdType senderId,
                          const PreProcessReplyMsgsList& batch,
                          const std::string& cid,
                          uint32_t replyMsgsSize);

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(PreProcessBatchReplyMsg)

  std::string getCid() const;
  uint16_t clientId() const { return msgBody()->clientId; }
  uint32_t numOfMessagesInBatch() const { return msgBody()->numOfMessagesInBatch; }
  PreProcessReplyMsgsList& getPreProcessReplyMsgs();
  void validate(const ReplicasInfo&) const override;

 protected:
  template <typename MessageT>
  friend size_t bftEngine::impl::sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    uint16_t clientId;
    NodeIdType senderId;
    uint32_t cidLength;
    uint32_t numOfMessagesInBatch;
    uint32_t repliesSize;
  };
#pragma pack(pop)

 private:
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }
  void setParams(NodeIdType senderId, uint16_t clientId, uint32_t numOfMessagesInBatch);
  Header* msgBody() const { return ((Header*)msgBody_); }

 private:
  std::string cid_;
  PreProcessReplyMsgsList preProcessReplyMsgsList_;
};

typedef std::shared_ptr<PreProcessReplyMsg> PreProcessReplyMsgSharedPtr;

}  // namespace preprocessor
