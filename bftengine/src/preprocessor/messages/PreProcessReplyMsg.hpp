// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "messages/MessageBase.hpp"
#include "sha3_256.h"
#include <memory>

namespace preprocessor {

#pragma pack(push, 1)
struct PreProcessReplyMsgHeader {
  MessageBase::Header header;
  SeqNum reqSeqNum;
  NodeIdType senderId;
  uint16_t clientId;
  uint8_t resultsHash[concord::util::SHA3_256::SIZE_IN_BYTES];
  uint32_t replyLength;
};
// The pre-executed results' hash signature resides in the message body
#pragma pack(pop)

class PreProcessReplyMsg : public MessageBase {
 public:
  PreProcessReplyMsg(NodeIdType senderId, uint16_t clientId, uint64_t reqSeqNum);

  void setupMsgBody(const char* buf, uint32_t bufLen);

  void validate(const bftEngine::impl::ReplicasInfo&) const override;
  const uint16_t clientId() const { return msgBody()->clientId; }
  const SeqNum reqSeqNum() const { return msgBody()->reqSeqNum; }
  const uint32_t replyLength() const { return msgBody()->replyLength; }
  const uint8_t* resultsHash() const { return msgBody()->resultsHash; }

 private:
  void setParams(NodeIdType senderId, uint16_t clientId, ReqId reqSeqNum);
  PreProcessReplyMsgHeader* msgBody() const { return ((PreProcessReplyMsgHeader*)msgBody_); }

 private:
  static std::unique_ptr<SigManager> sigManager_;

  static uint16_t maxReplyMsgSize_;
};

typedef std::shared_ptr<PreProcessReplyMsg> PreProcessReplyMsgSharedPtr;

}  // namespace preprocessor
