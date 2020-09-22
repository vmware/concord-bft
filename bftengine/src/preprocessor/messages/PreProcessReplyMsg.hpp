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
#include "sha3_256.h"
#include <memory>

namespace preprocessor {

typedef enum { STATUS_GOOD, STATUS_REJECT } ReplyStatus;

class PreProcessReplyMsg : public MessageBase {
 public:
  PreProcessReplyMsg(bftEngine::impl::SigManagerSharedPtr sigManager,
                     NodeIdType senderId,
                     uint16_t clientId,
                     uint64_t reqSeqNum);

  void setupMsgBody(const char* buf, uint32_t bufLen, const std::string& cid, ReplyStatus status);

  void validate(const bftEngine::impl::ReplicasInfo&) const override;
  const uint16_t clientId() const { return msgBody()->clientId; }
  const SeqNum reqSeqNum() const { return msgBody()->reqSeqNum; }
  const uint32_t replyLength() const { return msgBody()->replyLength; }
  const uint8_t* resultsHash() const { return msgBody()->resultsHash; }
  const uint8_t status() const { return msgBody()->status; }
  std::string getCid() const;

 protected:
#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    SeqNum reqSeqNum;
    NodeIdType senderId;
    uint16_t clientId;
    uint8_t status;
    uint8_t resultsHash[concord::util::SHA3_256::SIZE_IN_BYTES];
    uint32_t replyLength;
    uint32_t cidLength;
  };
// The pre-executed results' hash signature resides in the message body
#pragma pack(pop)

 private:
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }
  void setParams(NodeIdType senderId, uint16_t clientId, ReqId reqSeqNum);
  Header* msgBody() const { return ((Header*)msgBody_); }

 private:
  static uint16_t maxReplyMsgSize_;

  bftEngine::impl::SigManagerSharedPtr sigManager_;
};

typedef std::shared_ptr<PreProcessReplyMsg> PreProcessReplyMsgSharedPtr;

}  // namespace preprocessor
