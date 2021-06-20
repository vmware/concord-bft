// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "ClientPreProcessRequestMsg.hpp"
#include "ReplicasInfo.hpp"
#include "ClientMsgs.hpp"
#include "Logger.hpp"
#include <deque>

namespace bftEngine::impl {

typedef std::deque<preprocessor::ClientPreProcessReqMsgUniquePtr> ClientMsgsList;

class ClientBatchRequestMsg : public MessageBase {
  static_assert((uint16_t)BATCH_REQUEST_MSG_TYPE == (uint16_t)MsgCode::ClientBatchRequest, "");
  static_assert(sizeof(ClientBatchRequestMsgHeader::clientId) == sizeof(NodeIdType), "");
  static_assert(sizeof(ClientBatchRequestMsgHeader::numOfMessagesInBatch) == sizeof(uint32_t), "");
  static_assert(sizeof(ClientBatchRequestMsgHeader::dataSize) == sizeof(uint32_t), "");
  static_assert(sizeof(ClientBatchRequestMsgHeader) == 16, "ClientBatchRequestMsgHeader size is 16B");

 public:
  ClientBatchRequestMsg(NodeIdType clientId,
                        const std::deque<ClientRequestMsg*>& batch,
                        uint32_t batchBufSize,
                        const std::string& cid);

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(ClientBatchRequestMsg)

  uint16_t clientId() const { return msgBody()->clientId; }
  uint32_t numOfMessagesInBatch() const { return msgBody()->numOfMessagesInBatch; }
  uint32_t batchSize() const { return msgBody()->dataSize; }
  const std::string& getCid();
  ClientMsgsList& getClientPreProcessRequestMsgs();
  void validate(const ReplicasInfo&) const override;

 protected:
  ClientBatchRequestMsgHeader* msgBody() const { return ((ClientBatchRequestMsgHeader*)msgBody_); }

 private:
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }
  std::string cid_;
  ClientMsgsList clientMsgsList_;
};

template <>
inline size_t sizeOfHeader<ClientBatchRequestMsgHeader>() {
  return sizeof(ClientBatchRequestMsgHeader);
}

typedef std::unique_ptr<ClientBatchRequestMsg> ClientBatchRequestMsgUniquePtr;

}  // namespace bftEngine::impl
