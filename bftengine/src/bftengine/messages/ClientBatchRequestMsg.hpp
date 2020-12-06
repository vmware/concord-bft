// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "ClientRequestMsg.hpp"
#include "ReplicasInfo.hpp"
#include "ClientMsgs.hpp"
#include <deque>

namespace bftEngine::impl {

class ClientBatchRequestMsg : public MessageBase {
  static_assert((uint16_t)BATCH_REQUEST_MSG_TYPE == (uint16_t)MsgCode::ClientBatchRequest, "");
  static_assert(sizeof(ClientBatchRequestMsgHeader::clientId) == sizeof(NodeIdType), "");
  static_assert(sizeof(ClientBatchRequestMsgHeader::numOfMessagesInBatch) == sizeof(uint32_t), "");
  static_assert(sizeof(ClientBatchRequestMsgHeader::batchSize) == sizeof(uint32_t), "");
  static_assert(sizeof(ClientBatchRequestMsgHeader) == 10, "ClientBatchRequestMsgHeader size is 10B");

 public:
  ClientBatchRequestMsg(NodeIdType clientId, const std::deque<ClientRequestMsg*>& batch, uint32_t batchBufSize);

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(ClientBatchRequestMsg)

  uint16_t clientId() const { return msgBody()->clientId; }
  uint32_t numOfMessagesInBatch() const { return msgBody()->numOfMessagesInBatch; }
  uint32_t batchSize() const { return msgBody()->batchSize; }

  void validate(const ReplicasInfo&) const override;

 protected:
  ClientBatchRequestMsgHeader* msgBody() const { return ((ClientBatchRequestMsgHeader*)msgBody_); }
};

template <>
inline size_t sizeOfHeader<ClientBatchRequestMsgHeader>() {
  return sizeof(ClientBatchRequestMsgHeader);
}

}  // namespace bftEngine::impl
