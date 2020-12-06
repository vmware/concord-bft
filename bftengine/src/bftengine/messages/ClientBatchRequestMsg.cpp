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

#include "ClientBatchRequestMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine::impl {

ClientBatchRequestMsg::ClientBatchRequestMsg(NodeIdType clientId,
                                             const std::deque<ClientRequestMsg*>& batch,
                                             uint32_t batchBufSize)
    : MessageBase(clientId, MsgCode::ClientBatchRequest, 0, sizeof(ClientBatchRequestMsgHeader) + batchBufSize) {
  msgBody()->clientId = clientId;
  msgBody()->numOfMessagesInBatch = batch.size();
  msgBody()->batchSize = batchBufSize;
  char* data = body() + sizeof(ClientBatchRequestMsgHeader);
  for (auto const& msg : batch) {
    memcpy(data, msg->body(), msg->size());
    data += msg->size();
  }
}

void ClientBatchRequestMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(senderId() != repInfo.myId());
  if (size() < sizeof(ClientBatchRequestMsgHeader) ||
      size() < (sizeof(ClientBatchRequestMsgHeader) + msgBody()->batchSize))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

}  // namespace bftEngine::impl
