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

#include "PreProcessRequestMsg.hpp"
#include "assertUtils.hpp"
#include <cstring>

namespace preprocessor {

PreProcessRequestMsg::PreProcessRequestMsg(NodeIdType senderId,
                                           uint16_t clientId,
                                           uint16_t reqOffsetInBatch,
                                           uint64_t reqSeqNum,
                                           uint64_t reqRetryId,
                                           uint32_t reqLength,
                                           const char* request,
                                           const std::string& cid,
                                           const concordUtils::SpanContext& span_context)
    : MessageBase(
          senderId, MsgCode::PreProcessRequest, span_context.data().size(), (sizeof(Header) + reqLength + cid.size())) {
  setParams(senderId, clientId, reqOffsetInBatch, reqSeqNum, reqRetryId, reqLength);
  msgBody()->cidLength = cid.size();
  auto position = body() + sizeof(Header);
  memcpy(position, span_context.data().data(), span_context.data().size());
  position += span_context.data().size();
  memcpy(position, request, reqLength);
  position += reqLength;
  memcpy(position, cid.c_str(), cid.size());
  uint64_t msgLength = sizeof(Header) + span_context.data().size() + reqLength + cid.size();
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(),
            KVLOG(senderId, clientId, reqSeqNum, reqRetryId, sizeof(Header), reqLength, cid.size(), msgLength));
}

void PreProcessRequestMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(type() == MsgCode::PreProcessRequest);
  ConcordAssert(senderId() != repInfo.myId());

  if (size() < (sizeof(Header)) ||
      size() < (sizeof(Header) + spanContextSize() + msgBody()->requestLength + msgBody()->cidLength))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

void PreProcessRequestMsg::setParams(NodeIdType senderId,
                                     uint16_t clientId,
                                     uint16_t reqOffsetInBatch,
                                     ReqId reqSeqNum,
                                     uint64_t reqRetryId,
                                     uint32_t reqLength) {
  msgBody()->senderId = senderId;
  msgBody()->clientId = clientId;
  msgBody()->reqOffsetInBatch = reqOffsetInBatch;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->reqRetryId = reqRetryId;
  msgBody()->requestLength = reqLength;
}

std::string PreProcessRequestMsg::getCid() const {
  return std::string(body() + sizeof(Header) + spanContextSize() + msgBody()->requestLength, msgBody()->cidLength);
}

}  // namespace preprocessor
