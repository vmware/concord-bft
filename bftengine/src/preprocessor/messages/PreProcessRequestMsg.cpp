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

#include "PreProcessRequestMsg.hpp"
#include "assertUtils.hpp"
#include <cstring>

namespace preprocessor {

PreProcessRequestMsg::PreProcessRequestMsg(NodeIdType senderId,
                                           uint16_t clientId,
                                           uint64_t reqSeqNum,
                                           uint32_t reqLength,
                                           const char* request,
                                           const std::string& cid)
    : MessageBase(senderId, MsgCode::PreProcessRequest, (sizeof(Header) + reqLength + cid.size())) {
  setParams(senderId, clientId, reqSeqNum, reqLength);
  msgBody()->cidLength = cid.size();
  memcpy(body() + sizeof(Header), request, reqLength);
  memcpy(body() + sizeof(Header) + reqLength, cid.c_str(), cid.size());
  uint64_t msgLength = sizeof(Header) + reqLength + cid.size();
  LOG_DEBUG(GL,
            "senderId=" << senderId << " clientId=" << clientId << " reqSeqNum=" << reqSeqNum
                        << " headerSize=" << sizeof(Header) << " reqLength=" << reqLength << " cidSize=" << cid.size()
                        << " msgLength=" << msgLength);
}

void PreProcessRequestMsg::validate(const ReplicasInfo& repInfo) const {
  Assert(type() == MsgCode::PreProcessRequest);
  Assert(senderId() != repInfo.myId());

  if (size() < (sizeof(Header)) || size() < (sizeof(Header) + msgBody()->requestLength + msgBody()->cidLength))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

void PreProcessRequestMsg::setParams(NodeIdType senderId, uint16_t clientId, ReqId reqSeqNum, uint32_t reqLength) {
  msgBody()->senderId = senderId;
  msgBody()->clientId = clientId;
  msgBody()->reqSeqNum = reqSeqNum;
  msgBody()->requestLength = reqLength;
}

std::string PreProcessRequestMsg::getCid() const {
  return std::string(body() + sizeof(Header) + msgBody()->requestLength, msgBody()->cidLength);
}

}  // namespace preprocessor
