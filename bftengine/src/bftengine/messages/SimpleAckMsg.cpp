// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "SimpleAckMsg.hpp"
#include "ReplicasInfo.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

SimpleAckMsg::SimpleAckMsg(SeqNum s, ViewNum v, ReplicaId senderId, uint64_t ackData)
    : MessageBase(senderId, MsgCode::SimpleAck, sizeof(Header)) {
  b()->seqNum = s;
  b()->viewNum = v;
  b()->ackData = ackData;
}

void SimpleAckMsg::validate(const ReplicasInfo& repInfo) const {
  if (size() < sizeof(Header) ||
      senderId() == repInfo.myId() ||  // sent from another replica (otherwise, we don't need to convert)
      !repInfo.isIdOfReplica(senderId()))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}
}  // namespace impl
}  // namespace bftEngine
