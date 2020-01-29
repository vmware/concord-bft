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

#include "ClientPreProcessRequestMsg.hpp"
#include "assertUtils.hpp"

namespace preprocessor {

using namespace std;
using namespace bftEngine;

void ClientPreProcessRequestMsg::validate(const ReplicasInfo& repInfo) const {
  Assert(type() == MsgCode::ClientPreProcessRequest);
  Assert(senderId() != repInfo.myId());

  if (size() < (sizeof(ClientRequestMsgHeader) + msgBody()->requestLength))
    throw std::runtime_error(__PRETTY_FUNCTION__);
}

unique_ptr<MessageBase> ClientPreProcessRequestMsg::convertToClientRequestMsg() {
  msgBody_->msgType = MsgCode::ClientRequest;
  return unique_ptr<MessageBase>((MessageBase*)this);
}

}  // namespace preprocessor
