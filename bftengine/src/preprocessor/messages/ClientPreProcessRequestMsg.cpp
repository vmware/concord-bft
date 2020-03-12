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
#include "SimpleClient.hpp"
#include "assertUtils.hpp"

namespace preprocessor {

using namespace std;
using namespace bftEngine;

ClientPreProcessRequestMsg::ClientPreProcessRequestMsg(
    NodeIdType sender, uint64_t reqSeqNum, uint32_t requestLength, const char* request, const std::string& cid)
    : ClientRequestMsg(sender, PRE_PROCESS_REQ, reqSeqNum, requestLength, request, cid) {
  msgBody_->msgType = MsgCode::ClientPreProcessRequest;
}

unique_ptr<MessageBase> ClientPreProcessRequestMsg::convertToClientRequestMsg(bool resetPreProcessFlag) {
  msgBody_->msgType = MsgCode::ClientRequest;
  if (resetPreProcessFlag) msgBody()->flags &= ~(1 << 1);
  auto msg = unique_ptr<ClientRequestMsg>((ClientRequestMsg*)this);
  releaseOwnership();
  msg->acquireOwnership();
  return msg;
}

}  // namespace preprocessor
