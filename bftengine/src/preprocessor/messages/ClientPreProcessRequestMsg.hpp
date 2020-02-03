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

#include "messages/ClientRequestMsg.hpp"
#include "ReplicasInfo.hpp"
#include <memory>

namespace preprocessor {

// This message is created when a client sends a request that contains PRE_PROCESS_REQ flag turned on.

class ClientPreProcessRequestMsg : public ClientRequestMsg {
 public:
  ClientPreProcessRequestMsg(
      NodeIdType sender, bool isReadOnly, uint64_t reqSeqNum, uint32_t requestLength, const char* request)
      : ClientRequestMsg(sender, isReadOnly, reqSeqNum, reqSeqNum, request) {
    msgBody_->msgType = MsgCode::ClientPreProcessRequest;
  }

  void validate(const bftEngine::impl::ReplicasInfo&) const override;

  std::unique_ptr<MessageBase> convertToClientRequestMsg();
};

typedef std::shared_ptr<ClientPreProcessRequestMsg> ClientPreProcessReqMsgSharedPtr;

}  // namespace preprocessor
