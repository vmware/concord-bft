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
  ClientPreProcessRequestMsg(NodeIdType sender,
                             uint64_t reqSeqNum,
                             uint32_t requestLength,
                             const char* request,
                             uint64_t reqTimeoutMilli,
                             const std::string& cid,
                             const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  ClientPreProcessRequestMsg(MessageBase* msgBase) : ClientRequestMsg(msgBase) {}

  std::unique_ptr<MessageBase> convertToClientRequestMsg(bool resetPreProcessFlag, bool emptyReq = false);
};

typedef std::unique_ptr<ClientPreProcessRequestMsg> ClientPreProcessReqMsgUniquePtr;

}  // namespace preprocessor
