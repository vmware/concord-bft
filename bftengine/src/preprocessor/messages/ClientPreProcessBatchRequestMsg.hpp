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

#include "messages/ClientBatchRequestMsg.hpp"
#include "ReplicasInfo.hpp"

namespace bftEngine::impl {

class ClientPreProcessBatchRequestMsg : public ClientBatchRequestMsg {
 public:
  ClientPreProcessBatchRequestMsg(NodeIdType clientId,
                                  const std::deque<ClientRequestMsg*>& batch,
                                  uint32_t batchBufSize);

  ClientPreProcessBatchRequestMsg(MessageBase* msgBase) : ClientBatchRequestMsg(msgBase) {}
};

}  // namespace bftEngine::impl
