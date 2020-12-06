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

#include "ClientPreProcessBatchRequestMsg.hpp"
#include "SimpleClient.hpp"

namespace bftEngine::impl {

ClientPreProcessBatchRequestMsg::ClientPreProcessBatchRequestMsg(NodeIdType clientId,
                                                                 const std::deque<ClientRequestMsg*>& batch,
                                                                 uint32_t batchBufSize)
    : ClientBatchRequestMsg(clientId, batch, batchBufSize) {
  msgBody_->msgType = MsgCode::ClientPreProcessBatchRequest;
}

}  // namespace bftEngine::impl
