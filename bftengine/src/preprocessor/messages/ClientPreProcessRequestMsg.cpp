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

#include "ClientPreProcessRequestMsg.hpp"
#include "SimpleClient.hpp"

namespace preprocessor {

using namespace std;
using namespace bftEngine;

ClientPreProcessRequestMsg::ClientPreProcessRequestMsg(NodeIdType sender,
                                                       uint64_t reqSeqNum,
                                                       uint32_t requestLength,
                                                       const char* request,
                                                       uint64_t reqTimeoutMilli,
                                                       const std::string& cid,
                                                       const std::string& participant_id,
                                                       const concordUtils::SpanContext& spanContext,
                                                       const char* requestSignature,
                                                       uint32_t requestSignatureLen)
    : ClientRequestMsg(sender,
                       PRE_PROCESS_REQ,
                       reqSeqNum,
                       requestLength,
                       request,
                       reqTimeoutMilli,
                       cid,
                       1,
                       spanContext,
                       participant_id,
                       requestSignature,
                       requestSignatureLen) {
  msgBody_->msgType = MsgCode::ClientPreProcessRequest;
}

unique_ptr<MessageBase> ClientPreProcessRequestMsg::convertToClientRequestMsg(bool emptyReq) {
  msgBody()->flags &= ~(1 << 1);  // remove PRE_PROCESS_FLAG
  unique_ptr<MessageBase> clientRequestMsg = make_unique<ClientRequestMsg>(clientProxyId(),
                                                                           flags(),
                                                                           requestSeqNum(),
                                                                           emptyReq ? 0 : requestLength(),
                                                                           requestBuf(),
                                                                           requestTimeoutMilli(),
                                                                           getCid(),
                                                                           result(),
                                                                           spanContext<ClientRequestMsg>(),
                                                                           getParticipantId(),
                                                                           emptyReq ? nullptr : requestSignature(),
                                                                           emptyReq ? 0 : requestSignatureLength());
  return clientRequestMsg;
}

}  // namespace preprocessor
