// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ClientImp.h"

using bftEngine::ICommunication;

namespace SimpleKVBC {

IClient* createClient(const ClientConfig& conf,
                      bftEngine::ICommunication* comm) {
  ClientImp* c = new ClientImp();

  c->config_ = conf;
  c->replyBuf_ = (char*)std::malloc(conf.maxReplySize);
  c->seqGen_ = bftEngine::SeqNumberGeneratorForClientRequests::
      createSeqNumberGeneratorForClientRequests();
  c->comm_ = comm;
  c->bftClient_ = nullptr;

  return c;
}

Status ClientImp::start() {
  if (isRunning()) return Status::IllegalOperation("todo");

  comm_->Start();

  uint16_t fVal = config_.fVal;
  uint16_t cVal = config_.cVal;
  uint16_t clientId = config_.clientId;
  bftClient_ =
      bftEngine::SimpleClient::createSimpleClient(comm_, clientId, fVal, cVal);

  return Status::OK();
}

Status ClientImp::stop() {
  // TODO: implement
  return Status::IllegalOperation("Not implemented");
}

bool ClientImp::isRunning() { return (bftClient_ != nullptr); }

Status ClientImp::invokeCommandSynch(const Slice command,
                                     bool isReadOnly,
                                     Slice& outReply) {
  if (!isRunning()) return Status::IllegalOperation("todo");

  uint32_t replySize = 0;

  bftClient_->sendRequest(isReadOnly,
                          command.data,
                          command.size,
                          seqGen_->generateUniqueSequenceNumberForRequest(),
                          bftEngine::SimpleClient::INFINITE_TIMEOUT,
                          config_.maxReplySize,
                          replyBuf_,
                          replySize);

  char* p = (char*)std::malloc(replySize);
  memcpy(p, replyBuf_, replySize);
  memset(replyBuf_, 0, replySize);

  outReply = Slice(p, replySize);

  return Status::OK();
}

Status ClientImp::release(Slice& slice) {
  if (slice.size > 0) {
    char* p = (char*)slice.data;
    std::free(p);
    slice = Slice();
  }

  return Status::OK();
}
}  // namespace SimpleKVBC
