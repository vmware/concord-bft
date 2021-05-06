// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
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
#include "assertUtils.hpp"

using namespace bftEngine;
using bft::communication::ICommunication;

namespace concord::kvbc {

IClient* createClient(const ClientConfig& conf, ICommunication* comm) {
  ClientImp* c = new ClientImp();

  c->config_ = conf;
  c->seqGen_ = bftEngine::SeqNumberGeneratorForClientRequests::createSeqNumberGeneratorForClientRequests();
  c->comm_ = comm;
  c->bftClient_ = nullptr;

  return c;
}

Status ClientImp::start() {
  if (isRunning()) return Status::IllegalOperation("todo");

  uint16_t fVal = config_.fVal;
  uint16_t cVal = config_.cVal;
  uint16_t clientId = config_.clientId;
  bftClient_ = bftEngine::SimpleClient::createSimpleClient(comm_, clientId, fVal, cVal);

  // Only start the communication after creating the client, because the client sets the receiver of communication
  comm_->Start();

  return Status::OK();
}

Status ClientImp::stop() {
  // TODO: implement
  if (0 != comm_->Stop()) {
    return Status::GeneralError("No tls runner to stop");
  }
  return Status::OK();
}

bool ClientImp::isRunning() { return (bftClient_ != nullptr); }

Status ClientImp::invokeCommandSynch(const char* request,
                                     uint32_t requestSize,
                                     uint8_t flags,
                                     std::chrono::milliseconds timeout,
                                     uint32_t replySize,
                                     char* outReply,
                                     uint32_t* outActualReplySize,
                                     const std::string& cid,
                                     const std::string& span_context) {
  if (!isRunning()) return Status::IllegalOperation("todo");

  uint64_t timeoutMs = timeout <= std::chrono::milliseconds::zero() ? SimpleClient::INFINITE_TIMEOUT : timeout.count();

  auto res = bftClient_->sendRequest(flags,
                                     request,
                                     requestSize,
                                     seqGen_->generateUniqueSequenceNumberForRequest(),
                                     timeoutMs,
                                     replySize,
                                     outReply,
                                     *outActualReplySize,
                                     cid,
                                     span_context);
  switch (res) {
    case SUCCESS:
      return Status::OK();
    case NOT_READY:
      return Status::InterimError("The system is not ready");
    case TIMEOUT:
      return Status::GeneralError("Command timed out");
    case BUFFER_TOO_SMALL:
      return Status::InvalidArgument("Specified output buffer is too small");
    case INVALID_REQUEST:
      return Status::InvalidArgument("Request is invalid");
  }
  return Status::GeneralError("Unknown error");
}

void ClientImp::setMetricsAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  bftClient_->setAggregator(aggregator);
}

}  // namespace concord::kvbc
