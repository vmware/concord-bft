
// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "InternalBFTClient.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "Logger.hpp"

#include <chrono>
#include <utility>

uint64_t InternalBFTClient::sendRequest(uint64_t flags,
                                        uint32_t requestLength,
                                        const char* request,
                                        const std::string& cid) {
  return sendRequest(flags, requestLength, request, cid, IncomingMsgsStorage::Callback{});
}

uint64_t InternalBFTClient::sendRequest(uint64_t flags,
                                        uint32_t requestLength,
                                        const char* request,
                                        const std::string& cid,
                                        IncomingMsgsStorage::Callback onPoppedFromQueue) {
  auto now = getMonotonicTime().time_since_epoch();
  auto now_ms = std::chrono::duration_cast<std::chrono::microseconds>(now);
  auto sn = now_ms.count();
  auto crm = new ClientRequestMsg(getClientId(), flags, sn, requestLength, request, 60000, cid);
  msgComm_->getIncomingMsgsStorage()->pushExternalMsg(std::unique_ptr<MessageBase>(crm), std::move(onPoppedFromQueue));
  LOG_DEBUG(GL, "Sent internal consensus: seq num [" << sn << "] client id [" << getClientId() << "]");
  return sn;
}
