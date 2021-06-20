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

#pragma once

#include "KVBCInterfaces.h"
#include "bftengine/SimpleClient.hpp"

namespace concord::kvbc {

class ClientImp : public IClient {
 public:
  // IClient methods
  Status start() override;
  Status stop() override;

  bool isRunning() override;

  Status invokeCommandSynch(const char* request,
                            uint32_t requestSize,
                            uint8_t flags,
                            std::chrono::milliseconds timeout,
                            uint32_t replySize,
                            char* outReply,
                            uint32_t* outActualReplySize,
                            const std::string& cid,
                            const std::string& span_context) override;

  void setMetricsAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) override;

 protected:
  ClientImp() = default;
  ~ClientImp() override = default;

  ClientConfig config_;
  std::unique_ptr<bftEngine::SeqNumberGeneratorForClientRequests> seqGen_;
  bft::communication::ICommunication* comm_ = nullptr;

  bftEngine::SimpleClient* bftClient_ = nullptr;

  friend IClient* createClient(const ClientConfig& conf, bft::communication::ICommunication* comm);
};

}  // namespace concord::kvbc
