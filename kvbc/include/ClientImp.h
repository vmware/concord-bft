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

#pragma once

#include "KVBCInterfaces.h"
#include "SimpleClient.hpp"

using namespace bftEngine;

namespace concord {
namespace kvbc {

class ClientImp : public IClient {
 public:
  // IClient methods
  virtual Status start() override;
  virtual Status stop() override;

  virtual bool isRunning() override;

  virtual Status invokeCommandSynch(const char* request,
                                    uint32_t requestSize,
                                    bool isReadOnly,
                                    std::chrono::milliseconds timeout,
                                    uint32_t replySize,
                                    char* outReply,
                                    uint32_t* outActualReplySize) override;

 protected:
  ClientImp(){};
  ~ClientImp(){};

  ClientConfig config_;
  SeqNumberGeneratorForClientRequests* seqGen_ = nullptr;
  ICommunication* comm_ = nullptr;

  SimpleClient* bftClient_ = nullptr;

  friend IClient* createClient(const ClientConfig& conf, ICommunication* comm);
};
}  // namespace kvbc
}  // namespace concord
