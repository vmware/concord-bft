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

#include <cstddef>
#include <stdint.h>
#include <string>
#include "IStateTransfer.hpp"
#include "ICommunication.hpp"
#include "MetadataStorage.hpp"
#include "ReplicaConfig.hpp"

namespace bftEngine {
class RequestsHandler {
 public:
  virtual int execute(uint16_t clientId,
                      bool readOnly,
                      uint32_t requestSize,
                      const char* request,
                      uint32_t maxReplySize,
                      char* outReply,
                      uint32_t& outActualReplySize) = 0;
};

class Replica {
 public:
  static Replica* createNewReplica(ReplicaConfig* replicaConfig,
                                   RequestsHandler* requestsHandler,
                                   IStateTransfer* stateTransfer,
                                   ICommunication* communication,
                                   MetadataStorage* metadataStorage);

  static Replica* loadExistingReplica(RequestsHandler* requestsHandler,
                                      IStateTransfer* stateTransfer,
                                      ICommunication* communication,
                                      MetadataStorage* metadataStorage);

  virtual ~Replica();

  virtual bool isRunning() const = 0;

  virtual void start() = 0;

  virtual void stop() = 0;
};

}  // namespace bftEngine
