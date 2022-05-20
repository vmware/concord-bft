// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
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

#include "assertUtils.hpp"
#include "SimpleClient.hpp"
#include "commonDefs.h"
#include "Replica.hpp"
#include "SimpleStateTransfer.hpp"
#include "utils.hpp"

using namespace bftEngine;
using namespace bft::communication;
using namespace std;

// The replica state machine.
class SimpleAppState : public bftEngine::IRequestsHandler {
 private:
  uint64_t clientToIndex(NodeNum clientId);
  uint64_t getLastStateValue(NodeNum clientId);
  uint64_t getLastStateNum(NodeNum clientId);
  void setLastStateValue(NodeNum clientId, uint64_t value);
  void setLastStateNum(NodeNum clientId, uint64_t value);

 public:
  SimpleAppState(uint16_t numCl, uint16_t numRep);
  ~SimpleAppState();

  void execute(ExecutionRequestsQueue &requests,
               std::optional<Timestamp> timestamp,
               const std::string &batchCid,
               concordUtils::SpanWrapper &parent_span) override;

  void preExecute(IRequestsHandler::ExecutionRequest &req,
                  std::optional<Timestamp> timestamp,
                  const std::string &batchCid,
                  concordUtils::SpanWrapper &parent_span) override;

  struct State {
    // Number of modifications made.
    uint64_t stateNum = 0;
    // Register value.
    uint64_t lastValue = 0;
  };
  State *statePtr = nullptr;

  uint16_t numOfClients = 0;
  uint16_t numOfReplicas = 0;

  bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer *st = nullptr;
};
