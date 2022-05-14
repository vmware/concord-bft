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

#include "assertUtils.hpp"
#include "SimpleClient.hpp"
#include "commonDefs.h"

using namespace bftEngine;
using namespace bft::communication;
using namespace std;

// NOLINTNEXTLINE(misc-definitions-in-headers)
logging::Logger replicaLogger = logging::getLogger("simpletest.replica");

#define test_assert_replica(statement, message)                                                                   \
  {                                                                                                               \
    if (!(statement)) {                                                                                           \
      LOG_FATAL(replicaLogger, "assert fail with message: " << message); /* NOLINT(bugprone-macro-parentheses) */ \
      ConcordAssert(false);                                                                                       \
    }                                                                                                             \
  }

// The replica state machine.
class SimpleAppState : public IRequestsHandler {
 private:
  uint64_t client_to_index(NodeNum clientId) { return clientId - numOfReplicas; }

  uint64_t get_last_state_value(NodeNum clientId) {
    auto index = client_to_index(clientId);
    return statePtr[index].lastValue;
  }

  uint64_t get_last_state_num(NodeNum clientId) {
    auto index = client_to_index(clientId);
    return statePtr[index].stateNum;
  }

  void set_last_state_value(NodeNum clientId, uint64_t value) {
    auto index = client_to_index(clientId);
    statePtr[index].lastValue = value;
  }

  void set_last_state_num(NodeNum clientId, uint64_t value) {
    auto index = client_to_index(clientId);
    statePtr[index].stateNum = value;
  }

 public:
  SimpleAppState(uint16_t numCl, uint16_t numRep)
      : statePtr{new SimpleAppState::State[numCl]}, numOfClients{numCl}, numOfReplicas{numRep} {}
  ~SimpleAppState() { delete[] statePtr; }

  // Handler for the upcall from Concord-BFT.
  void execute(ExecutionRequestsQueue &requests,
               std::optional<Timestamp> timestamp,
               const std::string &batchCid,
               concordUtils::SpanWrapper &parent_span) override {
    for (auto &req : requests) {
      /* Skip below processing for all other requests other client READ and WRITE */
      if (!(req.flags & READ_ONLY_FLAG) && !(req.flags & EMPTY_FLAGS)) continue;

      // Not currently used
      req.outReplicaSpecificInfoSize = 0;

      bool readOnly = req.flags & READ_ONLY_FLAG;
      if (readOnly) {
        // Our read-only request includes only a type, no argument.
        test_assert_replica(req.requestSize == sizeof(uint64_t), "requestSize =! " << sizeof(uint64_t));

        // We only support the READ operation in read-only mode.
        test_assert_replica(*reinterpret_cast<const uint64_t *>(req.request) == READ_VAL_REQ,
                            "request is NOT " << READ_VAL_REQ);

        // Copy the latest register value to the reply buffer.
        test_assert_replica(req.maxReplySize >= sizeof(uint64_t), "maxReplySize < " << sizeof(uint64_t));
        uint64_t *pRet = const_cast<uint64_t *>(reinterpret_cast<const uint64_t *>(req.outReply));
        auto lastValue = get_last_state_value(req.clientId);
        *pRet = lastValue;
        req.outActualReplySize = sizeof(uint64_t);
      } else {
        // Our read-write request includes one eight-byte argument, in addition to
        // the request type.
        test_assert_replica(req.requestSize == 2 * sizeof(uint64_t), "requestSize != " << 2 * sizeof(uint64_t));

        // We only support the WRITE operation in read-write mode.
        const uint64_t *pReqId = reinterpret_cast<const uint64_t *>(req.request);
        test_assert_replica(*pReqId == SET_VAL_REQ, "*preqId != " << SET_VAL_REQ);

        // The value to write is the second eight bytes of the request.
        const uint64_t *pReqVal = (pReqId + 1);

        // Modify the register state.
        set_last_state_value(req.clientId, *pReqVal);
        // Count the number of times we've modified it.
        auto stateNum = get_last_state_num(req.clientId);
        set_last_state_num(req.clientId, stateNum + 1);

        // Reply with the number of times we've modified the register.
        test_assert_replica(req.maxReplySize >= sizeof(uint64_t), "maxReplySize < " << sizeof(uint64_t));
        uint64_t *pRet = const_cast<uint64_t *>(reinterpret_cast<const uint64_t *>(req.outReply));
        *pRet = stateNum;
        req.outActualReplySize = sizeof(uint64_t);

        st->markUpdate(statePtr, sizeof(State) * numOfClients);
      }
      req.outExecutionStatus = 0;  // SUCCESS
    }
  }

  void preExecute(IRequestsHandler::ExecutionRequest &req,
                  std::optional<Timestamp> timestamp,
                  const std::string &batchCid,
                  concordUtils::SpanWrapper &parent_span) override {}

  struct State {
    // Number of modifications made.
    uint64_t stateNum = 0;
    // Register value.
    uint64_t lastValue = 0;
  };
  State *statePtr;

  uint16_t numOfClients;
  uint16_t numOfReplicas;

  bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer *st = nullptr;
};
