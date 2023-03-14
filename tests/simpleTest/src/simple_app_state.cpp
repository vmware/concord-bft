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

#include "simple_app_state.hpp"

uint64_t SimpleAppState::clientToIndex(NodeNum clientId) { return clientId - numOfReplicas; }

uint64_t SimpleAppState::getLastStateValue(NodeNum clientId) {
  auto index = clientToIndex(clientId);
  return statePtr[index].lastValue;
}

uint64_t SimpleAppState::getLastStateNum(NodeNum clientId) {
  auto index = clientToIndex(clientId);
  return statePtr[index].stateNum;
}

void SimpleAppState::setLastStateValue(NodeNum clientId, uint64_t value) {
  auto index = clientToIndex(clientId);
  statePtr[index].lastValue = value;
}

void SimpleAppState::setLastStateNum(NodeNum clientId, uint64_t value) {
  auto index = clientToIndex(clientId);
  statePtr[index].stateNum = value;
}

SimpleAppState::SimpleAppState(uint16_t numCl, uint16_t numRep)
    : statePtr{new SimpleAppState::State[numCl]}, numOfClients{numCl}, numOfReplicas{numRep} {}

SimpleAppState::~SimpleAppState() { delete[] statePtr; }

// Handler for the upcall from Concord-BFT.
void SimpleAppState::execute(ExecutionRequestsQueue &requests,
                             std::optional<Timestamp> timestamp,
                             const std::string &batchCid,
                             concordUtils::SpanWrapper &parent_span) {
  for (auto &req : requests) {
    /* Skip below processing for all requests other than client READ and WRITE */
    if (!(req.flags & READ_ONLY_FLAG) && (req.flags != EMPTY_FLAGS)) continue;

    // Not currently used
    req.outReplicaSpecificInfoSize = 0;

    bool readOnly = req.flags & READ_ONLY_FLAG;
    if (readOnly) {
      // Our read-only request includes only a type, no argument.
      TestAssertReplica(req.requestSize == sizeof(uint64_t), "requestSize =! " << sizeof(uint64_t));

      // We only support the READ operation in read-only mode.
      TestAssertReplica(*reinterpret_cast<const uint64_t *>(req.request) == READ_VAL_REQ,
                        "request is NOT " << READ_VAL_REQ);

      // Copy the latest register value to the reply buffer.
      TestAssertReplica(req.maxReplySize >= sizeof(uint64_t), "maxReplySize < " << sizeof(uint64_t));
      uint64_t *pRet = const_cast<uint64_t *>(reinterpret_cast<const uint64_t *>(req.outReply));
      auto lastValue = getLastStateValue(req.clientId);
      *pRet = lastValue;
      req.outActualReplySize = sizeof(uint64_t);
    } else {
      // Our read-write request includes one eight-byte argument, in addition to
      // the request type.
      TestAssertReplica(req.requestSize == 2 * sizeof(uint64_t), "requestSize != " << 2 * sizeof(uint64_t));

      // We only support the WRITE operation in read-write mode.
      const uint64_t *pReqId = reinterpret_cast<const uint64_t *>(req.request);
      TestAssertReplica(*pReqId == SET_VAL_REQ, "*preqId != " << SET_VAL_REQ);

      // The value to write is the second eight bytes of the request.
      const uint64_t *pReqVal = (pReqId + 1);

      // Modify the register state.
      setLastStateValue(req.clientId, *pReqVal);
      // Count the number of times we've modified it.
      auto stateNum = getLastStateNum(req.clientId);
      setLastStateNum(req.clientId, stateNum + 1);

      // Reply with the number of times we've modified the register.
      TestAssertReplica(req.maxReplySize >= sizeof(uint64_t), "maxReplySize < " << sizeof(uint64_t));
      uint64_t *pRet = const_cast<uint64_t *>(reinterpret_cast<const uint64_t *>(req.outReply));
      *pRet = stateNum;
      req.outActualReplySize = sizeof(uint64_t);

      st->markUpdate(statePtr, sizeof(State) * numOfClients);
    }
    req.outExecutionStatus = 0;  // SUCCESS
  }
}

void SimpleAppState::preExecute(IRequestsHandler::ExecutionRequest &req,
                                std::optional<Timestamp> timestamp,
                                const std::string &batchCid,
                                concordUtils::SpanWrapper &parent_span) {}
