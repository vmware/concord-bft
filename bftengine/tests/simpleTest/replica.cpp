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

// This replica implements a single unsigned 64-bit register. Supported
// operations are:
//
//  1. Read the last value of the register.
//  2. Set the value of the register.
//
// Requests are given as either eight or sixteen byte strings. The first eight
// bytes of a requests specifies the type of the request, and the optional
// additional eight bytes specifies the parameters.
//
// # Read Operation
//
// Request bytes must be equal to `(uint64_t)100`.
//
// Response bytes will be equal to `(uint64_t)value`, where `value` is the value
// that was last written to the register.
//
// # Write Operation
//
// Request bytes must be equal to `(uint64_t)200` followed by `(uint64_t)value`,
// where `value` is the value to write to the register.
//
// Response bytes will be equal to `(uint64_t)sequence_number`, where
// `sequence_number` is the count of how many times the register has been
// written to (including this write, so the first response will be `1`).
//
// # Notes
//
// Endianness is not specified. All replicas are assumed to use the same
// native endianness.
//
// The read request must be specified as readOnly. (See
// bftEngine::SimpleClient::sendRequest.)
//
// Values for request types (the `100` and `200` mentioned above) are defined in
// commonDefs.h
//
// See the `scripts/` directory for information about how to run the replicas.

#include <cassert>
#include <thread>

// bftEngine includes
#include "CommFactory.hpp"
#include "Replica.hpp"
#include "ReplicaConfig.hpp"

// simpleTest includes
#include "commonDefs.h"

#ifdef USE_LOG4CPP
#include <log4cplus/configurator.h>
#endif

using bftEngine::ICommunication;
using bftEngine::PlainUDPCommunication;
using bftEngine::PlainUdpConfig;
using bftEngine::PlainTCPCommunication;
using bftEngine::PlainTcpConfig;
using bftEngine::Replica;
using bftEngine::ReplicaConfig;
using bftEngine::RequestsHandler;

// Declarations of functions from config.cpp.
void getReplicaConfig(uint16_t replicaId, bftEngine::ReplicaConfig* outConfig);
PlainUdpConfig getUDPConfig(uint16_t id);
extern PlainTcpConfig getTCPConfig(uint16_t id);

// The replica state machine.
class SimpleAppState : public RequestsHandler {
 public:
  // Handler for the upcall from Concord-BFT.
  int execute(uint16_t clientId,
              bool readOnly,
              uint32_t requestSize,
              const char* request,
              uint32_t maxReplySize,
              char* outReply,
              uint32_t& outActualReplySize) override {
    if (readOnly) {
      // Our read-only request includes only a type, no argument.
      assert(requestSize == sizeof(uint64_t));

      // We only support the READ operation in read-only mode.
      assert(*reinterpret_cast<const uint64_t*>(request) == READ_VAL_REQ);

      // Copy the latest register value to the reply buffer.
      assert(maxReplySize >= sizeof(uint64_t));
      uint64_t* pRet = reinterpret_cast<uint64_t*>(outReply);
      *pRet = lastValue;
      outActualReplySize = sizeof(uint64_t);
    } else {
      // Our read-write request includes one eight-byte argument, in addition to
      // the request type.
      assert(requestSize == 2 * sizeof(uint64_t));

      // We only support the WRITE operation in read-write mode.
      const uint64_t* pReqId = reinterpret_cast<const uint64_t*>(request);
      assert(*pReqId == SET_VAL_REQ);

      // The value to write is the second eight bytes of the request.
      const uint64_t* pReqVal = (pReqId + 1);

      // Modify the register state.
      lastValue = *pReqVal;
      // Count the number of times we've modified it.
      stateNum++;

      // Reply with the number of times we've modified the register.
      assert(maxReplySize >= sizeof(uint64_t));
      uint64_t* pRet = reinterpret_cast<uint64_t*>(outReply);
      *pRet = stateNum;
      outActualReplySize = sizeof(uint64_t);
    }

    return 0;
  }

 protected:
  // Number of modifications made.
  uint64_t stateNum = 0;
  // Register value.
  uint64_t lastValue = 0;
};

int main(int argc, char **argv) {
#ifdef USE_LOG4CPP
  using namespace log4cplus;
  initialize();
  BasicConfigurator config;
  config.configure();
#endif

  // This program expects one argument: the index number of the replica being
  // started. This index is used to choose the correct config files (which
  // choose the correct keys, ports, etc.).
  if (argc < 2) throw std::runtime_error("Unable to read replica id");
  uint16_t id = (argv[1][0] - '0');
  if (id >= 4) throw std::runtime_error("Illegal replica id");

  ReplicaConfig replicaConfig;
  getReplicaConfig(id, &replicaConfig);

#ifndef USE_COMM_PLAIN_TCP
  PlainUdpConfig conf = getUDPConfig(id);
#else
  PlainTcpConfig conf = getTCPConfig(id);
#endif
  ICommunication* comm = bftEngine::CommFactory::create(conf);
  
  // This is the state machine that the replica will drive.
  SimpleAppState simpleAppState;

  Replica* replica = Replica::createNewReplica(&replicaConfig,
                                               &simpleAppState,
                                               nullptr,
                                               comm,
                                               nullptr);

  replica->start();

  // The replica is now running in its own thread. Block the main thread forever
  // while that one handles requests in the background.
  while (true) std::this_thread::sleep_for(std::chrono::seconds(1));

  return 0;
}
