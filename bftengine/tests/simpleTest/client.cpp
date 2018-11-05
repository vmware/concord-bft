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

// This program implements a client that sends requests to the simple register
// state machine defined in replica.cpp. It sends a preset number of operations
// to the replicas, and occasionally checks that the responses match
// expectations.
//
// Operations alternate:
//
//  1. `readMod-1` write operations, each with a unique value
//    a. Every second write checks that the returned sequence number is as
//       expected.
//  2. Every `readMod`-th operation is a read, which checks that the value
//     returned is the same as the last value written.
//
// The program expects no arguments. See the `scripts/` directory for
// information about how to run the client.

#include <cassert>
#include <thread>
#include <iostream>

// bftEngine includes
#include "CommFactory.hpp"
#include "SimpleClient.hpp"

// simpleTest includes
#include "commonDefs.h"

#include "Logging.hpp"

#ifdef USE_LOG4CPP
#include <log4cplus/configurator.h>
#endif

using bftEngine::ICommunication;
using bftEngine::PlainUDPCommunication;
using bftEngine::PlainUdpConfig;
using bftEngine::PlainTCPCommunication;
using bftEngine::PlainTcpConfig;
using bftEngine::SeqNumberGeneratorForClientRequests;
using bftEngine::SimpleClient;

// Declarations of functions form config.cpp.
extern PlainUdpConfig getUDPConfig(
    uint16_t id, int numOfClients, int numOfReplicas);
extern PlainTcpConfig getTCPConfig(
    uint16_t id, int numOfClients, int numOfReplicas);

concordlogger::Logger clientLogger =
    concordlogger::Logger::getLogger("simpletest.client");

#define test_assert(statement, message) \
{ if (!(statement)) { \
LOG_FATAL(clientLogger, "assert fail with message: " << message); assert(false);}}

struct ClientParams {
  int numOfOperations = 2800;
  int16_t clientId = 4;
  int numOfReplicas = 4;
  int numOfClients = 1;
  int numOfFaulty = 1;
  int numOfSlow = 0;
};

ClientParams parse_params(int argc, char** argv) {
  ClientParams cp;
  if(argc < 2)
    return cp;

  for(int i=1; i < argc;) {
    string p(argv[i]);
    if(p == "-i")
      cp.numOfOperations = std::stoi(argv[i + 1]);
    else if(p == "-id")
      cp.clientId = std::stoi(argv[i + 1]);
    else if(p == "-r")
      cp.numOfReplicas = std::stoi(argv[i + 1]);
    else if(p == "-cl")
      cp.numOfClients = std::stoi(argv[i + 1]);
    else if(p == "-c")
      cp.numOfSlow = std::stoi(argv[i + 1]);
    else if(p == "-f")
      cp.numOfFaulty = std::stoi(argv[i + 1]);

    i += 2;
  }

  return cp;
}

int main(int argc, char **argv) {
// TODO(IG:) configure Log4Cplus's output format, using default for now
#ifdef USE_LOG4CPP
  using namespace log4cplus;
  initialize();
  BasicConfigurator config;
  config.configure();
#endif

  ClientParams cp = parse_params(argc, argv);
  LOG_INFO(clientLogger, "ClientParams: clientId: " << cp.clientId << ", numOfReplicas: " << cp.numOfReplicas << ", numOfClients: " << cp.numOfClients << ", numOfIterations: " << cp.numOfOperations << ", fVal: " << cp.numOfFaulty << ", cVal: " << cp.numOfSlow);

  // This client's index number. Must be larger than the largest replica index
  // number.
  const int16_t id = cp.clientId;

  // How often to read the latest value of the register (every `readMod` ops).
  const int readMod = 7;

  // Concord clients must tag each request with a unique sequence number. This
  // generator handles that for us.
  SeqNumberGeneratorForClientRequests* pSeqGen =
      SeqNumberGeneratorForClientRequests::
      createSeqNumberGeneratorForClientRequests();

  // Configure, create, and start the Concord client to use.
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf = getTCPConfig(id, cp.numOfClients, cp.numOfReplicas);
#else
  PlainUdpConfig conf = getUDPConfig(id, cp.numOfClients, cp.numOfReplicas);
#endif
  ICommunication* comm = bftEngine::CommFactory::create(conf);

  SimpleClient* client =
      SimpleClient::createSimpleClient(comm, id, cp.numOfFaulty, cp.numOfSlow);
  comm->Start();

  // The state number that the latest write operation returned.
  uint64_t expectedStateNum = 0;

  // The expectedStateNum is not valid until we have issued at least one write
  // operation.
  bool hasExpectedStateNum = false;

  // The value that the latest write operation sent.
  uint64_t expectedLastValue = 0;

  // The expectedLastValue is not valid until we have issued at least one write
  // operation.
  bool hasExpectedLastValue = false;

  LOG_INFO(clientLogger, "Starting " << cp.numOfOperations);

  for (int i = 1; i <= cp.numOfOperations; i++) {
    if(i > 0 && i % 100 == 0) {
      printf("Iterations count: 100\n");
      printf("Total iterations count: %i\n", i);
    }

    if (i % readMod == 0) {
      // Read the latest value every readMod-th operation.

      // Prepare request parameters.
      const bool readOnly = true;

      const uint32_t kRequestLength = 1;
      const uint64_t requestBuffer[kRequestLength] = {READ_VAL_REQ};
      const char* rawRequestBuffer =
          reinterpret_cast<const char*>(requestBuffer);
      const uint32_t rawRequestLength = sizeof(uint64_t) * kRequestLength;

      const uint64_t requestSequenceNumber =
          pSeqGen->generateUniqueSequenceNumberForRequest();

      const uint64_t timeout = SimpleClient::INFINITE_TIMEOUT;

      const uint32_t kReplyBufferLength = sizeof(uint64_t);
      char replyBuffer[kReplyBufferLength];
      uint32_t actualReplyLength = 0;

      client->sendRequest(readOnly,
                          rawRequestBuffer, rawRequestLength,
                          requestSequenceNumber,
                          timeout,
                          kReplyBufferLength, replyBuffer, actualReplyLength);

      // Read should respond with eight bytes of data.
      test_assert(actualReplyLength == sizeof(uint64_t),
          "actualReplyLength != " << sizeof(uint64_t));

      // Only assert the last expected value if we have previous set a value.
      if (hasExpectedLastValue)
        test_assert(
            *reinterpret_cast<uint64_t*>(replyBuffer) == expectedLastValue,
            "*reinterpret_cast<uint64_t*>(replyBuffer)!=" << expectedLastValue);
    } else {
      // Send a write, if we're not doing a read.

      // Generate a value to store.
      expectedLastValue = (i + 1)*(i + 7)*(i + 18);

      // Prepare request parameters.
      const bool readOnly = false;

      const uint32_t kRequestLength = 2;
      const uint64_t requestBuffer[kRequestLength] =
          {SET_VAL_REQ, expectedLastValue};
      const char* rawRequestBuffer =
          reinterpret_cast<const char*>(requestBuffer);
      const uint32_t rawRequestLength = sizeof(uint64_t) * kRequestLength;

      const uint64_t requestSequenceNumber =
          pSeqGen->generateUniqueSequenceNumberForRequest();

      const uint64_t timeout = SimpleClient::INFINITE_TIMEOUT;

      const uint32_t kReplyBufferLength = sizeof(uint64_t);
      char replyBuffer[kReplyBufferLength];
      uint32_t actualReplyLength = 0;

      client->sendRequest(readOnly,
                          rawRequestBuffer, rawRequestLength,
                          requestSequenceNumber,
                          timeout,
                          kReplyBufferLength, replyBuffer, actualReplyLength);

      // We can now check the expected value on the next read.
      hasExpectedLastValue = true;

      // Write should respond with eight bytes of data.
      test_assert(actualReplyLength == sizeof(uint64_t),
          "actualReplyLength != " << sizeof(uint64_t));

      uint64_t retVal = *reinterpret_cast<uint64_t*>(replyBuffer);

      // We don't know what state number to expect from the first request. The
      // replicas might still be up from a previous run of this test.
      if (hasExpectedStateNum) {
        // If we had done a previous write, then this write should return the
        // state number right after the state number that that write returned.
        expectedStateNum++;
        test_assert(retVal == expectedStateNum,
            "retVal != " << expectedLastValue);
      } else {
        hasExpectedStateNum = true;
        expectedStateNum = retVal;
      }
    }
  }

  // After all requests have been issued, stop communication and clean up.
  comm->Stop();

  delete pSeqGen;
  delete client;
  delete comm;

  LOG_INFO(clientLogger, "test done, iterations: " << cp.numOfOperations);
  return 0;
}
