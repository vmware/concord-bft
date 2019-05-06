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
#include <limits>

// bftEngine includes
#include "CommFactory.hpp"
#include "SimpleClient.hpp"

// simpleTest includes
#include "commonDefs.h"
#include "test_comm_config.hpp"
#include "test_parameters.hpp"
#include "Logging.hpp"
#include "histogram.hpp"
#include "misc.hpp"

#ifdef USE_LOG4CPP
#include <log4cplus/configurator.h>
#endif

using bftEngine::ICommunication;
using bftEngine::PlainUDPCommunication;
using bftEngine::PlainUdpConfig;
using bftEngine::PlainTCPCommunication;
using bftEngine::PlainTcpConfig;
using bftEngine::TlsTCPCommunication;
using bftEngine::TlsTcpConfig;
using bftEngine::SeqNumberGeneratorForClientRequests;
using bftEngine::SimpleClient;

concordlogger::Logger clientLogger =
    concordlogger::Logger::getLogger("simpletest.client");

#define test_assert(statement, message) \
{ if (!(statement)) { \
LOG_FATAL(clientLogger, "assert fail with message: " << message); assert(false);}}

void parse_params(int argc, char** argv, ClientParams &cp,
    bftEngine::SimpleClientParams &scp) {
  if(argc < 2)
    return;

  uint16_t min16_t_u = std::numeric_limits<uint16_t>::min();
  uint16_t max16_t_u = std::numeric_limits<uint16_t>::max();
  uint32_t min32_t = std::numeric_limits<uint32_t>::min();
  uint32_t max32_t = std::numeric_limits<uint32_t>::max();

  try {
    for (int i = 1; i < argc;) {
      string p(argv[i]);
      if (p == "-i") {
        auto numOp = std::stoi(argv[i + 1]);
        if (numOp < min32_t || numOp > max32_t) {
          printf("-i value is out of range (%u - %u)\n", min32_t, max32_t);
          exit(-1);
        }
        cp.numOfOperations = (uint32_t)numOp;
      } else if (p == "-id") {
        auto clId = std::stoi(argv[i + 1]);
        if (clId < min16_t_u || clId > max16_t_u) {
          printf(
              "-id value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        cp.clientId = (uint16_t)clId;
      } else if (p == "-r") {
        auto numRep = std::stoi(argv[i + 1]);
        if (numRep < min16_t_u || numRep > max16_t_u) {
          printf("-r value is out of range (%hu - %hu)\n", min16_t_u,
              max16_t_u);
          exit(-1);
        }
        cp.numOfReplicas = (uint16_t)numRep;
      } else if (p == "-cl") {
        auto numCl = std::stoi(argv[i + 1]);
        if (numCl < min16_t_u || numCl > max16_t_u) {
          printf("-cl value is out of range (%hu - %hu)\n", min16_t_u,
          max16_t_u);
          exit(-1);
        }
        cp.numOfClients = (uint16_t)numCl;
      } else if (p == "-c") {
        auto numSlow = std::stoi(argv[i + 1]);
        if (numSlow < min16_t_u || numSlow > max16_t_u) {
          printf(
              "-c value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        cp.numOfSlow = (uint16_t)numSlow;
      } else if (p == "-f") {
        auto numF = std::stoi(argv[i + 1]);
        if (numF < min16_t_u || numF > max16_t_u) {
          printf(
              "-f value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        cp.numOfFaulty = (uint16_t)numF;
      } else if (p == "-irt") {
        scp.clientInitialRetryTimeoutMilli = std::stoull(argv[i + 1]);
      } else if (p == "-minrt") {
        scp.clientMinRetryTimeoutMilli = std::stoull(argv[i + 1]);
      } else if (p == "-maxrt") {
        scp.clientMaxRetryTimeoutMilli = std::stoull(argv[i + 1]);
      } else if (p == "-srft") {
        auto srft = std::stoi(argv[i + 1]);
        if (srft < min16_t_u || srft > max16_t_u) {
          printf(
              "-srft value is out of range (%hu - %hu)\n", min16_t_u,
              max16_t_u);
          exit(-1);
        }
        scp.clientSendsRequestToAllReplicasFirstThresh = (uint16_t)srft;
      } else if (p == "-srpt") {
        auto srpt = std::stoi(argv[i + 1]);
        if (srpt < min16_t_u || srpt > max16_t_u) {
          printf(
              "-srpt value is out of range (%hu - %hu)\n", min16_t_u,
              max16_t_u);
          exit(-1);
        }
        scp.clientSendsRequestToAllReplicasPeriodThresh = (uint16_t)srpt;
      } else if (p == "-prt") {
        auto prt = std::stoi(argv[i + 1]);
        if (prt < min16_t_u || prt > max16_t_u) {
          printf(
              "-prt value is out of range (%hu - %hu)\n", min16_t_u,
              max16_t_u);
          exit(-1);
        }
        scp.clientPeriodicResetThresh = (uint16_t)prt;
      } else if (p == "-cf") {
        cp.configFileName = argv[i + 1];
      } else if (p == "-p") {
        cp.measurePerfomance = true;
        i += 1;
        continue; //skip i+=2
      } else {
        printf("Unknown parameter %s\n", p.c_str());
        exit(-1);
      }

      i += 2;
    }
  } catch (std::invalid_argument &e) {
    printf("Parameters should be integers only\n");
    exit(-1);
  } catch (std::out_of_range &e) {
    printf("One of the parameters is out of range\n");
    exit(-1);
  }

}

int main(int argc, char **argv) {
// TODO(IG:) configure Log4Cplus's output format, using default for now
#ifdef USE_LOG4CPP
  using namespace log4cplus;
  initialize();
  BasicConfigurator config;
  config.configure();
#endif

  ClientParams cp;
  bftEngine::SimpleClientParams scp;
  parse_params(argc, argv, cp, scp);

  LOG_INFO(clientLogger, "SimpleClientParams: clientInitialRetryTimeoutMilli: " << scp.clientInitialRetryTimeoutMilli
    << ", clientMinRetryTimeoutMilli: " << scp.clientMinRetryTimeoutMilli
    << ", clientMaxRetryTimeoutMilli: " << scp.clientMaxRetryTimeoutMilli
    << ", clientSendsRequestToAllReplicasFirstThresh: " << scp.clientSendsRequestToAllReplicasFirstThresh
    << ", clientSendsRequestToAllReplicasPeriodThresh: " << scp.clientSendsRequestToAllReplicasPeriodThresh
    << ", clientPeriodicResetThresh: " << scp.clientPeriodicResetThresh);

  // This client's index number. Must be larger than the largest replica index
  // number.
  const uint16_t id = cp.clientId;

  // How often to read the latest value of the register (every `readMod` ops).
  const int readMod = 7;

  // Concord clients must tag each request with a unique sequence number. This
  // generator handles that for us.
  SeqNumberGeneratorForClientRequests* pSeqGen =
      SeqNumberGeneratorForClientRequests::
      createSeqNumberGeneratorForClientRequests();

  TestCommConfig testCommConfig(clientLogger);
  // Configure, create, and start the Concord client to use.
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf = testCommConfig.GetTCPConfig(
      false, id, cp.numOfClients, cp.numOfReplicas, cp.configFileName);
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(
      false, id, cp.numOfClients, cp.numOfReplicas, cp.configFileName);
#else
  PlainUdpConfig conf = testCommConfig.GetUDPConfig(
      false, id, cp.numOfClients, cp.numOfReplicas, cp.configFileName);
#endif

  LOG_INFO(clientLogger, "ClientParams: clientId: "
                         << cp.clientId
                         << ", numOfReplicas: " << cp.numOfReplicas
                         << ", numOfClients: " << cp.numOfClients
                         << ", numOfIterations: " << cp.numOfOperations
                         << ", fVal: " << cp.numOfFaulty
                         << ", cVal: " << cp.numOfSlow);

  // Perform this check once all parameters configured.
  if (3 * cp.numOfFaulty + 2 * cp.numOfSlow + 1 != cp.numOfReplicas) {
    LOG_FATAL(clientLogger, "Number of replicas is not equal to 3f + 2c + 1 :"
                            " f=" << cp.numOfFaulty << ", c=" << cp.numOfSlow <<
                            ", numOfReplicas=" << cp.numOfReplicas);
    exit(-1);
  }

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

  concordUtils::Histogram hist;
  hist.Clear();

  for (int i = 1; i <= cp.numOfOperations; i++) {

    // the python script that runs the client needs to know how many
    // iterations has been done - that's the reason we use printf and not
    // logging module - to keep the output exactly as we expect.
    if(i > 0 && i % 100 == 0) {
      printf("Iterations count: 100\n");
      printf("Total iterations count: %i\n", i);
    }

    uint64_t start = get_monotonic_time();
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
    uint64_t end = get_monotonic_time();
    uint64_t elapsedMicro = end - start;

    if(cp.measurePerfomance) {
      hist.Add(elapsedMicro);
      LOG_INFO(clientLogger,
          "RAWLatencyMicro " << elapsedMicro
          << " Time " << (uint64_t)(end / 1e3));
    }
  }

  // After all requests have been issued, stop communication and clean up.
  comm->Stop();

  delete pSeqGen;
  delete client;
  delete comm;

  if(cp.measurePerfomance) {
    LOG_INFO(clientLogger, std::endl << "Performance info:" << std::endl << hist
    .ToString());
  }

  LOG_INFO(clientLogger, "test done, iterations: " << cp.numOfOperations);

  return 0;
}
