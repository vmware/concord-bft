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

#ifndef CONCORD_BFT_SIMPLE_TEST_CLIENT_HPP
#define CONCORD_BFT_SIMPLE_TEST_CLIENT_HPP

#include "test_comm_config.hpp"
#include "test_parameters.hpp"
#include "commonDefs.h"
#include "CommFactory.hpp"
#include "SimpleClient.hpp"
#include "CommDefs.hpp"
#include "histogram.hpp"
#include "misc.hpp"

using namespace bftEngine;
using namespace std;

#define test_assert(statement, message)                                 \
  {                                                                     \
    if (!(statement)) {                                                 \
      LOG_FATAL(clientLogger, "assert fail with message: " << message); \
      assert(false);                                                    \
    }                                                                   \
  }

class SimpleTestClient {
 private:
  ClientParams cp;
  concordlogger::Logger clientLogger;

 public:
  SimpleTestClient(ClientParams& clientParams, concordlogger::Logger& logger)
      : cp{clientParams}, clientLogger{logger} {}

  bool run() {
    // This client's index number. Must be larger than the largest replica index
    // number.
    const uint16_t id = cp.clientId;

    // How often to read the latest value of the register (every `readMod` ops).
    const int readMod = 7;

    // Concord clients must tag each request with a unique sequence number. This
    // generator handles that for us.
    SeqNumberGeneratorForClientRequests* pSeqGen =
        SeqNumberGeneratorForClientRequests::createSeqNumberGeneratorForClientRequests();

    TestCommConfig testCommConfig(clientLogger);
    // Configure, create, and start the Concord client to use.
#ifdef USE_COMM_PLAIN_TCP
    PlainTcpConfig conf = testCommConfig.GetTCPConfig(false, id, cp.numOfClients, cp.numOfReplicas, cp.configFileName);
#elif USE_COMM_TLS_TCP
    TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(false, id, cp.numOfClients, cp.numOfReplicas, cp.configFileName);
#else
    PlainUdpConfig conf = testCommConfig.GetUDPConfig(false, id, cp.numOfClients, cp.numOfReplicas, cp.configFileName);
#endif

    LOG_INFO(clientLogger,
             "ClientParams: clientId: " << cp.clientId << ", numOfReplicas: " << cp.numOfReplicas << ", numOfClients: "
                                        << cp.numOfClients << ", numOfIterations: " << cp.numOfOperations
                                        << ", fVal: " << cp.numOfFaulty << ", cVal: " << cp.numOfSlow);

    ICommunication* comm = bftEngine::CommFactory::create(conf);

    SimpleClient* client = SimpleClient::createSimpleClient(comm, id, cp.numOfFaulty, cp.numOfSlow);
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

    concordUtils::Histogram hist;
    hist.Clear();

    LOG_INFO(clientLogger, "Starting " << cp.numOfOperations);

    // Perform this check once all parameters configured.
    if (3 * cp.numOfFaulty + 2 * cp.numOfSlow + 1 != cp.numOfReplicas) {
      LOG_FATAL(clientLogger,
                "Number of replicas is not equal to 3f + 2c + 1 :"
                " f="
                    << cp.numOfFaulty << ", c=" << cp.numOfSlow << ", numOfReplicas=" << cp.numOfReplicas);
      exit(-1);
    }

    for (uint32_t i = 1; i <= cp.numOfOperations; i++) {
      // the python script that runs the client needs to know how many
      // iterations has been done - that's the reason we use printf and not
      // logging module - to keep the output exactly as we expect.
      if (i > 0 && i % 100 == 0) {
        printf("Iterations count: 100\n");
        printf("Total iterations count: %i\n", i);
      }

      uint64_t start = get_monotonic_time();
      if (i % readMod == 0) {
        // Read the latest value every readMod-th operation.

        // Prepare request parameters.
        const uint32_t kRequestLength = 1;
        const uint64_t requestBuffer[kRequestLength] = {READ_VAL_REQ};
        const char* rawRequestBuffer = reinterpret_cast<const char*>(requestBuffer);
        const uint32_t rawRequestLength = sizeof(uint64_t) * kRequestLength;

        const uint64_t requestSequenceNumber = pSeqGen->generateUniqueSequenceNumberForRequest();

        const uint64_t timeout = SimpleClient::INFINITE_TIMEOUT;

        const uint32_t kReplyBufferLength = sizeof(uint64_t);
        char replyBuffer[kReplyBufferLength];
        uint32_t actualReplyLength = 0;

        client->sendRequest(READ_ONLY_REQ,
                            rawRequestBuffer,
                            rawRequestLength,
                            requestSequenceNumber,
                            timeout,
                            kReplyBufferLength,
                            replyBuffer,
                            actualReplyLength);

        // Read should respond with eight bytes of data.
        test_assert(actualReplyLength == sizeof(uint64_t), "actualReplyLength != " << sizeof(uint64_t));

        // Only assert the last expected value if we have previous set a value.
        if (hasExpectedLastValue)
          test_assert(*reinterpret_cast<uint64_t*>(replyBuffer) == expectedLastValue,
                      "*reinterpret_cast<uint64_t*>(replyBuffer)!=" << expectedLastValue);
      } else {
        // Send a write, if we're not doing a read.

        // Generate a value to store.
        expectedLastValue = (i + 1) * (i + 7) * (i + 18);

        // Prepare request parameters.
        const uint32_t kRequestLength = 2;
        const uint64_t requestBuffer[kRequestLength] = {SET_VAL_REQ, expectedLastValue};
        const char* rawRequestBuffer = reinterpret_cast<const char*>(requestBuffer);
        const uint32_t rawRequestLength = sizeof(uint64_t) * kRequestLength;

        const uint64_t requestSequenceNumber = pSeqGen->generateUniqueSequenceNumberForRequest();

        const uint64_t timeout = SimpleClient::INFINITE_TIMEOUT;

        const uint32_t kReplyBufferLength = sizeof(uint64_t);
        char replyBuffer[kReplyBufferLength];
        uint32_t actualReplyLength = 0;

        client->sendRequest(EMPTY_FLAGS_REQ,
                            rawRequestBuffer,
                            rawRequestLength,
                            requestSequenceNumber,
                            timeout,
                            kReplyBufferLength,
                            replyBuffer,
                            actualReplyLength);

        // We can now check the expected value on the next read.
        hasExpectedLastValue = true;

        // Write should respond with eight bytes of data.
        test_assert(actualReplyLength == sizeof(uint64_t), "actualReplyLength != " << sizeof(uint64_t));

        uint64_t retVal = *reinterpret_cast<uint64_t*>(replyBuffer);

        // We don't know what state number to expect from the first request. The
        // replicas might still be up from a previous run of this test.
        if (hasExpectedStateNum) {
          // If we had done a previous write, then this write should return the
          // state number right after the state number that that write returned.
          expectedStateNum++;
          test_assert(retVal == expectedStateNum, "retVal != " << expectedLastValue);
        } else {
          hasExpectedStateNum = true;
          expectedStateNum = retVal;
        }
      }

      uint64_t end = get_monotonic_time();
      uint64_t elapsedMicro = end - start;

      if (cp.measurePerformance) {
        hist.Add(elapsedMicro);
        LOG_INFO(clientLogger, "RAWLatencyMicro " << elapsedMicro << " Time " << (uint64_t)(end / 1e3));
      }
    }

    // After all requests have been issued, stop communication and clean up.
    comm->Stop();

    delete pSeqGen;
    delete client;
    delete comm;

    if (cp.measurePerformance) {
      LOG_INFO(clientLogger,
               std::endl
                   << "Performance info from client " << cp.clientId << std::endl
                   << hist.ToString());
    }

    LOG_INFO(clientLogger, "test done, iterations: " << cp.numOfOperations);
    return true;
  }
};

#endif  // CONCORD_BFT_SIMPLE_TEST_CLIENT_HPP
