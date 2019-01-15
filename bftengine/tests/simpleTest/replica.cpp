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
#include <csignal>

// bftEngine includes
#include "CommFactory.hpp"
#include "Replica.hpp"
#include "ReplicaConfig.hpp"
#include "SimpleStateTransfer.hpp"
#include "Logging.hpp"

// simpleTest includes
#include "commonDefs.h"

#ifdef USE_LOG4CPP
#include <log4cplus/configurator.h>
#endif

concordlogger::Logger replicaLogger =
    concordlogger::Logger::getLogger("simpletest.replica");
bftEngine::Replica* replica = nullptr;

struct ReplicaParams {
  uint16_t replicaId;
  uint16_t numOfReplicas = 4;
  uint16_t numOfClients = 1;
  bool debug = false;
  bool viewChangeEnabled = false;
  uint32_t viewChangeTimeout = 60000; // ms
} rp;

void signalHandler( int signum ) {
  if(replica)
    replica->stop();

  LOG_INFO(replicaLogger, "replica " << rp.replicaId << " stopped");
  exit(0);
}

#define test_assert(statement, message) \
{ if (!(statement)) { \
LOG_FATAL(logger, "assert fail with message: " << message); assert(false);}}

using bftEngine::ICommunication;
using bftEngine::PlainUDPCommunication;
using bftEngine::PlainUdpConfig;
using bftEngine::PlainTCPCommunication;
using bftEngine::PlainTcpConfig;
using bftEngine::Replica;
using bftEngine::ReplicaConfig;
using bftEngine::RequestsHandler;
using namespace std;

// Declarations of functions from config.cpp.
void getReplicaConfig(uint16_t replicaId, bftEngine::ReplicaConfig* outConfig);
extern PlainUdpConfig getUDPConfig(
    uint16_t id, int numOfClients, int numOfReplicas);
extern PlainTcpConfig getTCPConfig(
    uint16_t id, int numOfClients, int numOfReplicas);

void parse_params(int argc, char** argv) {
  if(argc < 2) {
    throw std::runtime_error("Unable to read replica id");
  }

  uint16_t min16_t_u = std::numeric_limits<uint16_t>::min();
  uint16_t max16_t_u = std::numeric_limits<uint16_t>::max();
  uint32_t min32_t_u = std::numeric_limits<uint32_t>::min();
  uint32_t max32_t_u = std::numeric_limits<uint32_t>::max();

  if(argc < 3) { // backward compatibility, only ID is passed
    auto replicaId =  std::stoi(argv[1]);
    if (replicaId < min16_t_u || replicaId > max16_t_u) {
      printf("-id value is out of range (%hu - %hu)", min16_t_u, max16_t_u);
      exit(-1);
    }
    rp.replicaId = replicaId;
  } else {
    try {
      for (int i = 1; i < argc;) {
        string p(argv[i]);
        if (p == "-r") {
          auto numRep = std::stoi(argv[i + 1]);
          if (numRep < min16_t_u || numRep > max16_t_u) {
            printf("-r value is out of range (%hu - %hu)",
                   min16_t_u,
                   max16_t_u);
            exit(-1);
          }
          rp.numOfReplicas = numRep;
          i += 2;
        } else if (p == "-id") {
          auto repId = std::stoi(argv[i + 1]);
          if (repId < min16_t_u || repId > max16_t_u) {
            printf("-id value is out of range (%hu - %hu)",
                   min16_t_u,
                   max16_t_u);
            exit(-1);
          }
          rp.replicaId = repId;
          i += 2;
        } else if (p == "-c") {
          auto numCl = std::stoi(argv[i + 1]);
          if (numCl < min16_t_u || numCl > max16_t_u) {
            printf("-c value is out of range (%hu - %hu)",
                   min16_t_u,
                   max16_t_u);
            exit(-1);
          }
          rp.numOfClients = numCl;
          i += 2;
        } else if (p == "-debug") {
          rp.debug = true;
          i++;
        } else if (p == "-vc") {
          rp.viewChangeEnabled = true;
          i++;
        } else if (p == "-vct") {
          auto vct = std::stoi(argv[i + 1]);
          if (vct < min32_t_u || vct > max32_t_u) {
            printf("-vct value is out of range (%u - %u)", min16_t_u,
                   max16_t_u);
            exit(-1);
          }
          rp.viewChangeTimeout = vct;
          i += 2;
        } else {
          printf("Unknown parameter %s\n", p.c_str());
          exit(-1);
        }
      }
    } catch (std::invalid_argument &e) {
        printf("Parameters should be integers only\n");
        exit(-1);
      } catch (std::out_of_range &e) {
        printf("One of the parameters is out of range\n");
        exit(-1);
      }
  }

  LOG_INFO(replicaLogger, "ReplicaParams: replicaId: " << rp.replicaId
  << ", numOfReplicas: " << rp.numOfReplicas
  << ", numOfClients: " << rp.numOfClients
  << ", vcEnabled: " << rp.viewChangeEnabled
  << ", vcTimeout: " << rp.viewChangeTimeout
  << ", debug: " << rp.debug);
}

// The replica state machine.
class SimpleAppState : public RequestsHandler {
 private:
  uint64_t client_to_index(NodeNum clientId) {
    return clientId - numOfReplicas;
  }

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

  SimpleAppState(uint16_t numCl, uint16_t numRep) :
    statePtr{new SimpleAppState::State[numCl]},
    numOfClients{numCl},
    numOfReplicas{numRep} {}

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
      test_assert(requestSize == sizeof(uint64_t),
          "requestSize =! " << sizeof(uint64_t));

      // We only support the READ operation in read-only mode.
      test_assert(*reinterpret_cast<const uint64_t*>(request) == READ_VAL_REQ,
          "request is NOT " << READ_VAL_REQ);

      // Copy the latest register value to the reply buffer.
      test_assert(maxReplySize >= sizeof(uint64_t),
          "maxReplySize < " << sizeof(uint64_t));
      uint64_t* pRet = reinterpret_cast<uint64_t*>(outReply);
      auto lastValue = get_last_state_value(clientId);
      *pRet = lastValue;
      outActualReplySize = sizeof(uint64_t);
    } else {
      // Our read-write request includes one eight-byte argument, in addition to
      // the request type.
      test_assert(requestSize == 2 * sizeof(uint64_t),
          "requestSize != " << 2 * sizeof(uint64_t));

      // We only support the WRITE operation in read-write mode.
      const uint64_t* pReqId = reinterpret_cast<const uint64_t*>(request);
      test_assert(*pReqId == SET_VAL_REQ, "*preqId != " << SET_VAL_REQ);

      // The value to write is the second eight bytes of the request.
      const uint64_t* pReqVal = (pReqId + 1);

      // Modify the register state.
      set_last_state_value(clientId, *pReqVal);
      // Count the number of times we've modified it.
      auto stateNum = get_last_state_num(clientId);
      set_last_state_num(clientId, stateNum + 1);

      // Reply with the number of times we've modified the register.
      test_assert(maxReplySize >= sizeof(uint64_t),
          "maxReplySize < " << sizeof(uint64_t));
      uint64_t* pRet = reinterpret_cast<uint64_t*>(outReply);
      *pRet = stateNum;
      outActualReplySize = sizeof(uint64_t);

      st->markUpdate(statePtr, sizeof(State) * numOfClients);
    }

    return 0;
  }

  struct State {
    // Number of modifications made.
    uint64_t stateNum = 0;
    // Register value.
    uint64_t lastValue = 0;
  };
  State *statePtr;

  uint16_t numOfClients;
  uint16_t numOfReplicas;

  concordlogger::Logger logger = concordlogger::Logger::getLogger
      ("simpletest.replica");

  bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer* st = nullptr;
};

int main(int argc, char **argv) {
#ifdef USE_LOG4CPP
  using namespace log4cplus;
  initialize();
  BasicConfigurator config;
  config.configure();
#endif
  parse_params(argc, argv);

  // allows to attach debugger
  if(rp.debug)
    std::this_thread::sleep_for(chrono::seconds(20));

  signal(SIGABRT, signalHandler);
  signal(SIGTERM, signalHandler);
  signal(SIGKILL, signalHandler);

  ReplicaConfig replicaConfig;
  getReplicaConfig(rp.replicaId, &replicaConfig);
  replicaConfig.numOfClientProxies = rp.numOfClients;
  replicaConfig.autoViewChangeEnabled = rp.viewChangeEnabled;
  replicaConfig.viewChangeTimerMillisec = rp.viewChangeTimeout;

  LOG_DEBUG(replicaLogger,
      "ReplicaConfig: replicaId: " << replicaConfig.replicaId
      << ", fVal: " << replicaConfig.fVal
      << ", cVal: " << replicaConfig.cVal
      << ", autoViewChangeEnabled: " << replicaConfig.autoViewChangeEnabled
      << ", viewChangeTimerMillisec: " << rp.viewChangeTimeout);

#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf = getTCPConfig(rp.replicaId, rp.numOfClients, rp.numOfReplicas);
#else
  PlainUdpConfig conf = getUDPConfig(
      rp.replicaId, rp.numOfClients, rp.numOfReplicas);
#endif
  ICommunication* comm = bftEngine::CommFactory::create(conf);

  // This is the state machine that the replica will drive.
  SimpleAppState simpleAppState(rp.numOfClients, rp.numOfReplicas);

  bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer* st =
    bftEngine::SimpleInMemoryStateTransfer::create(
        simpleAppState.statePtr,
        sizeof(SimpleAppState::State) * rp.numOfClients,
        replicaConfig.replicaId,
        replicaConfig.fVal,
        replicaConfig.cVal, true);

  simpleAppState.st = st;

  replica = Replica::createNewReplica(
      &replicaConfig,
      &simpleAppState,
      st,
      comm,
      nullptr);

  replica->start();

  // The replica is now running in its own thread. Block the main thread until
  // sigabort, sigkill or sigterm are not raised and then exit gracefully

  while (replica->isRunning())
    std::this_thread::sleep_for(std::chrono::seconds(1));

  return 0;
}
