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
#include "test_comm_config.hpp"
#include "test_parameters.hpp"
#include "simple_test_replica.hpp"
#include "Logging.hpp"

#ifdef USE_LOG4CPP
#include <log4cplus/configurator.h>
#endif

#define REPLICA2_RESTART_NO_VC (0)
#define ALL_REPLICAS_RESTART_NO_VC (1)
#define ALL_REPLICAS_RESTART_VC (0)
#define PRIMARY_REPLICA_RESTART_VC (0)
static_assert(REPLICA2_RESTART_NO_VC + ALL_REPLICAS_RESTART_NO_VC + ALL_REPLICAS_RESTART_VC + PRIMARY_REPLICA_RESTART_VC <= 1, "");

using bftEngine::ICommunication;
using bftEngine::PlainUDPCommunication;
using bftEngine::PlainUdpConfig;
using bftEngine::PlainTCPCommunication;
using bftEngine::PlainTcpConfig;
using bftEngine::TlsTcpConfig;
using bftEngine::Replica;
using bftEngine::ReplicaConfig;
using bftEngine::RequestsHandler;
using namespace std;
using namespace bftEngine;

void parse_params(int argc, char** argv, ReplicaParams &rp) {
  if(argc < 2) {
    throw std::runtime_error("Unable to read replica id");
  }

  uint16_t min16_t_u = std::numeric_limits<uint16_t>::min();
  uint16_t max16_t_u = std::numeric_limits<uint16_t>::max();
  uint32_t min32_t_u = std::numeric_limits<uint32_t>::min();
  uint32_t max32_t_u = std::numeric_limits<uint32_t>::max();

  rp.keysFilePrefix = "private_replica_";

  if (argc < 3) { // backward compatibility, only ID is passed
    auto replicaId = std::stoi(argv[1]);
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
          uint32_t vct = std::stoi(argv[i + 1]);
          if (vct < min32_t_u || vct > max32_t_u) {
            printf("-vct value is out of range (%u - %u)", min16_t_u,
                   max16_t_u);
            exit(-1);
          }
          rp.viewChangeTimeout = vct;
          i += 2;
        } else if (p == "-cf") {
          rp.configFileName = argv[i + 1];
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

}

SimpleTestReplica *replica;
void signalHandler( int signum ) {
  if(replica)
    replica->stop();
  exit(0);  
}

int main(int argc, char **argv) {
#ifdef USE_LOG4CPP
  using namespace log4cplus;
  initialize();
  BasicConfigurator config;
  config.configure();
#endif
  ReplicaParams rp;
  parse_params(argc, argv, rp);

  // allows to attach debugger
  if(rp.debug)
    std::this_thread::sleep_for(chrono::seconds(20));

  signal(SIGABRT, signalHandler);
  signal(SIGTERM, signalHandler);

  ReplicaConfig replicaConfig;
  TestCommConfig testCommConfig(replicaLogger);
  testCommConfig.GetReplicaConfig(rp.replicaId, rp.keysFilePrefix, &replicaConfig);
  replicaConfig.numOfClientProxies = rp.numOfClients;
  replicaConfig.autoViewChangeEnabled = rp.viewChangeEnabled;
  replicaConfig.viewChangeTimerMillisec = rp.viewChangeTimeout;

#if REPLICA2_RESTART_VC || ALL_REPLICAS_RESTART_VC || PRIMARY_REPLICA_RESTART_VC
  rp.viewChangeEnabled = true;
#endif

  PersistencyTestInfo pti;
#if REPLICA2_RESTART_NO_VC
  pti.replica2RestartNoVC = true;
#endif
#if REPLICA2_RESTART_VC
  pti.replica2RestartVC = true;
#endif
#if ALL_REPLICAS_RESTART_NO_VC
  pti.allReplicasRestartNoVC = true;
  pti.initialSleepBetweenRestartsMillis = 5000;
  pti.sleepBetweenRestartsMultipler = 1.5;
#endif
#if ALL_REPLICAS_RESTART_VC
  pti.allReplicasRestartVC = true;
  pti.restartDelay = 65000;
#endif
#if PRIMARY_REPLICA_RESTART_VC
  pti.primaryReplicaRestartVC = true;
  pti.restartDelay = 120000;
#endif

  LOG_INFO(replicaLogger, "ReplicaParams: replicaId: "
      << rp.replicaId
      << ", numOfReplicas: " << rp.numOfReplicas
      << ", numOfClients: " << rp.numOfClients
      << ", vcEnabled: " << rp.viewChangeEnabled
      << ", vcTimeout: " << rp.viewChangeTimeout
      << ", debug: " << rp.debug);

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
  SimpleTestReplica *replica = SimpleTestReplica::create_replica(pti, rp);
  replica->start();
  // The replica is now running in its own thread. Block the main thread until
  // sigabort, sigkill or sigterm are not raised and then exit gracefully
  replica->run();
  return 0;
}
