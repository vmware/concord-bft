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

#include <cstdlib>
#include <iostream>
#include <limits>

#include "test_parameters.hpp"
#include "simple_test_client.hpp"
#include "Logger.hpp"

logging::Logger clientLogger = logging::getLogger("simpletest.client");

void parse_params(int argc, char **argv, ClientParams &cp, bftEngine::SimpleClientParams &scp) {
  if (argc < 2) return;

  uint16_t min16_t_u = std::numeric_limits<uint16_t>::min();
  uint16_t max16_t_u = std::numeric_limits<uint16_t>::max();
  uint32_t min32_t = std::numeric_limits<uint32_t>::min();
  uint32_t max32_t = std::numeric_limits<uint32_t>::max();

  try {
    for (int i = 1; i < argc;) {
      string p(argv[i]);
      if (p == "-i") {
        uint32_t numOp = std::stoi(argv[i + 1]);
        if (numOp < min32_t || numOp > max32_t) {
          printf("-i value is out of range (%u - %u)\n", min32_t, max32_t);
          exit(-1);
        }
        cp.numOfOperations = (uint32_t)numOp;
      } else if (p == "-id") {
        auto clId = std::stoi(argv[i + 1]);
        if (clId < min16_t_u || clId > max16_t_u) {
          printf("-id value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        cp.clientId = (uint16_t)clId;
      } else if (p == "-r") {
        auto numRep = std::stoi(argv[i + 1]);
        if (numRep < min16_t_u || numRep > max16_t_u) {
          printf("-r value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        cp.numOfReplicas = (uint16_t)numRep;
      } else if (p == "-cl") {
        auto numCl = std::stoi(argv[i + 1]);
        if (numCl < min16_t_u || numCl > max16_t_u) {
          printf("-cl value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        cp.numOfClients = (uint16_t)numCl;
      } else if (p == "-c") {
        auto numSlow = std::stoi(argv[i + 1]);
        if (numSlow < min16_t_u || numSlow > max16_t_u) {
          printf("-c value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        cp.numOfSlow = (uint16_t)numSlow;
      } else if (p == "-f") {
        auto numF = std::stoi(argv[i + 1]);
        if (numF < min16_t_u || numF > max16_t_u) {
          printf("-f value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
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
          printf("-srft value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        scp.clientSendsRequestToAllReplicasFirstThresh = (uint16_t)srft;
      } else if (p == "-srpt") {
        auto srpt = std::stoi(argv[i + 1]);
        if (srpt < min16_t_u || srpt > max16_t_u) {
          printf("-srpt value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        scp.clientSendsRequestToAllReplicasPeriodThresh = (uint16_t)srpt;
      } else if (p == "-prt") {
        auto prt = std::stoi(argv[i + 1]);
        if (prt < min16_t_u || prt > max16_t_u) {
          printf("-prt value is out of range (%hu - %hu)\n", min16_t_u, max16_t_u);
          exit(-1);
        }
        scp.clientPeriodicResetThresh = (uint16_t)prt;
      } else if (p == "-cf") {
        cp.configFileName = argv[i + 1];
      } else if (p == "-p") {
        cp.measurePerformance = true;
        i += 1;
        continue;  // skip i+=2
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

  ClientParams cp;
  bftEngine::SimpleClientParams scp;
  parse_params(argc, argv, cp, scp);

  LOG_INFO(clientLogger,
           "SimpleClientParams: clientInitialRetryTimeoutMilli: "
               << scp.clientInitialRetryTimeoutMilli << ", clientMinRetryTimeoutMilli: "
               << scp.clientMinRetryTimeoutMilli << ", clientMaxRetryTimeoutMilli: " << scp.clientMaxRetryTimeoutMilli
               << ", clientSendsRequestToAllReplicasFirstThresh: " << scp.clientSendsRequestToAllReplicasFirstThresh
               << ", clientSendsRequestToAllReplicasPeriodThresh: " << scp.clientSendsRequestToAllReplicasPeriodThresh
               << ", clientPeriodicResetThresh: " << scp.clientPeriodicResetThresh);

  SimpleTestClient cl(cp, clientLogger);
  return cl.run() ? EXIT_SUCCESS : EXIT_FAILURE;
}
