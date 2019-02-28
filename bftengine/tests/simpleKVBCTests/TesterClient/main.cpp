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

#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <stdlib.h>

#include "KVBCInterfaces.h"
#include "simpleKVBCTests.h"
#include "CommFactory.hpp"
#include "TestDefs.h"

#ifndef _WIN32
#include <sys/param.h>
#include <unistd.h>
#else
#include "winUtils.h"
#endif

using namespace SimpleKVBC;

using std::string;

int main(int argc, char** argv) {
#if defined(_WIN32)
  initWinSock();
#endif

  char argTempBuffer[PATH_MAX + 10];

  uint16_t clientId = UINT16_MAX;
  uint16_t fVal = UINT16_MAX;
  uint16_t cVal = UINT16_MAX;
  uint32_t numOfOps = UINT32_MAX;

  int o = 0;
  while ((o = getopt(argc, argv, "i:f:c:p:")) != EOF) {
    switch (o) {
      case 'i': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && tempId < UINT16_MAX) clientId = (uint16_t)tempId;
        // TODO: check clientId
      } break;

      case 'f': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string fStr = argTempBuffer;
        int tempfVal = std::stoi(fStr);
        if (tempfVal >= 1 && tempfVal < UINT16_MAX) fVal = (uint16_t)tempfVal;
        // TODO: check fVal
      } break;

      case 'c': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string cStr = argTempBuffer;
        int tempcVal = std::stoi(cStr);
        if (tempcVal >= 0 && tempcVal < UINT16_MAX) cVal = (uint16_t)tempcVal;
        // TODO: check cVal
      } break;

      case 'p': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string numOfOpsStr = argTempBuffer;
        int tempfVal = std::stoi(numOfOpsStr);
        if (tempfVal >= 1 && tempfVal < UINT32_MAX)
          numOfOps = (uint32_t)tempfVal;
        // TODO: check numOfOps
      } break;

      default:
        // nop
        break;
    }
  }

  if (clientId == UINT16_MAX || fVal == UINT16_MAX || cVal == UINT16_MAX ||
      numOfOps == UINT32_MAX) {
    fprintf(stderr, "%s -f F -c C -p NUM_OPS -i ID", argv[0]);
    exit(-1);
  }

  // TODO: check arguments

  const uint16_t numOfReplicas = 3 * fVal + 2 * cVal + 1;
  const uint16_t port = basePort + clientId * 2;

  std::unordered_map<NodeNum, NodeInfo> nodes;
  for (int i = 0; i < (numOfReplicas + numOfClientProxies); i++) {
    nodes.insert(
        {i,
         NodeInfo{ipAddress, (uint16_t)(basePort + i * 2), i < numOfReplicas}});
  }

  bftEngine::PlainUdpConfig commConfig(
      ipAddress, port, maxMsgSize, nodes, clientId);
  bftEngine::ICommunication* comm = bftEngine::CommFactory::create(commConfig);

  ClientConfig config;

  config.clientId = clientId;
  config.fVal = fVal;
  config.cVal = cVal;
  config.maxReplySize = maxMsgSize;

  IClient* c = createClient(config, comm);

  BasicRandomTests::run(c, numOfOps);
}