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
#include <sstream>
#include <signal.h>
#include <stdlib.h>
#include <thread>

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

  uint16_t repId = UINT16_MAX;
  string idStr;
  string keysFilePrefix;
  uint16_t fVal = UINT16_MAX;
  uint16_t cVal = UINT16_MAX;

  int o = 0;
  while ((o = getopt(argc, argv, "i:k:f:c:")) != EOF) {
    switch (o) {
      case 'i': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && tempId < UINT16_MAX) repId = (uint16_t)tempId;
        // TODO: check repId
      } break;

      case 'k': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        keysFilePrefix = argTempBuffer;
        // TODO: check keysFilePrefix
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

      default:
        // nop
        break;
    }
  }

  string keysFile;

  if (!keysFilePrefix.empty() && !idStr.empty())
    keysFile = keysFilePrefix + "_" + idStr;

  if (repId == UINT16_MAX || keysFile.empty() || fVal == UINT16_MAX ||
      cVal == UINT16_MAX) {
    fprintf(stderr, "%s -k KEYS_FILE_PREFIX -f F -c C -i ID", argv[0]);
    exit(-1);
  }

  // TODO: check arguments

  const uint16_t numOfReplicas = 3 * fVal + 2 * cVal + 1;

  std::unordered_map<NodeNum, NodeInfo> nodes;
  for (int i = 0; i < (numOfReplicas + numOfClientProxies); i++) {
    nodes.insert(
        {i,
         NodeInfo{ipAddress, (uint16_t)(basePort + i * 2), i < numOfReplicas}});
  }

  bftEngine::PlainUdpConfig commConfig(
      ipAddress, (uint16_t)(basePort + repId * 2), maxMsgSize, nodes, repId);
  bftEngine::ICommunication* comm = bftEngine::CommFactory::create(commConfig);

  ReplicaConfig c;

  c.pathOfKeysfile = keysFile;
  c.replicaId = repId;
  c.fVal = fVal;
  c.cVal = cVal;
  c.numOfClientProxies = numOfClientProxies;
  c.statusReportTimerMillisec = 20 * 1000;
  c.concurrencyLevel = 1;
  c.autoViewChangeEnabled = false;
  c.viewChangeTimerMillisec = 45 * 1000;
  c.maxBlockSize = 2 * 1024 * 1024;  // 2MB

  IReplica* r = createReplica(c, comm, BasicRandomTests::commandsHandler());

  r->start();
  while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
}
