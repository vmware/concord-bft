// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
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
#include <sys/param.h>

#include "KVBCInterfaces.h"
#include "CommFactory.hpp"
#include "test_comm_config.hpp"
#include "test_parameters.hpp"
#include "MetricsServer.hpp"
#include "ReplicaImp.h"
#include "commonKVBTests.hpp"
#include "memorydb/client.h"
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
#include "internalCommandsHandler.hpp"

using namespace SimpleKVBC;
using namespace bftEngine;

using std::string;
using ::TestCommConfig;

ReplicaParams rp;
concordlogger::Logger logger = concordlogger::Log::getLogger("skvbctest.replica");

int main(int argc, char** argv) {
  rp.replicaId = UINT16_MAX;
  rp.viewChangeEnabled = false;
  rp.viewChangeTimeout = 45 * 1000;

  // allows to attach debugger
  if (rp.debug) {
    std::this_thread::sleep_for(std::chrono::seconds(20));
  }

  char argTempBuffer[PATH_MAX + 10];
  string idStr;

  int o = 0;
  while ((o = getopt(argc, argv, "r:i:k:n:s:v:")) != EOF) {
    switch (o) {
      case 'i': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && tempId < UINT16_MAX) rp.replicaId = (uint16_t)tempId;
        // TODO: check repId
      } break;

      case 'k': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        rp.keysFilePrefix = argTempBuffer;
        // TODO: check keysFilePrefix
      } break;

      case 'n': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        rp.configFileName = argTempBuffer;
      } break;
      case 's': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && tempId < UINT16_MAX) rp.statusReportTimerMillisec = (uint16_t)tempId;
      } break;
      case 'v': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && (uint32_t)tempId < UINT32_MAX) {
          rp.viewChangeTimeout = (uint32_t)tempId;
          rp.viewChangeEnabled = true;
        }
      } break;

      default:
        // nop
        break;
    }
  }

  if (rp.replicaId == UINT16_MAX || rp.keysFilePrefix.empty()) {
    fprintf(stderr, "%s -k KEYS_FILE_PREFIX -i ID -n COMM_CONFIG_FILE", argv[0]);
    exit(-1);
  }

  // TODO: check arguments

  // used to get info from parsing the key file
  bftEngine::ReplicaConfig replicaConfig;

  TestCommConfig testCommConfig(logger);
  testCommConfig.GetReplicaConfig(rp.replicaId, rp.keysFilePrefix, &replicaConfig);

  // This allows more concurrency and only affects known ids in the
  // communication classes.
  replicaConfig.numOfClientProxies = 100;
  replicaConfig.autoViewChangeEnabled = rp.viewChangeEnabled;
  replicaConfig.viewChangeTimerMillisec = rp.viewChangeTimeout;
  replicaConfig.statusReportTimerMillisec = rp.statusReportTimerMillisec;
  replicaConfig.concurrencyLevel = 1;
  replicaConfig.debugStatisticsEnabled = true;

  uint16_t numOfReplicas = (uint16_t)(3 * replicaConfig.fVal + 2 * replicaConfig.cVal + 1);
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf = testCommConfig.GetTCPConfig(
      true, rp.replicaId, replicaConfig.numOfClientProxies, numOfReplicas, rp.configFileName);
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(
      true, rp.replicaId, replicaConfig.numOfClientProxies, numOfReplicas, rp.configFileName);
#else
  PlainUdpConfig conf = testCommConfig.GetUDPConfig(
      true, rp.replicaId, replicaConfig.numOfClientProxies, numOfReplicas, rp.configFileName);
#endif

  ICommunication* comm = CommFactory::create(conf);

  // UDP MetricsServer only used in tests.
  uint16_t metricsPort = conf.listenPort + 1000;
  concordMetrics::Server server(metricsPort);
  server.Start();

  auto* key_manipulator = new concord::storage::blockchain::KeyManipulator();
  auto comparator = concord::storage::memorydb::KeyComparator(key_manipulator);
  concord::storage::IDBClient* db = new concord::storage::memorydb::Client(comparator);

  // TODO(AJS) - We want 2 separate builds for the tester replica:
  //  1. Using MemoryDb
  //  2. Using RocksDB
  //
  //  The code below is what needs to change to use RocksDB. Remove the 2
  //  lines of code before this comment and uncomment out the following four
  //  lines of code. SimpleKVBC python tests pass with both MemoryDB and RocksDB
  //  backends.
  //
  //  auto comparator = concord::storage::rocksdb::KeyComparator(key_manipulator);
  //  std::stringstream dbPath;
  //  dbPath << BasicRandomTests::DB_FILE_PREFIX << rp.replicaId;
  //  auto db = new concord::storage::rocksdb::Client(dbPath.str(), &comparator);

  auto* dbAdapter = new concord::storage::blockchain::DBAdapter(db);

  ReplicaImp* replica = new ReplicaImp(comm, replicaConfig, dbAdapter, server.GetAggregator());

  InternalCommandsHandler cmdHandler(replica, replica, logger);
  replica->set_command_handler(&cmdHandler);

  replica->start();
  while (replica->isRunning()) std::this_thread::sleep_for(std::chrono::seconds(1));
}
