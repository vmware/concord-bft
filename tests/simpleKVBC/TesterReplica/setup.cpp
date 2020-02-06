// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <thread>
#include <sys/param.h>
#include <string>
#include <cstring>
#include <unistd.h>
#include <tuple>
#include <chrono>

#include "setup.hpp"
#include "CommFactory.hpp"
#include "config/test_comm_config.hpp"
#include "commonKVBTests.hpp"
#include "db_adapter.h"

#include "memorydb/client.h"

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
#endif

#ifdef USE_S3_OBJECT_STORE
#include "object_store/object_store_client.hpp"
#include "s3/client.hpp"
using concord::storage::ObjectStoreClient;
#endif

using namespace concord::storage;

namespace concord::kvbc {

std::unique_ptr<TestSetup> TestSetup::ParseArgs(int argc, char** argv) {
  ReplicaParams rp;
  rp.replicaId = UINT16_MAX;
  rp.viewChangeEnabled = false;
  rp.viewChangeTimeout = 45 * 1000;

  // allows to attach debugger
  if (rp.debug) {
    std::this_thread::sleep_for(std::chrono::seconds(20));
  }

  char argTempBuffer[PATH_MAX + 10];
  std::string idStr;

  int o = 0;
  while ((o = getopt(argc, argv, "r:i:k:n:s:v:a:po:")) != EOF) {
    switch (o) {
      case 'i': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && tempId < UINT16_MAX) rp.replicaId = (uint16_t)tempId;
      } break;

      case 'k': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        rp.keysFilePrefix = argTempBuffer;
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
        if (tempId >= 0 && (uint16_t)tempId < UINT16_MAX) {
          rp.viewChangeTimeout = (uint16_t)tempId;
          rp.viewChangeEnabled = true;
        }
      } break;
      case 'a': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && (uint16_t)tempId < UINT16_MAX) {
          rp.autoPrimaryRotationTimeout = (uint16_t)tempId;
          rp.autoPrimaryRotationEnabled = true;
        }
      } break;
      // We can only toggle persistence on or off. It defaults to InMemory
      // unless -p flag is provided.
      case 'p': {
        rp.persistencyMode = PersistencyMode::RocksDB;

      } break;

      default:
        break;
    }
  }

  if (rp.replicaId == UINT16_MAX || rp.keysFilePrefix.empty()) {
    fprintf(stderr, "%s -k KEYS_FILE_PREFIX -i ID -n COMM_CONFIG_FILE", argv[0]);
    exit(-1);
  }

  // used to get info from parsing the key file
  bftEngine::ReplicaConfig replicaConfig;
  concordlogger::Logger logger = concordlogger::Log::getLogger("skvbctest.replica");

  TestCommConfig testCommConfig(logger);
  testCommConfig.GetReplicaConfig(rp.replicaId, rp.keysFilePrefix, &replicaConfig);

  // This allows more concurrency and only affects known ids in the
  // communication classes.
  replicaConfig.numOfClientProxies = 100;
  replicaConfig.viewChangeProtocolEnabled = rp.viewChangeEnabled;
  replicaConfig.viewChangeTimerMillisec = rp.viewChangeTimeout;
  replicaConfig.autoPrimaryRotationEnabled = rp.autoPrimaryRotationEnabled;
  replicaConfig.autoPrimaryRotationTimerMillisec = rp.autoPrimaryRotationTimeout;
  replicaConfig.statusReportTimerMillisec = rp.statusReportTimerMillisec;
  replicaConfig.concurrencyLevel = 1;
  replicaConfig.debugStatisticsEnabled = true;

  uint16_t numOfReplicas = (uint16_t)(3 * replicaConfig.fVal + 2 * replicaConfig.cVal + 1);
#ifdef USE_COMM_PLAIN_TCP
  bftEngine::PlainTcpConfig conf = testCommConfig.GetTCPConfig(
      true, rp.replicaId, replicaConfig.numOfClientProxies, numOfReplicas, rp.configFileName);
#elif USE_COMM_TLS_TCP
  bftEngine::TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(
      true, rp.replicaId, replicaConfig.numOfClientProxies, numOfReplicas, rp.configFileName);
#else
  bftEngine::PlainUdpConfig conf = testCommConfig.GetUDPConfig(
      true, rp.replicaId, replicaConfig.numOfClientProxies, numOfReplicas, rp.configFileName);
#endif

  std::unique_ptr<bftEngine::ICommunication> comm(bftEngine::CommFactory::create(conf));

  uint16_t metrics_port = conf.listenPort + 1000;

  return std::unique_ptr<TestSetup>(new TestSetup{
      replicaConfig, std::move(comm), logger, metrics_port, rp.persistencyMode == PersistencyMode::RocksDB});
}

std::tuple<std::shared_ptr<concord::storage::IDBClient>, IDbAdapter*> TestSetup::get_inmem_db_configuration() {
  auto comparator = concord::storage::memorydb::KeyComparator(new DBKeyComparator());
  std::shared_ptr<concord::storage::IDBClient> db = std::make_shared<concord::storage::memorydb::Client>(comparator);
  return std::make_tuple(db, new DBAdapter{db});
}
/** Get replica db configuration
 * @return storage::IDBClient instance for metadata storage
 * @return kvbc::IDBAdapter instance
 */
std::tuple<std::shared_ptr<concord::storage::IDBClient>, IDbAdapter*> TestSetup::get_db_configuration() {
#ifndef USE_ROCKSDB
  return get_inmem_db_configuration();
#else
  if (!UsePersistentStorage()) get_inmem_db_configuration();

  auto* comparator = new concord::storage::rocksdb::KeyComparator(new DBKeyComparator());
  std::stringstream dbPath;
  dbPath << BasicRandomTests::DB_FILE_PREFIX << GetReplicaConfig().replicaId;
  std::shared_ptr<concord::storage::IDBClient> db(new concord::storage::rocksdb::Client(dbPath.str(), comparator));
  db->init();
  IDbAdapter* dbAdapter = nullptr;

#ifdef USE_S3_OBJECT_STORE  // TODO [TK] config
  if (GetReplicaConfig().isReadOnly) {
    concord::storage::s3::StoreConfig config;
    config.bucketName = "blockchain-dev-asx";
    config.accessKey = "blockchain-dev";
    config.protocol = "HTTP";
    config.url = "10.70.30.244:9020";
    config.secretKey = "Rz0mdbUNGJBxqdzprn5XGSXPr2AfkgcQsYS4y698";
    std::string prefix = std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count());

    std::shared_ptr<concord::storage::IDBClient> s3client(new ObjectStoreClient(new s3::Client(config)));
    s3client->init();
    dbAdapter = new DBAdapter(s3client, std::unique_ptr<IDataKeyGenerator>(new S3KeyGenerator(prefix)), true);
  } else {
    dbAdapter = new DBAdapter(db);
  }
#else
  dbAdapter = new DBAdapter(db);
#endif

  return std::make_tuple(db, dbAdapter);

#endif
}

}  // namespace concord::kvbc
