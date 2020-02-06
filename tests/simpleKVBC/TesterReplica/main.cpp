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

#include "setup.hpp"
#include "ReplicaImp.h"
#include "memorydb/client.h"
#include "internalCommandsHandler.hpp"
#include "commonKVBTests.hpp"
#include "replica_state_sync_imp.hpp"
#include "block_metadata.hpp"
#include "SimpleBCStateTransfer.hpp"
#include "ror_test_setup.hpp"
#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
#endif
#include <memory>

using namespace concord::kvbc;
using namespace concord::storage;

int main(int argc, char** argv) {
  auto setup = concord::kvbc::TestSetup::ParseArgs(argc, argv);
  
  auto logger = setup->GetLogger();
  auto* db_key_comparator = new concord::storage::blockchain::DBKeyComparator();
  std::shared_ptr<concord::storage::IDBClient> db;

  if (setup->UsePersistentStorage()) {
#ifdef USE_ROCKSDB
    auto* comparator = new concord::storage::rocksdb::KeyComparator(db_key_comparator);
    std::stringstream dbPath;
    dbPath << BasicRandomTests::DB_FILE_PREFIX << setup->GetReplicaConfig().replicaId;
    db.reset(new concord::storage::rocksdb::Client(dbPath.str(), comparator));
#else
    // Abort if we haven't built rocksdb storage
    LOG_ERROR(
        logger,
        "Must build with -DBUILD_ROCKSDB_STORAGE=TRUE cmake option in order to test with persistent storage enabled");
    exit(-1);
#endif
  } else {
    // Use in-memory storage
    auto comparator = concord::storage::memorydb::KeyComparator(db_key_comparator);
    db.reset(new concord::storage::memorydb::Client(comparator));
  }

  auto* dbAdapter = new concord::storage::blockchain::DBAdapter(db);
  std::shared_ptr<ReplicaImp> replica = nullptr;

  // auto rorMode = setup->GetRoRMode();
  auto rorMode = setup->GetReplicaConfig().replicaId == 4 ? RoRAppStateMode::S3 : RoRAppStateMode::None;
  replica = std::make_shared<ReplicaImp>(
        setup->GetCommunication(), setup->GetReplicaConfig(), dbAdapter, setup->GetMetricsServer().GetAggregator());
  if(rorMode == RoRAppStateMode::Default) {
    replica->setReplicaStateSync(new ReplicaStateSyncImp(new BlockMetadata(*replica)));
    replica->set_app_state(nullptr); //set BlockChainAppState by default
  } else {
    using namespace bftEngine::SimpleBlockchainStateTransfer;
    std::shared_ptr<IDBClient> local_client = set_local_client();
    local_client->init();
    std::shared_ptr<IDBClient> os_client = set_object_store_client();
    os_client->init();
    std::shared_ptr<IAppState> appState = 
    std::make_shared<concord::consensus::RoRAppState>(
      std::make_shared<concord::storage::blockchain::DBKeyManipulator>(), local_client, os_client);
    replica->set_app_state(appState);
  }

  // Start metrics server after creation of the replica so that we ensure
  // registration of metrics from the replica with the aggregator and don't
  // return empty metrics from the metrics server.
  setup->GetMetricsServer().Start();

  InternalCommandsHandler cmdHandler(replica, replica, logger);
  replica->set_command_handler(&cmdHandler);
  replica->start();

  while (replica->isRunning()) std::this_thread::sleep_for(std::chrono::seconds(1));
}
