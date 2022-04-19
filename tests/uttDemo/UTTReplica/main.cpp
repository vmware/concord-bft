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
#include "Replica.h"
#include "UTTCommandsHandler.hpp"
#include "replica_state_sync_imp.hpp"
#include "block_metadata.hpp"
#include "SimpleBCStateTransfer.hpp"
#include "secrets_manager_plain.h"
#include "bftengine/ControlStateManager.hpp"
#include "messages/ReplicaRestartReadyMsg.hpp"
//#include "bftengine/ReconfigurationCmd.hpp"
//#include "client/reconfiguration/cre_interfaces.hpp"
#include "assertUtils.hpp"
#include "Metrics.hpp"
#include <csignal>

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
#endif

#include <memory>
#include <unistd.h>

namespace concord::kvbc::test {
std::shared_ptr<concord::kvbc::Replica> replica;

std::atomic_bool timeToExit = false;

void run_replica(int argc, char** argv) {
  const auto setup = TestSetup::ParseArgs(argc, argv);
  logging::initLogger(setup->getLogPropertiesFile());
  auto logger = setup->GetLogger();
  MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(setup->GetReplicaConfig().replicaId));
  MDC_PUT(MDC_THREAD_KEY, "main");

  replica = std::make_shared<Replica>(setup->GetCommunication(),
                                      setup->GetReplicaConfig(),
                                      setup->GetStorageFactory(),
                                      setup->GetMetricsServer().GetAggregator(),
                                      setup->GetPerformanceManager(),
                                      std::map<std::string, categorization::CATEGORY_TYPE>{
                                          {VERSIONED_KV_CAT_ID, categorization::CATEGORY_TYPE::versioned_kv},
                                          /*{BLOCK_MERKLE_CAT_ID, categorization::CATEGORY_TYPE::block_merkle}*/},
                                      setup->GetSecretManager());
  bftEngine::ControlStateManager::instance().addOnRestartProofCallBack(
      [argv, &setup]() {
        setup->GetCommunication()->stop();
        setup->GetMetricsServer().Stop();
        execv(argv[0], argv);
      },
      static_cast<uint8_t>(ReplicaRestartReadyMsg::Reason::Scale));

  auto* blockMetadata = new BlockMetadata(*replica);
  if (!setup->GetReplicaConfig().isReadOnly) replica->setReplicaStateSync(new ReplicaStateSyncImp(blockMetadata));

  auto cmdHandler =
      std::make_shared<UTTCommandsHandler>(replica.get(), replica.get(), logger, &(*replica->kvBlockchain()));

  replica->set_command_handler(cmdHandler);
  replica->setStateSnapshotValueConverter(categorization::KeyValueBlockchain::kNoopConverter);
  replica->start();

  // Start metrics server after creation of the replica so that we ensure
  // registration of metrics from the replica with the aggregator and don't
  // return empty metrics from the metrics server.
  setup->GetMetricsServer().Start();
  while (replica->isRunning()) {
    if (timeToExit) {
      setup->GetMetricsServer().Stop();
      replica->stop();
    } else {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}
}  // namespace concord::kvbc::test

using namespace std;

namespace {
static void signal_handler(int signal_num) {
  LOG_INFO(GL, "Program received signal " << signal_num);
  concord::kvbc::test::timeToExit = true;
}
}  // namespace
int main(int argc, char** argv) {
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  try {
    concord::kvbc::test::run_replica(argc, argv);
  } catch (const std::exception& e) {
    LOG_FATAL(GL, "exception: " << e.what());
  }
  return 0;
}
