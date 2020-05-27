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
#include "internalCommandsHandler.hpp"
#include "replica_state_sync_imp.hpp"
#include "block_metadata.hpp"
#include "SimpleBCStateTransfer.hpp"
#include "assertUtils.hpp"
#include <csignal>

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
#endif

#include <memory>

namespace concord::kvbc::test {

void run_replica(int argc, char** argv) {
  const auto setup = TestSetup::ParseArgs(argc, argv);
  concordlogger::Log::initLogger("log4cplus.properties");
  auto logger = setup->GetLogger();
  MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(setup->GetReplicaConfig().replicaId));
  MDC_PUT(MDC_THREAD_KEY, "main");

  std::shared_ptr<ReplicaImp> replica = std::make_shared<ReplicaImp>(setup->GetCommunication(),
                                                                     setup->GetReplicaConfig(),
                                                                     setup->GetStorageFactory(),
                                                                     setup->GetMetricsServer().GetAggregator());

  auto* blockMetadata = new BlockMetadata(*replica);

  if (!setup->GetReplicaConfig().isReadOnly) replica->setReplicaStateSync(new ReplicaStateSyncImp(blockMetadata));

  InternalCommandsHandler* cmdHandler =
      new InternalCommandsHandler(replica.get(), replica.get(), blockMetadata, logger);
  replica->set_command_handler(cmdHandler);
  replica->start();

  // Start metrics server after creation of the replica so that we ensure
  // registration of metrics from the replica with the aggregator and don't
  // return empty metrics from the metrics server.
  setup->GetMetricsServer().Start();
  while (replica->isRunning()) std::this_thread::sleep_for(std::chrono::seconds(1));
}
}  // namespace concord::kvbc::test

using namespace std;

void signal_handler(int signal_num) {
  LOG_INFO(GL, "Program received signal " << signal_num);
  exit(0);
}

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
