// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "simple_test_replica.hpp"
#include "test_comm_config.hpp"

// NOLINTNEXTLINE(misc-definitions-in-headers)

SimpleTestReplica::SimpleTestReplica(ICommunication *commObject,
                                     IRequestsHandler *state,
                                     const ReplicaConfig &rc,
                                     ISimpleTestReplicaBehavior *behvPtr,
                                     bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer *inMemoryST,
                                     MetadataStorage *metaDataStorage)
    : comm{commObject}, replicaConfig{rc}, behaviorPtr{behvPtr}, statePtr(state) {
  bftEngine::IControlHandler::instance(new bftEngine::ControlHandler());
  replica = IReplica::createNewReplica(rc,
                                       std::shared_ptr<bftEngine::IRequestsHandler>(state),
                                       inMemoryST,
                                       comm,
                                       metaDataStorage,
                                       std::make_shared<concord::performance::PerformanceManager>(),
                                       nullptr /*SecretsManagerEnc*/,
                                       [](bool) {});  // call back
  replica->SetAggregator(std::make_shared<concordMetrics::Aggregator>());
}

SimpleTestReplica::~SimpleTestReplica() {
  // TODO(DD): Reset manually because apparently the order matters - fixit
  replica.reset();
  if (comm) {
    comm->stop();
    delete comm;
  }
  if (behaviorPtr) {
    delete behaviorPtr;
  }
  if (statePtr) {
    delete statePtr;
  }
}

uint16_t SimpleTestReplica::get_replica_id() { return replicaConfig.replicaId; }

void SimpleTestReplica::start() { replica->start(); }

void SimpleTestReplica::stop() {
  replica->stop();
  if (runnerThread) {
    runnerThread->join();
  }
  LOG_INFO(replicaLogger, "replica " << replicaConfig.replicaId << " stopped");
}

bool SimpleTestReplica::isRunning() { return replica->isRunning(); }

void SimpleTestReplica::run() {
  if (replica->isRunning() && behaviorPtr->to_be_restarted()) {
    uint32_t initialSleepBetweenRestartsMillis = behaviorPtr->get_initial_sleep_between_restarts_ms();
    LOG_INFO(replicaLogger, "Restarting replica in " << initialSleepBetweenRestartsMillis << " ms");
    std::this_thread::sleep_for(std::chrono::milliseconds(initialSleepBetweenRestartsMillis));
  }
  while (replica->isRunning()) {
    bool toBeRestarted = behaviorPtr->to_be_restarted();
    if (toBeRestarted) {
      if (replica && replica->isRunning()) {
        uint32_t downTime = behaviorPtr->get_down_time_millis();
        LOG_INFO(replicaLogger, "Restarting replica");
        replica->restartForDebug(downTime);
        behaviorPtr->on_restarted();
        LOG_INFO(replicaLogger, "Replica restarted");
      }
    } else {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

void SimpleTestReplica::run_non_blocking() { runnerThread = new std::thread(std::bind(&SimpleTestReplica::run, this)); }

SimpleTestReplica *SimpleTestReplica::create_replica(ISimpleTestReplicaBehavior *behv,
                                                     ReplicaParams rp,
                                                     MetadataStorage *metaDataStorage) {
  TestCommConfig testCommConfig(replicaLogger);
  ReplicaConfig &replicaConfig = ReplicaConfig::instance();
  testCommConfig.GetReplicaConfig(rp.replicaId, rp.keysFilePrefix, &replicaConfig);
  replicaConfig.numOfClientProxies = rp.numOfClients;
  replicaConfig.viewChangeProtocolEnabled = rp.viewChangeEnabled;
  replicaConfig.viewChangeTimerMillisec = rp.viewChangeTimeout;
  replicaConfig.replicaId = rp.replicaId;
  replicaConfig.statusReportTimerMillisec = 10000;
  replicaConfig.keyExchangeOnStart = false;
  replicaConfig.set("concord.bft.keyExchage.clientKeysEnabled", false);
  replicaConfig.concurrencyLevel = 1;
  replicaConfig.debugPersistentStorageEnabled =
      rp.persistencyMode == PersistencyMode::InMemory || rp.persistencyMode == PersistencyMode::File;

  // This is the state machine that the replica will drive.
  SimpleAppState *simpleAppState = new SimpleAppState(rp.numOfClients, rp.numOfReplicas);

#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf =
      testCommConfig.GetTCPConfig(true, rp.replicaId, rp.numOfClients, rp.numOfReplicas, rp.configFileName);
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf =
      testCommConfig.GetTlsTCPConfig(true, rp.replicaId, rp.numOfClients, rp.numOfReplicas, rp.configFileName);
#else
  PlainUdpConfig conf =
      testCommConfig.GetUDPConfig(true, rp.replicaId, rp.numOfClients, rp.numOfReplicas, rp.configFileName);
#endif
  auto comm = bft::communication::CommFactory::create(conf);

  bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer *st =
      bftEngine::SimpleInMemoryStateTransfer::create(simpleAppState->statePtr,
                                                     sizeof(SimpleAppState::State) * rp.numOfClients,
                                                     replicaConfig.replicaId,
                                                     replicaConfig.fVal,
                                                     replicaConfig.cVal,
                                                     true);

  simpleAppState->st = st;
  SimpleTestReplica *replica = new SimpleTestReplica(comm, simpleAppState, replicaConfig, behv, st, metaDataStorage);
  return replica;
}
