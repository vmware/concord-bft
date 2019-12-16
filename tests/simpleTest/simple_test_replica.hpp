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

#ifndef CONCORD_BFT_SIMPLE_TEST_REPLICA_HPP
#define CONCORD_BFT_SIMPLE_TEST_REPLICA_HPP

#include "CommFactory.hpp"
#include "Replica.hpp"
#include "ReplicaConfig.hpp"
#include "SimpleStateTransfer.hpp"
#include "FileStorage.hpp"
#include <thread>
#include "commonDefs.h"
#include "simple_test_replica_behavior.hpp"

using namespace bftEngine;
using namespace std;

concordlogger::Logger replicaLogger = concordlogger::Log::getLogger("simpletest.replica");

#define test_assert_replica(statement, message)                          \
  {                                                                      \
    if (!(statement)) {                                                  \
      LOG_FATAL(replicaLogger, "assert fail with message: " << message); \
      assert(false);                                                     \
    }                                                                    \
  }

// The replica state machine.
class SimpleAppState : public RequestsHandler {
 private:
  uint64_t client_to_index(NodeNum clientId) { return clientId - numOfReplicas; }

  uint64_t get_last_state_value(NodeNum clientId) {
    auto index = client_to_index(clientId);
    return statePtr[index].lastValue;
  }

  uint64_t get_last_state_num(NodeNum clientId) {
    auto index = client_to_index(clientId);
    return statePtr[index].stateNum;
  }

  void set_last_state_value(NodeNum clientId, uint64_t value) {
    auto index = client_to_index(clientId);
    statePtr[index].lastValue = value;
  }

  void set_last_state_num(NodeNum clientId, uint64_t value) {
    auto index = client_to_index(clientId);
    statePtr[index].stateNum = value;
  }

 public:
  SimpleAppState(uint16_t numCl, uint16_t numRep)
      : statePtr{new SimpleAppState::State[numCl]}, numOfClients{numCl}, numOfReplicas{numRep} {}

  // Handler for the upcall from Concord-BFT.
  int execute(uint16_t clientId,
              uint64_t sequenceNum,
              bool readOnly,
              uint32_t requestSize,
              const char *request,
              uint32_t maxReplySize,
              char *outReply,
              uint32_t &outActualReplySize) override {
    if (readOnly) {
      // Our read-only request includes only a type, no argument.
      test_assert_replica(requestSize == sizeof(uint64_t), "requestSize =! " << sizeof(uint64_t));

      // We only support the READ operation in read-only mode.
      test_assert_replica(*reinterpret_cast<const uint64_t *>(request) == READ_VAL_REQ,
                          "request is NOT " << READ_VAL_REQ);

      // Copy the latest register value to the reply buffer.
      test_assert_replica(maxReplySize >= sizeof(uint64_t), "maxReplySize < " << sizeof(uint64_t));
      uint64_t *pRet = reinterpret_cast<uint64_t *>(outReply);
      auto lastValue = get_last_state_value(clientId);
      *pRet = lastValue;
      outActualReplySize = sizeof(uint64_t);
    } else {
      // Our read-write request includes one eight-byte argument, in addition to
      // the request type.
      test_assert_replica(requestSize == 2 * sizeof(uint64_t), "requestSize != " << 2 * sizeof(uint64_t));

      // We only support the WRITE operation in read-write mode.
      const uint64_t *pReqId = reinterpret_cast<const uint64_t *>(request);
      test_assert_replica(*pReqId == SET_VAL_REQ, "*preqId != " << SET_VAL_REQ);

      // The value to write is the second eight bytes of the request.
      const uint64_t *pReqVal = (pReqId + 1);

      // Modify the register state.
      set_last_state_value(clientId, *pReqVal);
      // Count the number of times we've modified it.
      auto stateNum = get_last_state_num(clientId);
      set_last_state_num(clientId, stateNum + 1);

      // Reply with the number of times we've modified the register.
      test_assert_replica(maxReplySize >= sizeof(uint64_t), "maxReplySize < " << sizeof(uint64_t));
      uint64_t *pRet = reinterpret_cast<uint64_t *>(outReply);
      *pRet = stateNum;
      outActualReplySize = sizeof(uint64_t);

      st->markUpdate(statePtr, sizeof(State) * numOfClients);
    }
    return 0;
  }

  struct State {
    // Number of modifications made.
    uint64_t stateNum = 0;
    // Register value.
    uint64_t lastValue = 0;
  };
  State *statePtr;

  uint16_t numOfClients;
  uint16_t numOfReplicas;

  bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer *st = nullptr;
};

class SimpleTestReplica {
 private:
  ICommunication *comm;
  bftEngine::Replica *replica = nullptr;
  ReplicaConfig replicaConfig;
  std::thread *runnerThread = nullptr;
  ISimpleTestReplicaBehavior *behaviorPtr;

 public:
  SimpleTestReplica(ICommunication *commObject,
                    RequestsHandler &state,
                    ReplicaConfig rc,
                    ISimpleTestReplicaBehavior *behvPtr,
                    bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer *inMemoryST,
                    MetadataStorage *metaDataStorage)
      : comm{commObject}, replicaConfig{rc}, behaviorPtr{behvPtr} {
    replica = Replica::createNewReplica(&rc, &state, inMemoryST, comm, metaDataStorage);
  }

  ~SimpleTestReplica() {
    if (replica) {
      delete replica;
    }
  }

  uint16_t get_replica_id() { return replicaConfig.replicaId; }

  void start() { replica->start(); }

  void stop() {
    replica->stop();
    if (runnerThread) {
      runnerThread->join();
    }
    LOG_INFO(replicaLogger, "replica " << replicaConfig.replicaId << " stopped");
  }

  bool isRunning() { return replica->isRunning(); }

  void run() {
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

  void run_non_blocking() { runnerThread = new std::thread(std::bind(&SimpleTestReplica::run, this)); }

  static SimpleTestReplica *create_replica(ISimpleTestReplicaBehavior *behv,
                                           ReplicaParams rp,
                                           MetadataStorage *metaDataStorage) {
    TestCommConfig testCommConfig(replicaLogger);
    ReplicaConfig replicaConfig;
    testCommConfig.GetReplicaConfig(rp.replicaId, rp.keysFilePrefix, &replicaConfig);
    replicaConfig.numOfClientProxies = rp.numOfClients;
    replicaConfig.viewChangeProtocolEnabled = rp.viewChangeEnabled;
    replicaConfig.viewChangeTimerMillisec = rp.viewChangeTimeout;
    replicaConfig.replicaId = rp.replicaId;
    replicaConfig.statusReportTimerMillisec = 10000;
    replicaConfig.concurrencyLevel = 1;
    replicaConfig.debugPersistentStorageEnabled =
        rp.persistencyMode == PersistencyMode::InMemory || rp.persistencyMode == PersistencyMode::File;

    replicaConfig.singletonFromThis();

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
    auto comm = bftEngine::CommFactory::create(conf);

    bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer *st =
        bftEngine::SimpleInMemoryStateTransfer::create(simpleAppState->statePtr,
                                                       sizeof(SimpleAppState::State) * rp.numOfClients,
                                                       replicaConfig.replicaId,
                                                       replicaConfig.fVal,
                                                       replicaConfig.cVal,
                                                       true);

    simpleAppState->st = st;
    SimpleTestReplica *replica = new SimpleTestReplica(comm, *simpleAppState, replicaConfig, behv, st, metaDataStorage);
    return replica;
  }
};

#endif  // CONCORD_BFT_SIMPLE_TEST_REPLICA_HPP
