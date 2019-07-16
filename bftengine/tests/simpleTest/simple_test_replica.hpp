//
// Created by Igor Golikov on 2019-07-01.
//

#ifndef CONCORD_BFT_SIMPLE_TEST_REPLICA_HPP
#define CONCORD_BFT_SIMPLE_TEST_REPLICA_HPP

#include "commonDefs.h"
#include "CommFactory.hpp"
#include "Replica.hpp"
#include "ReplicaConfig.hpp"
#include "SimpleStateTransfer.hpp"
#include <thread>

using namespace bftEngine;
using namespace std;

#define test_assert_replica(statement, message) \
{ if (!(statement)) { \
LOG_FATAL(logger, "assert fail with message: " << message); assert(false);}}

concordlogger::Logger replicaLogger =
    concordlogger::Log::getLogger("simpletest.replica");

// The replica state machine.
class SimpleAppState : public RequestsHandler {
 private:
  uint64_t client_to_index(NodeNum clientId) {
    return clientId - numOfReplicas;
  }

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

  SimpleAppState(uint16_t numCl, uint16_t numRep) :
      statePtr{new SimpleAppState::State[numCl]},
      numOfClients{numCl},
      numOfReplicas{numRep} {}

  // Handler for the upcall from Concord-BFT.
  int execute(uint16_t clientId,
              uint64_t sequenceNum,
              bool readOnly,
              uint32_t requestSize,
              const char* request,
              uint32_t maxReplySize,
              char* outReply,
              uint32_t& outActualReplySize) override {
    if (readOnly) {
      // Our read-only request includes only a type, no argument.
      test_assert_replica(requestSize == sizeof(uint64_t),
                  "requestSize =! " << sizeof(uint64_t));

      // We only support the READ operation in read-only mode.
      test_assert_replica(*reinterpret_cast<const uint64_t*>(request) == READ_VAL_REQ,
                  "request is NOT " << READ_VAL_REQ);

      // Copy the latest register value to the reply buffer.
      test_assert_replica(maxReplySize >= sizeof(uint64_t),
                  "maxReplySize < " << sizeof(uint64_t));
      uint64_t* pRet = reinterpret_cast<uint64_t*>(outReply);
      auto lastValue = get_last_state_value(clientId);
      *pRet = lastValue;
      outActualReplySize = sizeof(uint64_t);
    } else {
      // Our read-write request includes one eight-byte argument, in addition to
      // the request type.
      test_assert_replica(requestSize == 2 * sizeof(uint64_t),
                  "requestSize != " << 2 * sizeof(uint64_t));

      // We only support the WRITE operation in read-write mode.
      const uint64_t* pReqId = reinterpret_cast<const uint64_t*>(request);
      test_assert_replica(*pReqId == SET_VAL_REQ, "*preqId != " << SET_VAL_REQ);

      // The value to write is the second eight bytes of the request.
      const uint64_t* pReqVal = (pReqId + 1);

      // Modify the register state.
      set_last_state_value(clientId, *pReqVal);
      // Count the number of times we've modified it.
      auto stateNum = get_last_state_num(clientId);
      set_last_state_num(clientId, stateNum + 1);

      // Reply with the number of times we've modified the register.
      test_assert_replica(maxReplySize >= sizeof(uint64_t),
                  "maxReplySize < " << sizeof(uint64_t));
      uint64_t* pRet = reinterpret_cast<uint64_t*>(outReply);
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

  concordlogger::Logger logger = concordlogger::Log::getLogger
      ("simpletest.replica");

  bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer* st = nullptr;
};

struct PersistencyTestInfo {
  bool replica2RestartNoVC = false;
  bool allReplicasRestartNoVC = false;
  bool allReplicasRestartVC = false;
  bool primaryReplicaRestartVC = false;
  uint32_t restartDelay = 0;
  uint32_t initialSleepBetweenRestartsMillis = 5000;
  double sleepBetweenRestartsMultipler = 1.2;
};

class SimpleTestReplica {
 private:
  ICommunication *comm;
  bftEngine::Replica* replica = nullptr;
  ReplicaConfig replicaConfig;
  std::thread *runnerThread = nullptr;

 public:
  PersistencyTestInfo pti;

  SimpleTestReplica(
      ICommunication *commObject,
      RequestsHandler &state,
      ReplicaConfig rc,
      const PersistencyTestInfo &persistencyTestInfo,
      bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer *inMemoryST) :
      comm{commObject}, replicaConfig{rc}, pti{persistencyTestInfo} {
    replica = Replica::createNewReplica(&rc,  &state, inMemoryST, comm, nullptr);
  }

  ~SimpleTestReplica() {
    if(replica) {
      replica->stop();
    }
    if(runnerThread) {
      runnerThread->join();
    }
    if(replica) {
      delete replica;
    }

  }

  uint16_t get_replica_id(){
    return replicaConfig.replicaId;
  }

  void start() {
    replica->start();
  }

  void stop() {
    replica->stop();
    LOG_INFO(replicaLogger, "replica " << replicaConfig.replicaId << " stopped");
  }

  bool isRunning() {
    return replica->isRunning();
  }

  void run() {
    srand (time(NULL));
    while (replica->isRunning()) {
      if(pti.replica2RestartNoVC) {
        if (replicaConfig.replicaId == 2) {
          std::this_thread::sleep_for(std::chrono::seconds(10));
          if(replica && replica->isRunning()) {
            LOG_INFO(replicaLogger, "Restarting");
            replica->restartForDebug(pti.restartDelay);
            LOG_INFO(replicaLogger, "Restarted");
          }
        }
      }
      else if (pti.allReplicasRestartNoVC) {
        std::this_thread::sleep_for(std::chrono::milliseconds(
          pti.initialSleepBetweenRestartsMillis
        ));
        pti.initialSleepBetweenRestartsMillis = pti.initialSleepBetweenRestartsMillis *
          pti.sleepBetweenRestartsMultipler + (1000 * replicaConfig.replicaId * replicaConfig.replicaId);
        if(replica && replica->isRunning()) {
            LOG_INFO(replicaLogger, "Restarting");
            pti.restartDelay = (rand() % 40 + 2) * 1000;
            replica->restartForDebug(pti.restartDelay);
            LOG_INFO(replicaLogger, "Restarted");
          }
      }
      else if (pti.allReplicasRestartVC) {
        std::this_thread::sleep_for(std::chrono::seconds(18 *
            (2 + replicaConfig.replicaId) * (2 + replicaConfig.replicaId)));
       if(replica && replica->isRunning()) {
            LOG_INFO(replicaLogger, "Restarting");
            replica->restartForDebug(pti.restartDelay);
            LOG_INFO(replicaLogger, "Restarted");
          }
      }
      else if(pti.primaryReplicaRestartVC) {
        if(replicaConfig.replicaId == 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds((uint32_t)
            (pti.initialSleepBetweenRestartsMillis * pti.sleepBetweenRestartsMultipler)
          ));
          pti.sleepBetweenRestartsMultipler *= 1.2;
          if(replica && replica->isRunning()) {
            LOG_INFO(replicaLogger, "Restarting");
            replica->restartForDebug(pti.restartDelay);
            LOG_INFO(replicaLogger, "Restarted");
          }
        }
      }
      else {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }
  }

  void run_non_blocking() {
    runnerThread = new std::thread(std::bind(&SimpleTestReplica::run, this));
  }

  static SimpleTestReplica* create_replica(
  PersistencyTestInfo pti, ReplicaParams rp) {
      TestCommConfig testCommConfig(replicaLogger);
      ReplicaConfig replicaConfig;
      testCommConfig.GetReplicaConfig(
          rp.replicaId, rp.keysFilePrefix, &replicaConfig);
      replicaConfig.numOfClientProxies = rp.numOfClients;
      replicaConfig.autoViewChangeEnabled = rp.viewChangeEnabled;
      replicaConfig.viewChangeTimerMillisec = rp.viewChangeTimeout;
      replicaConfig.replicaId = rp.replicaId;
      replicaConfig.statusReportTimerMillisec = 10000;
      replicaConfig.concurrencyLevel = 1;

      // This is the state machine that the replica will drive.
      SimpleAppState *simpleAppState = new SimpleAppState(rp.numOfClients, rp
          .numOfReplicas);

#ifdef USE_COMM_PLAIN_TCP
      PlainTcpConfig conf = testCommConfig.GetTCPConfig(true, rp.replicaId,
                                                    rp.numOfClients,
                                                    rp.numOfReplicas,
                                                    rp.configFileName);
#elif USE_COMM_TLS_TCP
      TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(true, rp.replicaId,
                                                         rp.numOfClients,
                                                         rp.numOfReplicas,
                                                         rp.configFileName);
#else
      PlainUdpConfig conf = testCommConfig.GetUDPConfig(true, rp.replicaId,
                                                      rp.numOfClients,
                                                      rp.numOfReplicas,
                                                      rp.configFileName);
#endif
      auto comm = bftEngine::CommFactory::create(conf);

      bftEngine::SimpleInMemoryStateTransfer::ISimpleInMemoryStateTransfer *st =
          bftEngine::SimpleInMemoryStateTransfer::create(
              simpleAppState->statePtr,
              sizeof(SimpleAppState::State) * rp.numOfClients,
              replicaConfig.replicaId,
              replicaConfig.fVal,
              replicaConfig.cVal, true);

      simpleAppState->st = st;
      SimpleTestReplica *replica = new SimpleTestReplica(comm, *simpleAppState,
                                                         replicaConfig, pti, st);
      return replica;
}

};

#endif //CONCORD_BFT_SIMPLE_TEST_REPLICA_HPP
