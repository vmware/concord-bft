//
// Created by Igor Golikov on 2019-07-01.
//

#include "gtest/gtest.h"
#include <vector>
#include "simple_test_client.hpp"
#include "simple_test_replica.hpp"
#include "Logging.hpp"
#include <thread>
#include <chrono>

namespace test
{
namespace persistency
{
class PersistencyTest: public testing::Test
{
 protected:
  PersistencyTest() {
    //std::this_thread::sleep_for(std::chrono::seconds(20));
    ClientParams cp;
    cp.numOfOperations = 1000;
    bftEngine::SimpleClientParams scp;
    client = new SimpleTestClient(cp, clientLogger);

    for(int i = 0; i < 4; i++) {
      TestCommConfig testCommConfig(replicaLogger);
      ReplicaParams rp;
      rp.replicaId = i;
      rp.keysFilePrefix = "private_replica_";
      ReplicaConfig replicaConfig;
      testCommConfig.GetReplicaConfig(
          rp.replicaId, rp.keysFilePrefix, &replicaConfig);
      replicaConfig.numOfClientProxies = rp.numOfClients;
      replicaConfig.autoViewChangeEnabled = rp.viewChangeEnabled;
      replicaConfig.viewChangeTimerMillisec = rp.viewChangeTimeout;

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
      PersistencyTestInfo pti;
      SimpleTestReplica *replica = new SimpleTestReplica(comm, *simpleAppState,
                                                         &replicaConfig, pti, st);
      replicas.push_back(replica);
    }
  }

  ~PersistencyTest() {
    for(auto replica : replicas) {
      replica->stop();
    }

    for(auto t : replicaThreads) {
      t->join();
    }
  }

  SimpleTestClient *client;
  vector<SimpleTestReplica*> replicas;
  vector<std::thread*> replicaThreads;
  concordlogger::Logger clientLogger = concordlogger::Log::getLogger
      ("clientlogger");
  concordlogger::Logger replicaLogger = concordlogger::Log::getLogger
      ("replicalogger");
};

void run_replica(SimpleTestReplica *rep) {
  rep->start();
  rep->run();
}

TEST_F(PersistencyTest, RegressionNoPersistency) {
  for(auto replica : replicas) {
    std::thread *t = new std::thread(run_replica, replica);
    replicaThreads.push_back(t);
  }

  ASSERT_TRUE(client->run());
}

/*
TEST_F(PersistencyTest, Replica2RestartNoVC) {
  for(auto replica : replicas) {
    if(replica->get_replica_id() == 2) {
      replica->pti.replica2RestartNoVC = true;
    }
    std::thread *t = new std::thread(run_replica, replica);
    replicaThreads.push_back(t);
  }

  ASSERT_TRUE(client->run());
}
*/

/*
TEST_F(PersistencyTest, Replica2RestartVC) {
  for(auto replica : replicas) {
    if(replica->get_replica_id() == 2) {
      replica->pti.replica2RestartVC = true;
    }
    std::thread *t = new std::thread(run_replica, replica);
    replicaThreads.push_back(t);
  }

  ASSERT_TRUE(client->run());
}
 */

}
}
