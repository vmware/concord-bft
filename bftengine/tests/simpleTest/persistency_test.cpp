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
#include <cstdlib>
#include <time.h>

namespace test
{
namespace persistency
{
class PersistencyTest: public testing::Test
{
 protected:
  PersistencyTest() {
    //std::this_thread::sleep_for(std::chrono::seconds(20));
  }

  ~PersistencyTest() {
    for(auto replica : replicas) {
      replica->stop();
    }

    for(auto t : replicaThreads) {
      t->join();
    }
  }

  void run_replica(SimpleTestReplica *rep) {
    rep->start();
    rep->run();
  }

  void create_client(int numOfOperations) {
    ClientParams cp;
    cp.numOfOperations = numOfOperations;
    bftEngine::SimpleClientParams scp;
    client = new SimpleTestClient(cp, clientLogger);
  }

  void create_and_run_replica(ReplicaParams rp, PersistencyTestInfo pti) {
    rp.keysFilePrefix = "private_replica_";
    SimpleTestReplica *replica = SimpleTestReplica::create_replica(pti, rp);
    replicas.push_back(replica);
    std::thread *t = new std::thread(std::bind(&PersistencyTest::run_replica, this ,replica));
    replicaThreads.push_back(t);
  }

  SimpleTestClient *client;
  vector<SimpleTestReplica*> replicas;
  vector<std::thread*> replicaThreads;
  concordlogger::Logger clientLogger = concordlogger::Log::getLogger
      ("clientlogger");
  concordlogger::Logger replicaLogger = concordlogger::Log::getLogger
      ("replicalogger");
};

/*
TEST_F(PersistencyTest, RegressionNoPersistency) {
  create_client(2800);
  for(int i = 0; i < 4;i++) {
    PersistencyTestInfo pti;
    ReplicaParams rp;
    rp.replicaId = i;
    create_and_run_replica(rp, pti);
  }

  ASSERT_TRUE(client->run());
}
*/

/*
TEST_F(PersistencyTest, Replica2RestartNoVC) {
  create_client(5000);
  for(int i = 0; i < 4;i++) {
    PersistencyTestInfo pti;
    pti.replica2RestartNoVC = true;
    ReplicaParams rp;
    rp.replicaId = i;
    create_and_run_replica(rp, pti);
  }

  ASSERT_TRUE(client->run());
}
*/

TEST_F(PersistencyTest, AllReplicasRestartNoVC) {
  create_client(20000);
  srand (time(NULL));
  
  for(int i = 0; i < 4;i++) {
    auto delay = rand() % 8;
    PersistencyTestInfo pti;
    pti.allReplicasRestartNoVC = true;
    pti.restartDelay = delay * 1000;
    ReplicaParams rp;
    rp.replicaId = i;
    create_and_run_replica(rp, pti);
  }

  ASSERT_TRUE(client->run());
}

TEST_F(PersistencyTest, PrimaryRestartVC) {
  create_client(20000);
  srand (time(NULL));
  
  for(int i = 0; i < 4;i++) {
    auto delay = rand() % 20 + 60;
    PersistencyTestInfo pti;
    pti.restartDelay = delay * 1000;
    pti.primaryReplicaRestartVC = true;
    ReplicaParams rp;
    rp.viewChangeEnabled = true;
    rp.replicaId = i;
    create_and_run_replica(rp, pti);
  }

  ASSERT_TRUE(client->run());
}

}
}
