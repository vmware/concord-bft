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
  ClientParams cp;
  bftEngine::SimpleClientParams scp;
  client = new SimpleTestClient(cp, clientLogger);

  for(int i = 0; i < 4;i++) {
    PersistencyTestInfo pti;
    ReplicaParams rp;
    rp.keysFilePrefix = "private_replica_";
    rp.replicaId = i;
    SimpleTestReplica *replica = SimpleTestReplica::create_replica(pti, rp);
    replicas.push_back(replica);
    std::thread *t = new std::thread(run_replica, replica);
    replicaThreads.push_back(t);
  }

  ASSERT_TRUE(client->run());
}

TEST_F(PersistencyTest, Replica2RestartNoVC) {
  ClientParams cp;
  cp.numOfOperations = 5000;
  bftEngine::SimpleClientParams scp;
  client = new SimpleTestClient(cp, clientLogger);
  for(int i = 0; i < 4;i++) {
    PersistencyTestInfo pti;
    pti.replica2RestartNoVC = true;
    ReplicaParams rp;
    rp.keysFilePrefix = "private_replica_";
    rp.replicaId = i;
    SimpleTestReplica *replica = SimpleTestReplica::create_replica(pti, rp);
    replicas.push_back(replica);
    std::thread *t = new std::thread(run_replica, replica);
    replicaThreads.push_back(t);
  }

  ASSERT_TRUE(client->run());
}

/*
TEST_F(PersistencyTest, Replica2RestartVC) {
  for(int i = 0; i < 4;i++) {
    PersistencyTestInfo pti;
    pti.replica2RestartVC = true;
    ReplicaParams rp;
    rp.viewChangeEnabled = true;
    rp.viewChangeTimeout = 10000;
    rp.keysFilePrefix = "private_replica_";
    rp.replicaId = i;
    SimpleTestReplica *replica = SimpleTestReplica::create_replica(pti, rp);
    replicas.push_back(replica);
    std::thread *t = new std::thread(run_replica, replica);
    replicaThreads.push_back(t);
  }

  ASSERT_TRUE(client->run());
}
*/

}
}
