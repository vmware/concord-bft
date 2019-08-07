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
#include <time.h>

namespace test
{
namespace persistency
{
class PersistencyTest: public testing::Test
{
 protected:
  PersistencyTest() {
  }

  ~PersistencyTest() {
    for(auto replica : replicas) {
        replica->stop();
    }

    for(auto t : replicaThreads) {
      t->join();
    }

    replicas.clear();
    delete client;
  }

  void run_replica(SimpleTestReplica *rep) {
    rep->start();
    rep->run();
  }

  void create_client(int numOfOperations) {
    ClientParams cp;
    cp.numOfOperations = numOfOperations;
    client = new SimpleTestClient(cp, clientLogger);
  }

  void create_and_run_replica(
    ReplicaParams rp, ISimpleTestReplicaBehavior *behv) {
      rp.keysFilePrefix = "private_replica_";
      SimpleTestReplica *replica = SimpleTestReplica::create_replica(
        behv, rp, nullptr);
      replicas.push_back(replica);
      std::thread *t = new std::thread(
        std::bind(&PersistencyTest::run_replica, this ,replica));
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

TEST_F(PersistencyTest, RegressionNoPersistency) {
  create_client(2800);
  for(int i = 0; i < 4;i++) {
    ReplicaParams rp;
    rp.persistencyMode = PersistencyMode::Off;
    rp.replicaId = i;
    auto b = 
      create_replica_behavior(ReplicaBehavior::Default, rp);
    create_and_run_replica(rp, b);
  }

  ASSERT_TRUE(client->run());
}

// this test make take a while to complete...
TEST_F(PersistencyTest, PrimaryRestartVC) {
  create_client(2000);
  
  for(int i = 0; i < 4;i++) {
    ReplicaParams rp;
    rp.persistencyMode = PersistencyMode::InMemory;
    rp.viewChangeEnabled = true;
    rp.replicaId = i;
    ISimpleTestReplicaBehavior *b = new OneTimePrimaryDownVC(rp);
    create_and_run_replica(rp, b);
  }

  ASSERT_TRUE(client->run());
}

}
}
