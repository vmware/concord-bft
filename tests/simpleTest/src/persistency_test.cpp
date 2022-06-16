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

// These tests are used as basic regression tests for meta data persistency

#include "assertUtils.hpp"
#include "gtest/gtest.h"
#include <vector>
#include "Logger.hpp"
#include <thread>
#include <memory>
#include "simple_test_client.hpp"
#include "simple_test_replica.hpp"
#include "diagnostics.h"

namespace test::persistency {
class PersistencyTest : public testing::Test {
 protected:
  void TearDown() override {
    for (const auto &it : replicas) {
      it->stop();
    }
    for (const auto &it : replicaThreads) {
      if (it->joinable()) {
        it->join();
      }
    }
    replicas.clear();
  }

  void run_replica(shared_ptr<SimpleTestReplica> rep) {
    rep->start();
    rep->run();
  }

  void create_client(int numOfOperations) {
    ClientParams cp;
    cp.numOfOperations = numOfOperations;
    client = std::make_unique<SimpleTestClient>(SimpleTestClient(cp, clientLogger));
  }

  void create_and_run_replica(ReplicaParams rp, ISimpleTestReplicaBehavior *behv) {
    concord::diagnostics::RegistrarSingleton::getInstance().status.clear();
    rp.keysFilePrefix = "private_replica_";
    auto replica = std::shared_ptr<SimpleTestReplica>(SimpleTestReplica::create_replica(behv, rp, nullptr));
    replicas.push_back(replica);
    auto t = std::make_shared<std::thread>(std::thread(std::bind(&PersistencyTest::run_replica, this, replica)));
    replicaThreads.push_back(t);
  }

  void regressionNoPersistency(uint16_t numOfReq) {
    create_client(numOfReq);
    for (int i = 0; i < 4; i++) {
      ReplicaParams rp;
      rp.persistencyMode = PersistencyMode::Off;
      rp.replicaId = i;
      auto b = create_replica_behavior(ReplicaBehavior::Default, rp);
      create_and_run_replica(rp, b);
    }
    ASSERT_TRUE(client->run());
  }

  std::unique_ptr<SimpleTestClient> client;
  vector<std::shared_ptr<SimpleTestReplica>> replicas;
  vector<std::shared_ptr<std::thread>> replicaThreads;
  logging::Logger clientLogger = logging::getLogger("clientlogger");
  uint16_t numOfRequests = 1500;
  uint8_t numOfRestarts = 8;
  uint16_t numOfRequestsInCycle = 50;
};

TEST_F(PersistencyTest, RegressionNoPersistency) { regressionNoPersistency(numOfRequests); }

// This test make take a while to complete...
TEST_F(PersistencyTest, PrimaryRestartVC) {
  create_client(numOfRequests);
  for (int i = 0; i < 4; i++) {
    ReplicaParams rp;
    rp.persistencyMode = PersistencyMode::InMemory;
    rp.viewChangeEnabled = true;
    rp.replicaId = i;
    ISimpleTestReplicaBehavior *b = new OneTimePrimaryDownVC(rp);
    create_and_run_replica(rp, b);
  }
  ASSERT_TRUE(client->run());
}

// This test make take a while to complete...
TEST_F(PersistencyTest, RegressionNoPersistencyInCycle) {
  for (auto k = 0; k < numOfRestarts; k++) {
    regressionNoPersistency(numOfRequestsInCycle);
    TearDown();
  }
}

GTEST_API_ int main(int argc, char **argv) {
  printf("Running main() from gtest_main.cc\n");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace test::persistency
