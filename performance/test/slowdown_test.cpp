// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "gtest/gtest.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <exception>
#include <iterator>
#include <thread>
#include <chrono>
#include <ReplicaImp.h>
#include <communication/ICommunication.hpp>
#include <ReplicaConfig.hpp>
#include "merkle_tree_storage_factory.h"
#include "PerformanceManager.hpp"

namespace {

using namespace concord::performance;
using namespace bft::communication;

class MockComm : public bft::communication::ICommunication {
 public:
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override {
    (void)node;
    return ConnectionStatus::Connected;
  }

  int getMaxMessageSize() override { return 64000; }

  bool isRunning() const override { return true; }

  int sendAsyncMessage(NodeNum destNode, const char* const message, size_t messageLength) override {
    (void)destNode;
    (void)message;
    (void)messageLength;
    return 0;
  }

  void setReceiver(NodeNum receiverNum, IReceiver* receiver) override {
    (void)receiverNum;
    (void)receiver;
  }

  int Start() override { return 0; }

  int Stop() override { return 0; }
};

TEST(slowdown_test, enabled_disabled) {
  PerformanceManager pm;
  EXPECT_FALSE(pm.isEnabled<SlowdownManager>());
  auto sm = std::make_shared<SlowdownConfiguration>();
  PerformanceManager pm1(sm);
#ifdef USE_SLOWDOWN
  EXPECT_TRUE(pm1.isEnabled<SlowdownManager>());
#else
  EXPECT_FALSE(pm1.isEnabled<SlowdownManager>());
#endif
}

TEST(slowdown_test, empty_configuration) {
  PerformanceManager pm;
  SlowDownResult res = pm.Delay<SlowdownPhase::PreProcessorAfterPreexecPrimary>();
  EXPECT_EQ(res.phase, SlowdownPhase::None);
  EXPECT_EQ(res.totalWaitDuration, 0);
  EXPECT_EQ(res.totalSleepDuration, 0);
  EXPECT_EQ(res.totalKeyCount, 0);
  EXPECT_EQ(res.totalValueSize, 0);
}

TEST(slowdown_test, simple_configuration) {
  auto sm = std::make_shared<SlowdownConfiguration>();
  BusyWaitPolicyConfig bp(100, 30);
  SleepPolicyConfig sp(40);
  std::vector<std::shared_ptr<SlowdownPolicyConfig>> policies;
  policies.push_back(std::make_shared<BusyWaitPolicyConfig>(bp));
  policies.push_back(std::make_shared<SleepPolicyConfig>(sp));
  (*sm)[SlowdownPhase::BftClientBeforeSendPrimary] = policies;
  PerformanceManager pm(sm);
  EXPECT_TRUE(pm.isEnabled<SlowdownManager>());
  SlowDownResult res = pm.Delay<SlowdownPhase::BftClientBeforeSendPrimary>();
  EXPECT_EQ(res.phase, SlowdownPhase::BftClientBeforeSendPrimary);
  EXPECT_EQ(res.totalWaitDuration, bp.wait_duration_ms);
  auto expectedSleepDur = bp.sleep_duration_ms ? bp.wait_duration_ms / bp.sleep_duration_ms * bp.sleep_duration_ms : 0;
  EXPECT_GE(res.totalSleepDuration, sp.sleep_duration_ms + expectedSleepDur);
  EXPECT_EQ(res.totalKeyCount, 0);
  EXPECT_EQ(res.totalValueSize, 0);
}

TEST(slowdown_test, hybrid_configuration) {
  auto sm = std::make_shared<SlowdownConfiguration>();
  BusyWaitPolicyConfig bp(200, 50);
  SleepPolicyConfig sp(40);
  AddKeysPolicyConfig ap(10, 200, 3000);
  std::vector<std::shared_ptr<SlowdownPolicyConfig>> policies;
  policies.push_back(std::make_shared<BusyWaitPolicyConfig>(bp));
  policies.push_back(std::make_shared<SleepPolicyConfig>(sp));
  policies.push_back(std::make_shared<AddKeysPolicyConfig>(ap));
  sm->insert({SlowdownPhase::StorageBeforeDbWrite, policies});
  sm->insert({SlowdownPhase::StorageBeforeKVBC, policies});
  PerformanceManager pm(sm);
  EXPECT_TRUE(pm.isEnabled<SlowdownManager>());
  concord::kvbc::SetOfKeyValuePairs set;
  auto s = std::chrono::steady_clock::now();
  SlowDownResult res = pm.Delay<SlowdownPhase::StorageBeforeDbWrite>(set);
  auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - s).count();
  EXPECT_GE(dur, bp.wait_duration_ms + sp.sleep_duration_ms);
  EXPECT_EQ(res.phase, SlowdownPhase::StorageBeforeDbWrite);
  EXPECT_EQ(res.totalWaitDuration, bp.wait_duration_ms);
  auto expectedSleepDur = bp.sleep_duration_ms ? bp.wait_duration_ms / bp.sleep_duration_ms * bp.sleep_duration_ms : 0;
  EXPECT_GE(res.totalSleepDuration, expectedSleepDur + sp.sleep_duration_ms);
  EXPECT_EQ(res.totalKeyCount, 10);
  EXPECT_EQ(set.size(), 10);
  EXPECT_GE(res.totalValueSize, 10 * 3000);
  set.clear();
  EXPECT_EQ(set.size(), 0);
  pm.Delay<SlowdownPhase::StorageBeforeKVBC>(set);
  EXPECT_EQ(set.size(), 10);
}

TEST(slowdown_test, add_keys_configuration) {
  using namespace concord::kvbc;
  using namespace concord::kvbc::v2MerkleTree;

  auto sm = std::make_shared<SlowdownConfiguration>();
  AddKeysPolicyConfig ap(10, 200, 3000);
  std::vector<std::shared_ptr<SlowdownPolicyConfig>> policies;
  policies.push_back(std::make_shared<AddKeysPolicyConfig>(ap));

  std::unique_ptr<IStorageFactory> f = std::make_unique<MemoryDBStorageFactory>();
  auto comm = MockComm();
  bftEngine::ReplicaConfig& rc = bftEngine::ReplicaConfig::instance();
  rc.numReplicas = 4;
  rc.fVal = 1;
  rc.cVal = 0;
  rc.numOfExternalClients = 1;
  (*sm)[SlowdownPhase::StorageBeforeKVBC] = policies;
  auto pm = std::make_shared<PerformanceManager>(sm);

  ReplicaImp r(
      dynamic_cast<ICommunication*>(&comm), rc, std::move(f), std::make_shared<concordMetrics::Aggregator>(), pm);
  SetOfKeyValuePairs set;
  set.insert({concordUtils::Sliver("key"), concordUtils::Sliver("value")});
  set.insert({concordUtils::Sliver("key1"), concordUtils::Sliver("value2")});
  set.insert({concordUtils::Sliver("key2"), concordUtils::Sliver("value2")});
  auto size = set.size();
  concordUtils::SpanWrapper s;
  BlockId id;
  r.addBlock(set, id, s);
  EXPECT_EQ(id, 1);
  EXPECT_EQ(set.size(), size);  // was not changed
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}