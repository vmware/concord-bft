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

#include "PerformanceManager.hpp"

namespace {

using namespace concord::performance;

TEST(slowdown_test, empty_configuration) {
  PerformanceManager pm;
  SlowDownResult res = pm.Delay<SlowdownPhase::PreProcessorAfterPreexecPrimary>();
  assert(res.phase == SlowdownPhase::None);
  assert(res.totalWaitDuration == 0);
  assert(res.totalSleepDuration == 0);
  assert(res.totalKeyCount == 0);
  assert(res.totalValueSize == 0);
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
  SlowDownResult res = pm.Delay<SlowdownPhase::BftClientBeforeSendPrimary>();
  assert(res.phase == SlowdownPhase::BftClientBeforeSendPrimary);
  assert(res.totalWaitDuration == bp.wait_duration_ms);
  assert(res.totalSleepDuration == bp.sleep_duration_ms + sp.sleep_duration_ms);
  assert(res.totalKeyCount == 0);
  assert(res.totalValueSize == 0);
}

TEST(slowdown_test, hybrid_configuration) {
  auto sm = std::make_shared<SlowdownConfiguration>();
  BusyWaitPolicyConfig bp(100, 30);
  SleepPolicyConfig sp(40);
  AddKeysPolicyConfig ap(10, 200, 3000);
  std::vector<std::shared_ptr<SlowdownPolicyConfig>> policies;
  policies.push_back(std::make_shared<BusyWaitPolicyConfig>(bp));
  policies.push_back(std::make_shared<SleepPolicyConfig>(sp));
  policies.push_back(std::make_shared<AddKeysPolicyConfig>(ap));
  (*sm)[SlowdownPhase::StorageBeforeDbWrite] = policies;
  PerformanceManager pm(sm);
  concord::kvbc::SetOfKeyValuePairs set;
  SlowDownResult res = pm.Delay<SlowdownPhase::StorageBeforeDbWrite>(set);
  assert(res.phase == SlowdownPhase::StorageBeforeDbWrite);
  assert(res.totalWaitDuration == bp.wait_duration_ms);
  assert(res.totalSleepDuration == bp.sleep_duration_ms + sp.sleep_duration_ms);
  assert(res.totalKeyCount == 10);
  assert(set.size() == 10);
  assert(res.totalValueSize >= 10 * 3000);
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}