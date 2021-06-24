// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the 'License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "gtest/gtest.h"
#include "assertUtils.hpp"
#include "client_reconfiguration_engine.hpp"

using namespace cre::state;
using namespace cre::config;
using namespace cre;
namespace {
std::string metrics_component = "client_reconfiguration_engine";
std::string invalids_counter = "invalid_handlers";
std::string errors_counter = "errored_handlers";
class TestStateClient : public IStateClient {
 public:
  State getNextState(uint64_t lastKnownBlockId) override {
    std::string lastKnownBid = std::to_string(lastKnownBlockId + 1);
    return State{lastKnownBlockId + 1, std::vector<uint8_t>(lastKnownBid.begin(), lastKnownBid.end())};
  }
};

class TestExecuteHandler : public IStateHandler {
 public:
  bool validate(const State&) override { return true; }
  bool execute(const State& state) override {
    std::string newBid(state.data.begin(), state.data.end());
    lastKnownState = state.block;
    return true;
  }
  uint64_t lastKnownState{0};
};

class TestPersistOnChainHandler : public IStateHandler {
 public:
  bool validate(const State&) override { return true; }
  bool execute(const State& state) override {
    blocks.push_back(state.block);
    return true;
  }
  std::vector<uint64_t> blocks;
};

class TestInvalidHandler : public IStateHandler {
 public:
  bool validate(const State&) override { return false; }
  bool execute(const State& state) override { return true; }
};

class TestErroredHandler : public IStateHandler {
 public:
  bool validate(const State&) override { return true; }
  bool execute(const State& state) override { return false; }
};
Config c{0, 10};

TEST(test_client_reconfiguration_engine, test_normal_start_and_shutdown) {
  ASSERT_NO_THROW(ClientReconfigurationEngine(c, new TestStateClient()));
  ClientReconfigurationEngine cre(c, new TestStateClient());
  ASSERT_NO_THROW(cre.start());
  std::this_thread::sleep_for(1s);
}

TEST(test_client_reconfiguration_engine, test_invalid_handler) {
  ClientReconfigurationEngine cre(c, new TestStateClient());
  cre.registerHandler(std::make_shared<TestInvalidHandler>());
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  cre.setAggregator(aggregator);
  cre.start();
  std::this_thread::sleep_for(1s);
  ASSERT_GT(aggregator->GetCounter(metrics_component, invalids_counter).Get(), 1);
}

TEST(test_client_reconfiguration_engine, test_errored_handler) {
  ClientReconfigurationEngine cre(c, new TestStateClient());
  cre.registerHandler(std::make_shared<TestErroredHandler>());
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  cre.setAggregator(aggregator);
  cre.start();
  std::this_thread::sleep_for(1s);
  ASSERT_GT(aggregator->GetCounter(metrics_component, errors_counter).Get(), 1);
}

TEST(test_client_reconfiguration_engine, test_cre) {
  std::shared_ptr<ClientReconfigurationEngine> cre =
      std::make_shared<ClientReconfigurationEngine>(c, new TestStateClient());
  std::shared_ptr<TestExecuteHandler> handler = std::make_shared<TestExecuteHandler>();
  std::shared_ptr<TestPersistOnChainHandler> chainHandler = std::make_shared<TestPersistOnChainHandler>();
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  cre->registerHandler(handler);
  cre->registerUpdateStateHandler(chainHandler);
  cre->setAggregator(aggregator);
  cre->start();
  std::this_thread::sleep_for(1s);
  ASSERT_EQ(aggregator->GetCounter(metrics_component, errors_counter).Get(), 0);
  ASSERT_EQ(aggregator->GetCounter(metrics_component, invalids_counter).Get(), 0);
  ASSERT_GT(handler->lastKnownState, 0);
  ASSERT_GT(chainHandler->blocks.size(), 0);
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}