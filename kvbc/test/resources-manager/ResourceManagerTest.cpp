// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "IntervalMappingResourceManager.hpp"
#include "IResourceManager.hpp"
#include "ISystemResourceEntity.hpp"
#include <thread>
using namespace std::chrono_literals;

#include <gtest/gtest.h>

namespace {

const double MaxToleratedError = 0.5;

using namespace concord::performance;

class ResourceEntityMock : public ISystemResourceEntity {
 public:
  virtual ~ResourceEntityMock() = default;
  virtual int64_t getAvailableResources() const override { return availableResources; }
  virtual uint64_t getMeasurement(const type type) const override {
    switch (type) {
      case type::pruning_utilization:
        return cpuMeasurements;
      default:
        return measurements;
    }
  }

  virtual const std::string getResourceName() const override { return "MOCK"; }

  virtual void reset() override {
    availableResources = 0;
    measurements = 0;
    cpuMeasurements = 0;
  }

  virtual void stop() override {}
  virtual void start() override {}

  virtual void addMeasurement(const measurement& measurement) override {}

  int64_t availableResources;
  uint64_t measurements;
  uint64_t cpuMeasurements;
};  // namespace

TEST(IntervalMappingResourceManager_test, duration) {
  std::vector<std::pair<uint64_t, uint64_t>> mapping{{200, 100}, {600, 10}, {1000, 5}};
  auto consensusEngineResourceMonitor = ResourceEntityMock{};

  auto interval_mapping = IntervalMappingResourceManager::createIntervalMappingResourceManager(
      consensusEngineResourceMonitor, std::move(mapping), IntervalMappingResourceManagerConfiguration{});
  auto first_duration = interval_mapping->getDurationFromLastCallSec();
  // First duration is 0
  ASSERT_EQ(first_duration, 0);
  // Second duration, short interval, should be rounded up to 1
  {
    auto duration = interval_mapping->getDurationFromLastCallSec();
    ASSERT_EQ(duration, 1);
  }
  {
    std::this_thread::sleep_for(2s);
    auto duration = interval_mapping->getDurationFromLastCallSec();
    ASSERT_EQ(duration, 2);
  }
}

TEST(IntervalMappingResourceManager_test, prune_info) {
  std::vector<std::pair<uint64_t, uint64_t>> mapping{{60, 40}, {100, 30}, {300, 20}, {500, 10}};
  auto consensusEngineResourceMonitor = ResourceEntityMock{};
  auto interval_mapping = IntervalMappingResourceManager::createIntervalMappingResourceManager(
      consensusEngineResourceMonitor, std::move(mapping), IntervalMappingResourceManagerConfiguration{});
  // First prune info is 0
  consensusEngineResourceMonitor.measurements = 110;
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 0);
  }
  consensusEngineResourceMonitor.measurements = 110;
  // second prune gets rate of 110tps
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 20);
  }
  // tps is now will be devided by two i.e. 119/2 = 59
  consensusEngineResourceMonitor.measurements = 119;
  {
    std::this_thread::sleep_for(2s);
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 40);
  }
  // duration 1 -> 600 tps
  consensusEngineResourceMonitor.measurements = 600;
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 0);
  }
  // duration 4 -> 150 tps
  consensusEngineResourceMonitor.measurements = 600;
  {
    std::this_thread::sleep_for(4s);
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 20);
  }
  // duration 1 -> 300 tps
  consensusEngineResourceMonitor.measurements = 300;
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 20);
  }
}

TEST(IntervalMappingResourceManager_test, periodic_interval) {
  std::vector<std::pair<uint64_t, uint64_t>> mapping{{60, 40}, {100, 30}, {300, 20}, {500, 10}};
  auto consensusEngineResourceMonitor = ResourceEntityMock{};
  auto interval_mapping = IntervalMappingResourceManager::createIntervalMappingResourceManager(
      consensusEngineResourceMonitor, std::move(mapping));
  // reset every three calls
  interval_mapping->setPeriod(3);
  // First prune info is 0
  consensusEngineResourceMonitor.measurements = 80;
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 0);
  }

  consensusEngineResourceMonitor.measurements = 110;
  // second prune gets rate of 110tps
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 20);
  }
  // tps is now will be devided by two i.e. 119/2 = 59
  consensusEngineResourceMonitor.measurements = 119;
  {
    std::this_thread::sleep_for(2s);
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 40);
  }
  // period should happen
  consensusEngineResourceMonitor.measurements = 110;
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 0);
  }

  consensusEngineResourceMonitor.measurements = 310;
  // second prune gets rate of 110tps
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 10);
  }
  // tps is now will be devided by two i.e. 119/2 = 59
  consensusEngineResourceMonitor.measurements = 119;
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 20);
  }
  // period should happen
  consensusEngineResourceMonitor.measurements = 10;
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 0);
  }
}

TEST(IntervalMappingResourceManager_test, tps_drop_due_to_pruning_mitigation) {
  std::vector<std::pair<uint64_t, uint64_t>> mapping{{60, 40}, {100, 30}, {300, 20}, {500, 10}};
  auto consensusEngineResourceMonitor = ResourceEntityMock{};
  auto interval_mapping = IntervalMappingResourceManager::createIntervalMappingResourceManager(
      consensusEngineResourceMonitor, std::move(mapping), IntervalMappingResourceManagerConfiguration{});

  // reset every three calls
  interval_mapping->setPeriod(20);
  // First prune info is 0
  consensusEngineResourceMonitor.measurements = 110;
  consensusEngineResourceMonitor.cpuMeasurements = 0;
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 0);
  }

  consensusEngineResourceMonitor.measurements = 110;
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_EQ(prune_info.blocksPerSecond, 20);
  }

  consensusEngineResourceMonitor.measurements = 90;
  consensusEngineResourceMonitor.cpuMeasurements = 80;
  // Too much tps is lost. Slow down pruning
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_LT(abs(prune_info.blocksPerSecond - 6), MaxToleratedError);
  }

  consensusEngineResourceMonitor.measurements = 400;
  consensusEngineResourceMonitor.cpuMeasurements = 50;
  // Tps is growing and pruning ulization is on acceptable level, so mapping to 10
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_LT(abs(prune_info.blocksPerSecond - 10), MaxToleratedError);
  }

  consensusEngineResourceMonitor.measurements = 380;
  consensusEngineResourceMonitor.cpuMeasurements = 90;
  // Pruning utlization is high but the loss in the tps is acceptable
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_LT(abs(prune_info.blocksPerSecond - 10), MaxToleratedError);
  }

  consensusEngineResourceMonitor.measurements = 180;
  consensusEngineResourceMonitor.cpuMeasurements = 90;
  // tps drops naturally, but for safety adjustment is made
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_LT(abs(prune_info.blocksPerSecond - 2), MaxToleratedError);
  }

  consensusEngineResourceMonitor.measurements = 180;
  consensusEngineResourceMonitor.cpuMeasurements = 20;
  // tps stable - resources to prune
  {
    auto prune_info = interval_mapping->getPruneInfo();
    ASSERT_LT(abs(prune_info.blocksPerSecond - 20), MaxToleratedError);
  }
}
}  // anonymous namespace

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
