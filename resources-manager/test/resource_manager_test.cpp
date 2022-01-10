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

#include "IntervalMappingResourceManager.hpp"
#include "IResourceManager.hpp"
#include "ISystemResourceEntity.hpp"
#include "SubstractFromMaxResourceManager.hpp"

namespace {

using namespace concord::performance;

class ResourceEntityMock : public ISystemResourceEntity {
 public:
  virtual ~ResourceEntityMock() = default;
  virtual int64_t getAvailableResources() const override { return availableResources; }
  virtual uint64_t getMeasurements() const override { return measurements; }
  virtual const std::string& getResourceName() const override { return mock; }

  int64_t availableResources;
  uint64_t measurements;
  const std::string mock = "MOCK";
};

TEST(resource_manager_test, IntervalMappingResourceManager_test) {
  std::vector<std::pair<uint64_t, uint64_t>> mapping{{200, 100}, {600, 10}, {1000, 5}};
  auto consensusEngineResourceMonitor = std::make_shared<ResourceEntityMock>();
  consensusEngineResourceMonitor->availableResources = 110;

  std::shared_ptr<IResourceManager> sut(IntervalMappingResourceManager::createIntervalMappingResourceManager(
      consensusEngineResourceMonitor, std::move(mapping)));

  EXPECT_EQ(sut->getAvailableResources(), 100);
  consensusEngineResourceMonitor->availableResources = 200;
  EXPECT_EQ(sut->getAvailableResources(), 100);
  consensusEngineResourceMonitor->availableResources = 400;
  EXPECT_EQ(sut->getAvailableResources(), 10);
  consensusEngineResourceMonitor->availableResources = 800;
  EXPECT_EQ(sut->getAvailableResources(), 5);
  consensusEngineResourceMonitor->availableResources = 1800;
  EXPECT_EQ(sut->getAvailableResources(), 0);
}

TEST(resource_manager_test, SubstractFromMaxResourceManager_test) {
  auto consensusEngineResourceMonitor = std::make_shared<ResourceEntityMock>();
  consensusEngineResourceMonitor->availableResources = 110;

  auto databaseResourceMonitor = std::make_shared<ResourceEntityMock>();
  databaseResourceMonitor->availableResources = 50;
  std::vector<std::shared_ptr<ISystemResourceEntity>> systemResources = {consensusEngineResourceMonitor,
                                                                         databaseResourceMonitor};

  std::shared_ptr<IResourceManager> sut(new SubstractFromMaxResourceManager(1000, std::move(systemResources)));

  EXPECT_EQ(sut->getAvailableResources(), 840);
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}