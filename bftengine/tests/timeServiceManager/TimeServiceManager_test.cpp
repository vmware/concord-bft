// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "TimeServiceManager.hpp"
#include "Metrics.hpp"
#include "ReplicaConfig.hpp"
#include "gtest/gtest.h"
#include "serialize.hpp"
#include <chrono>
#include <memory>

using namespace bftEngine;
using namespace bftEngine::impl;

struct ReservedPagesMock : public IReservedPages {
  mutable bool is_first_load = true;
  std::string page_ = std::string(sizeof(ConsensusTickRep), 0);
  ReservedPagesMock() { ReservedPagesClientBase::setReservedPages(this); }
  ~ReservedPagesMock() { ReservedPagesClientBase::setReservedPages(nullptr); }
  virtual uint32_t numberOfReservedPages() const { return 1; };
  virtual uint32_t sizeOfReservedPage() const { return page_.size(); };
  virtual bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const {
    if (is_first_load) {
      is_first_load = false;
      return false;
    }
    (void)reservedPageId;
    memcpy(outReservedPage, page_.c_str(), copyLength);
    return true;
  };
  virtual void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) {
    (void)reservedPageId;
    page_ = std::string(inReservedPage, inReservedPage + copyLength);
  };
  virtual void zeroReservedPage(uint32_t reservedPageId) {
    (void)reservedPageId;
    page_ = std::string(sizeof(ConsensusTickRep), 0);
  };
};

struct FakeClock {
  static std::chrono::milliseconds current_time;
  static std::chrono::system_clock::time_point now() { return std::chrono::system_clock::time_point{current_time}; }
};

std::chrono::milliseconds FakeClock::current_time = std::chrono::milliseconds::min();

TEST(TimeServiceManager, TimeWithinLimits) {
  ReservedPagesMock m;
  auto& config = ReplicaConfig::instance();
  config.timeServiceEnabled = true;
  config.timeServiceEpsilonMillis = std::chrono::milliseconds{1};
  config.timeServiceHardLimitMillis = std::chrono::seconds{3};
  config.timeServiceSoftLimitMillis = std::chrono::milliseconds{500};

  const auto now = ConsensusTime{1000};
  FakeClock::current_time = now;

  auto aggregator = std::make_shared<concordMetrics::Aggregator>();
  auto manager = TimeServiceManager<FakeClock>{aggregator};
  const auto time = manager.getTimePoint();

  EXPECT_TRUE(manager.isPrimarysTimeWithinBounds(time));
  EXPECT_EQ(aggregator->GetCounter("time_service", "hard_limit_reached_counter").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "soft_limit_reached_counter").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "new_time_is_less_or_equal_to_previous").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "ill_formed_preprepare").Get(), 0);
}

TEST(TimeServiceManager, TimeOutOfHardLimits) {
  ReservedPagesMock m;
  auto& config = ReplicaConfig::instance();
  config.timeServiceEnabled = true;
  config.timeServiceEpsilonMillis = std::chrono::milliseconds{1};
  config.timeServiceHardLimitMillis = std::chrono::seconds{3};
  config.timeServiceSoftLimitMillis = std::chrono::milliseconds{500};

  const auto now = ConsensusTime{1000};
  FakeClock::current_time = now;

  auto aggregator = std::make_shared<concordMetrics::Aggregator>();
  auto manager = TimeServiceManager<FakeClock>{aggregator};
  const auto time = manager.getTimePoint();

  FakeClock::current_time = now + config.timeServiceHardLimitMillis + std::chrono::milliseconds{1};
  EXPECT_FALSE(manager.isPrimarysTimeWithinBounds(time));

  FakeClock::current_time = now - config.timeServiceHardLimitMillis - std::chrono::milliseconds{1};
  EXPECT_FALSE(manager.isPrimarysTimeWithinBounds(time));
  EXPECT_EQ(aggregator->GetCounter("time_service", "hard_limit_reached_counter").Get(), 2);
  EXPECT_EQ(aggregator->GetCounter("time_service", "soft_limit_reached_counter").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "new_time_is_less_or_equal_to_previous").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "ill_formed_preprepare").Get(), 0);
}

TEST(TimeServiceManager, TimeOnTheEdgeOfHardLimits) {
  ReservedPagesMock m;
  auto& config = ReplicaConfig::instance();
  config.timeServiceEnabled = true;
  config.timeServiceEpsilonMillis = std::chrono::milliseconds{1};
  config.timeServiceHardLimitMillis = std::chrono::seconds{3};
  config.timeServiceSoftLimitMillis = std::chrono::milliseconds{500};

  const auto now = ConsensusTime{1000};
  FakeClock::current_time = now;

  auto aggregator = std::make_shared<concordMetrics::Aggregator>();
  auto manager = TimeServiceManager<FakeClock>{aggregator};
  const auto time = manager.getTimePoint();

  FakeClock::current_time = now + config.timeServiceHardLimitMillis;
  EXPECT_TRUE(manager.isPrimarysTimeWithinBounds(time));

  FakeClock::current_time = now - config.timeServiceHardLimitMillis;
  EXPECT_TRUE(manager.isPrimarysTimeWithinBounds(time));

  EXPECT_EQ(aggregator->GetCounter("time_service", "hard_limit_reached_counter").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "soft_limit_reached_counter").Get(), 2);
  EXPECT_EQ(aggregator->GetCounter("time_service", "new_time_is_less_or_equal_to_previous").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "ill_formed_preprepare").Get(), 0);
}

TEST(TimeServiceManager, TimeOutOfSoftLimits) {
  ReservedPagesMock m;
  auto& config = ReplicaConfig::instance();
  config.timeServiceEnabled = true;
  config.timeServiceEpsilonMillis = std::chrono::milliseconds{1};
  config.timeServiceHardLimitMillis = std::chrono::seconds{3};
  config.timeServiceSoftLimitMillis = std::chrono::milliseconds{500};

  const auto now = ConsensusTime{1000};
  FakeClock::current_time = now;

  auto aggregator = std::make_shared<concordMetrics::Aggregator>();
  auto manager = TimeServiceManager<FakeClock>{aggregator};
  const auto time = manager.getTimePoint();

  FakeClock::current_time = now + config.timeServiceSoftLimitMillis + std::chrono::milliseconds{1};
  EXPECT_TRUE(manager.isPrimarysTimeWithinBounds(time));

  FakeClock::current_time = now - config.timeServiceSoftLimitMillis - std::chrono::milliseconds{1};
  EXPECT_TRUE(manager.isPrimarysTimeWithinBounds(time));

  EXPECT_EQ(aggregator->GetCounter("time_service", "hard_limit_reached_counter").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "soft_limit_reached_counter").Get(), 2);
  EXPECT_EQ(aggregator->GetCounter("time_service", "new_time_is_less_or_equal_to_previous").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "ill_formed_preprepare").Get(), 0);
}

TEST(TimeServiceManager, TimeOnTheEdgeOfSoftLimits) {
  ReservedPagesMock m;
  auto& config = ReplicaConfig::instance();
  config.timeServiceEnabled = true;
  config.timeServiceEpsilonMillis = std::chrono::milliseconds{1};
  config.timeServiceHardLimitMillis = std::chrono::seconds{3};
  config.timeServiceSoftLimitMillis = std::chrono::milliseconds{500};

  const auto now = ConsensusTime{1000};
  FakeClock::current_time = now;

  auto aggregator = std::make_shared<concordMetrics::Aggregator>();
  auto manager = TimeServiceManager<FakeClock>{aggregator};
  const auto time = manager.getTimePoint();

  FakeClock::current_time = now + config.timeServiceSoftLimitMillis;
  EXPECT_TRUE(manager.isPrimarysTimeWithinBounds(time));

  FakeClock::current_time = now - config.timeServiceSoftLimitMillis;
  EXPECT_TRUE(manager.isPrimarysTimeWithinBounds(time));

  EXPECT_EQ(aggregator->GetCounter("time_service", "hard_limit_reached_counter").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "soft_limit_reached_counter").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "new_time_is_less_or_equal_to_previous").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "ill_formed_preprepare").Get(), 0);
}

TEST(TimeServiceManager, CompareAndUpdate) {
  ReservedPagesMock m;
  auto& config = ReplicaConfig::instance();
  config.timeServiceEnabled = true;
  config.timeServiceEpsilonMillis = std::chrono::milliseconds{1};
  config.timeServiceHardLimitMillis = std::chrono::seconds{3};
  config.timeServiceSoftLimitMillis = std::chrono::milliseconds{500};

  const auto now = ConsensusTime{1000};
  auto aggregator = std::make_shared<concordMetrics::Aggregator>();
  auto manager = TimeServiceManager<std::chrono::system_clock>{aggregator};

  // check that if now does not move, the manager increases it by epsilon
  for (size_t i = 0U; i < 10; ++i) {
    EXPECT_EQ(now + (config.timeServiceEpsilonMillis * i), manager.compareAndUpdate(now));
  }

  EXPECT_EQ(aggregator->GetCounter("time_service", "hard_limit_reached_counter").Get(), 0);
  EXPECT_EQ(aggregator->GetCounter("time_service", "soft_limit_reached_counter").Get(), 0);
  // DD: The first time compareAndUpdate is called there is no saved value, so TS accepts the given one.
  EXPECT_EQ(aggregator->GetCounter("time_service", "new_time_is_less_or_equal_to_previous").Get(), 9);
  EXPECT_EQ(aggregator->GetCounter("time_service", "ill_formed_preprepare").Get(), 0);
}

TEST(TimeServiceManager, recoverTime) {
  ReservedPagesMock m;
  auto& config = ReplicaConfig::instance();
  config.timeServiceEnabled = true;
  config.timeServiceEpsilonMillis = std::chrono::milliseconds{1};
  config.timeServiceHardLimitMillis = std::chrono::seconds{3};
  config.timeServiceSoftLimitMillis = std::chrono::milliseconds{500};

  const auto now = ConsensusTime{1000};
  auto aggregator = std::make_shared<concordMetrics::Aggregator>();
  auto manager = TimeServiceManager<std::chrono::system_clock>{aggregator};
  auto update = manager.compareAndUpdate(now);
  EXPECT_EQ(update, now);

  auto ticksFromRecovery = now.count() - 10;
  EXPECT_LT(ticksFromRecovery, update.count());
  // scenario 1 - recover time is the same as the stored
  manager.recoverTime(now.count());
  EXPECT_EQ(manager.getTime(), now);
  // scenario 2 - recover time is smaller than stored - replace
  manager.recoverTime(ticksFromRecovery);
  EXPECT_EQ(ticksFromRecovery, manager.getTime().count());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
