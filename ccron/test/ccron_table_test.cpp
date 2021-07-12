// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"

#include "ccron_msgs.cmf.hpp"

#include "ccron/cron_res_page_client.hpp"
#include "ccron/cron_table.hpp"
#include "ccron/periodic_action.hpp"

#include "ReservedPagesClient.hpp"
#include "ReservedPagesMock.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include <vector>

namespace {

using namespace concord::cron;
using namespace std::chrono_literals;

class ccron_table_test : public ::testing::Test {
 protected:
  const std::uint32_t kComponentId = 99;
  CronTable table_{kComponentId};
  const Tick tick_{kComponentId, 42, 10ms};
  const std::uint32_t kPersistSchedulePos = 100;

  std::vector<std::uint32_t> actions_called_;
  std::vector<std::uint32_t> schedule_next_called_;
  std::vector<std::uint32_t> on_remove_called_;

  bftEngine::test::ReservedPagesMock<CronResPagesClient> res_pages_mock_;

  void SetUp() override { bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_); }

 protected:
  auto action(std::uint32_t position, bool check_tick = true) {
    return [this, position, check_tick](const Tick& t) {
      if (check_tick) {
        ASSERT_EQ(t, tick_);
      }
      actions_called_.push_back(position);
    };
  }

  auto scheduleNext(std::uint32_t position) {
    return [this, position](const Tick& t) {
      ASSERT_EQ(t, tick_);
      schedule_next_called_.push_back(position);
    };
  }

  auto onRemove() {
    return [this](std::uint32_t component_id, std::uint32_t position) {
      ASSERT_EQ(kComponentId, component_id);
      on_remove_called_.push_back(position);
    };
  }

  auto trueRule() const {
    return [this](const Tick& t) {
      // Use EXPECT_EQ instead of ASSERT_EQ, because EXPECT_EQ doesn't require a void return type for the lambda. This
      // is a GTest workaround. The test will still fail if ticks aren't equal.
      EXPECT_EQ(t, tick_);
      return true;
    };
  }

  auto falseRule() const {
    return [this](const Tick& t) {
      // Use EXPECT_EQ instead of ASSERT_EQ, because EXPECT_EQ doesn't require a void return type for the lambda. This
      // is a GTest workaround. The test will still fail if ticks aren't equal.
      EXPECT_EQ(t, tick_);
      return false;
    };
  }

  PeriodicActionSchedule getScheduleFromResPage() const {
    auto schedule = PeriodicActionSchedule{};
    EXPECT_EQ(1, res_pages_mock_.pages().size());
    const auto& page = res_pages_mock_.pages().begin()->second;
    auto page_ptr = reinterpret_cast<const uint8_t*>(page.data());
    deserialize(page_ptr, page_ptr + page.size(), schedule);
    EXPECT_EQ(1, schedule.components.size());
    EXPECT_EQ(1, schedule.components.count(kComponentId));
    return schedule;
  }
};

TEST_F(ccron_table_test, evaluates_in_order) {
  table_.addEntry(CronEntry{3, trueRule(), action(3)});
  table_.addEntry(CronEntry{1, trueRule(), action(1), scheduleNext(1)});
  table_.addEntry(CronEntry{4, trueRule(), action(4), scheduleNext(4)});
  table_.addEntry(CronEntry{2, falseRule(), action(2)});
  table_.evaluate(tick_);

  // Evaluates in order, not calling entry 2 as its rule is false.
  ASSERT_EQ(3, actions_called_.size());
  ASSERT_EQ(1, actions_called_[0]);
  ASSERT_EQ(3, actions_called_[1]);
  ASSERT_EQ(4, actions_called_[2]);

  // Evaluates in order and scheduling 1 and 4 only, because 2's rules is false and 3 doesn't have a next schedule.
  ASSERT_EQ(2, schedule_next_called_.size());
  ASSERT_EQ(1, schedule_next_called_[0]);
  ASSERT_EQ(4, schedule_next_called_[1]);
}

TEST_F(ccron_table_test, remove_entry) {
  table_.addEntry(CronEntry{1, trueRule(), action(1), scheduleNext(1), onRemove()});
  table_.addEntry(CronEntry{2, trueRule(), action(2), scheduleNext(2), onRemove()});

  ASSERT_TRUE(table_.removeEntry(1));
  ASSERT_EQ(1, on_remove_called_.size());
  ASSERT_EQ(1, on_remove_called_[0]);

  table_.evaluate(tick_);

  // Actions and next schedulers are called for the other entry after remove.
  ASSERT_EQ(1, actions_called_.size());
  ASSERT_EQ(2, actions_called_[0]);
  ASSERT_EQ(1, schedule_next_called_.size());
  ASSERT_EQ(2, schedule_next_called_[0]);

  // Try to remove again.
  ASSERT_FALSE(table_.removeEntry(1));

  // No additional remove calls.
  ASSERT_EQ(1, on_remove_called_.size());
  ASSERT_EQ(1, on_remove_called_[0]);

  table_.evaluate(tick_);

  // Actions and next schedulers are called for the other entry again after a failed remove.
  ASSERT_EQ(2, actions_called_.size());
  ASSERT_EQ(2, actions_called_[1]);
  ASSERT_EQ(2, schedule_next_called_.size());
  ASSERT_EQ(2, schedule_next_called_[1]);

  // Remove the last entry.
  ASSERT_TRUE(table_.removeEntry(2));
  ASSERT_EQ(2, on_remove_called_.size());
  ASSERT_EQ(1, on_remove_called_[0]);
  ASSERT_EQ(2, on_remove_called_[1]);

  ASSERT_TRUE(table_.empty());
}

TEST_F(ccron_table_test, remove_all_entries) {
  table_.addEntry(CronEntry{1, trueRule(), action(1), scheduleNext(1), onRemove()});
  table_.addEntry(CronEntry{2, trueRule(), action(2), scheduleNext(2), onRemove()});

  ASSERT_EQ(2, table_.removeAllEntries());
  ASSERT_EQ(2, on_remove_called_.size());
  ASSERT_EQ(1, on_remove_called_[0]);
  ASSERT_EQ(2, on_remove_called_[1]);
}

TEST_F(ccron_table_test, at) {
  table_.addEntry(CronEntry{1, trueRule(), action(1)});

  ASSERT_NE(nullptr, table_.at(1));
  ASSERT_EQ(1, table_.at(1)->position);
  ASSERT_EQ(nullptr, table_.at(2));
}

TEST_F(ccron_table_test, number_of_entries_and_empty) {
  table_.addEntry(CronEntry{1, trueRule(), action(1)});
  table_.addEntry(CronEntry{2, trueRule(), action(2)});

  ASSERT_EQ(2, table_.numberOfEntries());
  ASSERT_FALSE(table_.empty());

  table_.removeAllEntries();
  ASSERT_EQ(0, table_.numberOfEntries());
  ASSERT_TRUE(table_.empty());

  auto new_table = CronTable{kComponentId};
  ASSERT_EQ(0, new_table.numberOfEntries());
  ASSERT_TRUE(new_table.empty());
}

TEST_F(ccron_table_test, component_id) { ASSERT_EQ(kComponentId, table_.componentId()); }

TEST_F(ccron_table_test, periodic_action) {
  const auto check_tick = false;
  const auto position1 = 7;
  const auto position2 = 8;
  const auto tick1 = Tick{kComponentId, 1, 1s};
  const auto tick2 = Tick{kComponentId, 2, 2s};
  const auto tick3 = Tick{kComponentId, 3, 3s};
  const auto tick4 = Tick{kComponentId, 4, 4s};
  const auto tick5 = Tick{kComponentId, 5, 5s};
  const auto tick6 = Tick{kComponentId, 6, 6s};
  const auto tick7 = Tick{kComponentId, 7, 7s};

  table_.addEntry(periodicAction(position1, action(position1, check_tick), 2s));
  table_.addEntry(persistPeriodicSchedule(kPersistSchedulePos));

  // First tick always executes the action. We schedule the next invocation for 1s + 2s = 3s.
  table_.evaluate(tick1);
  ASSERT_EQ(1, actions_called_.size());
  ASSERT_EQ(position1, actions_called_[0]);

  // tick2 at 2s should not execute the action.
  table_.evaluate(tick2);
  ASSERT_EQ(1, actions_called_.size());
  ASSERT_EQ(position1, actions_called_[0]);

  // tick3 at 3s should execute the action and schedule for 3s + 2s = 5s.
  table_.evaluate(tick3);
  ASSERT_EQ(2, actions_called_.size());
  ASSERT_EQ(position1, actions_called_[0]);
  ASSERT_EQ(position1, actions_called_[1]);

  // Make sure new tables honour the persisted schedule.
  {
    auto new_table = CronTable{kComponentId};
    new_table.addEntry(periodicAction(position1, action(position1, check_tick), 2s));
    new_table.addEntry(persistPeriodicSchedule(kPersistSchedulePos));

    // tick4 should not execute the action.
    new_table.evaluate(tick4);
    ASSERT_EQ(2, actions_called_.size());
    ASSERT_EQ(position1, actions_called_[0]);
    ASSERT_EQ(position1, actions_called_[1]);

    // tick5 should execute the action and schedule for 5s + 2s = 7s.
    new_table.evaluate(tick5);
    ASSERT_EQ(3, actions_called_.size());
    ASSERT_EQ(position1, actions_called_[0]);
    ASSERT_EQ(position1, actions_called_[1]);
    ASSERT_EQ(position1, actions_called_[2]);
  }

  // Make sure new entries are honored for the first time.
  {
    auto new_table = CronTable{kComponentId};
    new_table.addEntry(periodicAction(position1, action(position1, check_tick), 2s));
    new_table.addEntry(periodicAction(position2, action(position2, check_tick), 1s));
    new_table.addEntry(persistPeriodicSchedule(kPersistSchedulePos));

    // tick6 should execute the second action for the first time and not call the first action.
    new_table.evaluate(tick6);
    ASSERT_EQ(4, actions_called_.size());
    ASSERT_EQ(position1, actions_called_[0]);
    ASSERT_EQ(position1, actions_called_[1]);
    ASSERT_EQ(position1, actions_called_[2]);
    ASSERT_EQ(position2, actions_called_[3]);

    // tick7 should execute both actions.
    new_table.evaluate(tick7);
    ASSERT_EQ(6, actions_called_.size());
    ASSERT_EQ(position1, actions_called_[0]);
    ASSERT_EQ(position1, actions_called_[1]);
    ASSERT_EQ(position1, actions_called_[2]);
    ASSERT_EQ(position2, actions_called_[3]);
    ASSERT_EQ(position1, actions_called_[4]);
    ASSERT_EQ(position2, actions_called_[5]);
  }

  // Make sure that removing an entry removes its saved schedule only.
  {
    auto new_table = CronTable{kComponentId};
    new_table.addEntry(periodicAction(position1, action(position1, check_tick), 2s));
    new_table.addEntry(periodicAction(position2, action(position2, check_tick), 1s));
    new_table.addEntry(persistPeriodicSchedule(kPersistSchedulePos));

    auto schedule_before = getScheduleFromResPage();
    const auto& saved_table_before = schedule_before.components[kComponentId];
    ASSERT_EQ(2, saved_table_before.size());
    ASSERT_EQ(1, saved_table_before.count(position1));
    ASSERT_EQ(1, saved_table_before.count(position2));

    // Remove the entry at position1.
    ASSERT_TRUE(new_table.removeEntry(position1));

    // Verify that only position2 is left.
    auto schedule_after = getScheduleFromResPage();
    const auto& saved_table_after = schedule_after.components[kComponentId];
    ASSERT_EQ(1, saved_table_after.size());
    ASSERT_EQ(1, saved_table_after.count(position2));
  }

  // Make sure that removing the schedule entry zeroes the reserved page.
  {
    auto new_table = CronTable{kComponentId};
    new_table.addEntry(persistPeriodicSchedule(kPersistSchedulePos));

    ASSERT_FALSE(res_pages_mock_.isReservedPageZeroed(kPeriodicCronReservedPageId));
    ASSERT_TRUE(new_table.removeEntry(kPersistSchedulePos));
    ASSERT_TRUE(res_pages_mock_.isReservedPageZeroed(kPeriodicCronReservedPageId));
  }
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
