// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//

#include <cstdlib>
#include "gtest/gtest.h"
#include "Timers.hpp"

using namespace std;
using namespace std::chrono;

namespace concordUtil {

typedef Timers::Handle Handle;

TEST(TimersTest, Basic) {
  milliseconds duration(100);

  int oneshot_counter = 0;
  int recurring_counter = 0;
  auto timers = Timers();
  steady_clock::time_point now = steady_clock::now();

  // Create a oneshot and a recurring timer and add them to the heap. Each one
  // will fire a callback when it expires that increments a counter.
  timers.add(
      duration, Timers::Timer::ONESHOT, [&oneshot_counter](Handle h) { ++oneshot_counter; }, now);
  auto recurring_handle = timers.add(
      duration, Timers::Timer::RECURRING, [&recurring_counter](Handle h) { ++recurring_counter; }, now);

  // Clock hasn't advanced so no timers should fire
  timers.evaluate(now);
  ASSERT_EQ(0, oneshot_counter);
  ASSERT_EQ(0, recurring_counter);

  // Fake steady clock advancement
  now += duration;

  // Both timers should fire as clock has advanced past their expiry time
  timers.evaluate(now);
  ASSERT_EQ(1, oneshot_counter);
  ASSERT_EQ(1, recurring_counter);

  // No timers should fire, as they already fired and the clock hasn't moved
  timers.evaluate(now);
  ASSERT_EQ(1, oneshot_counter);
  ASSERT_EQ(1, recurring_counter);

  // Advance the clock again.
  now += duration;

  // Only the recurring timer should fire as the oneshot has been removed.
  timers.evaluate(now);
  ASSERT_EQ(1, oneshot_counter);
  ASSERT_EQ(2, recurring_counter);

  // Reset the duration of the recurring timer to be twice as long.
  timers.reset(recurring_handle, duration * 2, now);

  // Advance the clock by the original duration.
  now += duration;

  // Neither timer should fire as the recurring timer is the only one left, and
  // it hasn't expired yet.
  timers.evaluate(now);
  ASSERT_EQ(1, oneshot_counter);
  ASSERT_EQ(2, recurring_counter);

  // Advance the clock by the original duration again.
  now += duration;

  // The recurring timer which was rescheduled should fire, since the extended
  // duration has elapsed.
  timers.evaluate(now);
  ASSERT_EQ(1, oneshot_counter);
  ASSERT_EQ(3, recurring_counter);

  // Delete the recurring timer
  timers.cancel(recurring_handle);

  // Advance the clock again by the extended duration.
  now += duration * 2;

  // No timers should fire as both have been removed.
  timers.evaluate(now);
  ASSERT_EQ(1, oneshot_counter);
  ASSERT_EQ(3, recurring_counter);

  // Add a third timer and ensure it fires.
  // This tests the monotonicity of counter ids as used by handles.
  bool third_timer_fired = false;
  auto handle = timers.add(
      duration, Timers::Timer::ONESHOT, [&third_timer_fired](Handle h) { third_timer_fired = true; }, now);
  now += duration;
  timers.evaluate(now);
  ASSERT_TRUE(third_timer_fired);

  // Try to reset a timer that no longer exists. This should throw an exception.
  ASSERT_THROW(timers.reset(handle, duration * 2, now), std::invalid_argument);
}

}  // namespace concordUtil
