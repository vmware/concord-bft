// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <chrono>
#include <thread>

#include "client/client_pool/client_pool_timer.hpp"
#include "gtest/gtest.h"

using concord_client_pool::Timer;

using std::chrono::milliseconds;
using std::make_shared;
using std::make_unique;
using std::shared_ptr;

using namespace std::chrono_literals;

using TestClient = shared_ptr<void>;

const milliseconds kTestTimeout = 5ms;

TEST(client_pool_timer, startCausesCallback) {
  uint16_t num_times_called = 0;
  auto timer = Timer<TestClient>(kTestTimeout, [&num_times_called](TestClient&& c) -> void { num_times_called++; });

  TestClient client1;
  timer.start(client1);

  // Note while we expect the timer to make a call back after about kTestTimeout, it makes no hard timing guarantees,
  // so we may have to wait longer sometimes.
  while (num_times_called < 1) {  // NOLINT(bugprone-infinite-loop): timer should update num_times_called
    std::this_thread::sleep_for(kTestTimeout * 2);
  }
  EXPECT_EQ(num_times_called, 1) << "Timer::start caused an unexpected number of callbacks to timeout funciton.";

  std::this_thread::sleep_for(kTestTimeout * 20);
  EXPECT_EQ(num_times_called, 1) << "Timer::start appears to have caused extraneous callback(s) after the initial one.";

  TestClient client2;
  timer.start(client2);
  while (num_times_called < 2) {  // NOLINT(bugprone-infinite-loop): timer should update num_times_called
    std::this_thread::sleep_for(kTestTimeout * 2);
  }
  EXPECT_EQ(num_times_called, 2) << "Timer::start caused an unexpected number of callbacks to timeout function.";

  TestClient client3;
  timer.start(client3);
  while (num_times_called < 3) {  // NOLINT(bugprone-infinite-loop): timer should update num_times_called
    std::this_thread::sleep_for(kTestTimeout * 2);
  }
  EXPECT_EQ(num_times_called, 3) << "Timer::start caused an unexpected number of callbacks to timeout funciton.";
}

TEST(client_pool_timer, cancelReturnsNonNegativeDuration) {
  uint16_t num_times_called = 0;
  auto timer = Timer<TestClient>(kTestTimeout, [&num_times_called](TestClient&& c) -> void { num_times_called++; });

  TestClient client;
  timer.start(client);

  // Note we do not test whether the started timeout was cancelled successfully, since Timer::cancel is not guaranteed
  // to succeed in cancelling an ongoing timeout.
  milliseconds time_to_cancellation = timer.cancel();
  EXPECT_GE(time_to_cancellation.count(), 0) << "Timer::cancel returned a negative duration.";

  time_to_cancellation = timer.cancel();
  EXPECT_GE(time_to_cancellation.count(), 0)
      << "Timer::cancel returned a negative duration when called a second time without starting a new timeout.";
}

TEST(client_pool_timer, stopTimerThreadStopsCallbacks) {
  uint16_t timer_1_num_times_called = 0;
  auto timer1 = Timer<TestClient>(kTestTimeout,
                                  [&timer_1_num_times_called](TestClient&& c) -> void { timer_1_num_times_called++; });

  timer1.stopTimerThread();

  TestClient client1;
  timer1.start(client1);
  std::this_thread::sleep_for(kTestTimeout * 20);
  EXPECT_EQ(timer_1_num_times_called, 0) << "Timer::start caused a callback after Timer::stopTimerThread completed.";

  uint16_t timer_2_num_times_called = 0;
  auto timer2 = Timer<TestClient>(kTestTimeout,
                                  [&timer_2_num_times_called](TestClient&& c) -> void { timer_2_num_times_called++; });

  TestClient client2;
  timer2.start(client2);
  timer2.stopTimerThread();
  uint16_t calls_shortly_after_stopping = timer_2_num_times_called;

  TestClient client3;
  timer2.start(client3);
  std::this_thread::sleep_for(kTestTimeout * 20);
  EXPECT_EQ(timer_2_num_times_called, calls_shortly_after_stopping)
      << "Timer::start caused a callback after Timer::stopTimerThread completed, when stopTimerThread had been called "
         "while there was potentially an ongoing timeout.";
}

TEST(client_pool_timer, startCausesCallbacksWithTheCorrectClient) {
  auto most_recent_client_called = make_shared<uint16_t>(0);
  uint16_t num_times_called = 0;
  auto timer = Timer<shared_ptr<uint16_t>>(
      kTestTimeout, [&num_times_called, &most_recent_client_called](shared_ptr<uint16_t>&& c) -> void {
        most_recent_client_called = c;
        num_times_called++;
      });

  auto client1 = make_shared<uint16_t>(1);
  timer.start(client1);

  // Note while we expect the timer to make a call back after about kTestTimeout, it makes no hard timing guarantees,
  // so we may have to wait longer sometimes.
  while (num_times_called < 1) {  // NOLINT(bugprone-infinite-loop): timer should update num_times_called
    std::this_thread::sleep_for(kTestTimeout * 2);
  }
  EXPECT_EQ(*most_recent_client_called, *client1)
      << "Timer::start did not correctly provide its ClientT parameter to the Timer's on_timeout function on callback.";

  auto client2 = make_shared<uint16_t>(2);
  timer.start(client2);
  while (num_times_called < 2) {  // NOLINT(bugprone-infinite-loop): timer should update num_times_called
    std::this_thread::sleep_for(kTestTimeout * 2);
  }
  EXPECT_EQ(*most_recent_client_called, *client2)
      << "Timer::start did not correctly provide its ClientT parameter to the Timer's on_timeout function on callback.";
}

TEST(client_pool_timer, noCallbacksCompleteAfterDestructor) {
  uint16_t num_times_called = 0;
  auto timer =
      make_unique<Timer<TestClient>>(kTestTimeout, [&num_times_called](TestClient&& c) -> void { num_times_called++; });

  TestClient client1;
  timer->start(client1);
  timer.reset();
  uint16_t calls_shortly_after_destructor = num_times_called;
  std::this_thread::sleep_for(kTestTimeout * 20);
  EXPECT_EQ(num_times_called, calls_shortly_after_destructor)
      << "A Timer object appears to have made a callback after its destructor completed.";
}

TEST(client_pool_timer, timerWith0TimeoutMakesNoCallbacks) {
  uint16_t num_times_called = 0;
  auto timer = Timer<TestClient>(0ms, [&num_times_called](TestClient&& c) -> void { num_times_called++; });

  TestClient client1;
  timer.start(client1);

  std::this_thread::sleep_for(kTestTimeout * 20);
  EXPECT_EQ(num_times_called, 0)
      << "Timer::start appears to have caused a callback for a Timer with a timeout interval of 0.";

  EXPECT_EQ(timer.cancel(), 0ms)
      << "Timer::cancel returned an unexpected value for a Timer constructed with a timeout interval of 0.";

  EXPECT_NO_THROW(timer.stopTimerThread())
      << "Timer::stopTimerThread unexpectedly threw an exception for a Timer constructed with a timeout interval of 0.";
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();
  return res;
}
