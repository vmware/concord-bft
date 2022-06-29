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

using namespace std::chrono_literals;

using TestClient = std::shared_ptr<void>;

TEST(client_pool_timer, work_items) {
  uint16_t num_times_called = 0;
  std::chrono::milliseconds timeout = 10ms;
  auto timer = Timer<TestClient>(timeout, [&num_times_called](TestClient&& c) -> void { num_times_called++; });

  // Wait for timeout
  TestClient client1;
  timer.start(client1);
  std::this_thread::sleep_for(timeout * 2);
  ASSERT_EQ(num_times_called, 1);

  // Cancel timeout
  TestClient client2;
  timer.start(client2);
  timer.cancel();
  ASSERT_EQ(num_times_called, 1);

  // Wait for timeout
  TestClient client3;
  timer.start(client3);
  std::this_thread::sleep_for(timeout * 2);
  ASSERT_EQ(num_times_called, 2);

  // Wait for timeout
  TestClient client4;
  timer.start(client4);
  std::this_thread::sleep_for(timeout * 2);
  ASSERT_EQ(num_times_called, 3);

  // Stop timer thread
  TestClient client5;
  timer.start(client5);
  timer.stopTimerThread();
  ASSERT_EQ(num_times_called, 3);

  // Starting new timer won't work because the thread is stopped
  TestClient client6;
  timer.start(client6);
  std::this_thread::sleep_for(timeout * 2);
  ASSERT_EQ(num_times_called, 3);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();
  return res;
}
