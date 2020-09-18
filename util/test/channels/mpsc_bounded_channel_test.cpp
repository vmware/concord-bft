
// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <thread>
#include "gtest/gtest.h"
#include "channels/mpsc_bounded_channel.h"

using namespace std::literals::chrono_literals;

#include <iostream>
using namespace std;

namespace concord::channel {

TEST(ChannelsTest, same_thread_no_contention) {
  auto [sender, receiver] = makeBoundedMpscChannel<int>(2);

  // Sends 1 and 2 are successful
  ASSERT_EQ(std::nullopt, sender.send(1));
  ASSERT_EQ(std::nullopt, sender.send(2));

  // A third send should fail due to a full buffer and return the message
  ASSERT_EQ(3, sender.send(3).value());

  // Receiving one value makes room for another
  ASSERT_EQ(1, receiver.recv());
  ASSERT_EQ(std::nullopt, sender.send(4));

  // We should be able to pull 2 values off the channel
  ASSERT_EQ(2, receiver.recv());
  ASSERT_EQ(4, receiver.recv());

  // A third pull should timeout
  ASSERT_EQ(std::nullopt, receiver.recv(1ms));

  // Adding another value will make it available.
  ASSERT_EQ(std::nullopt, sender.send(5));
  ASSERT_EQ(5, receiver.recv(1ms).value());
}

TEST(ChannelsTest, two_threads_one_sender_one_receiver) {
  auto [sender, receiver] = makeBoundedMpscChannel<int>(2);

  // Add a value before we start the receiving thread
  ASSERT_EQ(std::nullopt, sender.send(1));

  auto thread = std::thread([receiver = std::move(receiver)]() mutable {
    ASSERT_EQ(1, receiver.recv());
    ASSERT_EQ(2, receiver.recv());
    ASSERT_EQ(3, receiver.recv());
    ASSERT_EQ(4, receiver.recv());
    ASSERT_THROW(receiver.recv(), NoSendersError);
  });

  ASSERT_EQ(std::nullopt, sender.send(2));

  // Loop until the receive thread has pulled this value off the queue.
  while (sender.send(3).has_value())
    ;

  // Move the sender into a new scope so that it gets destroyed. Then the receiver should see a
  // NoSendersError.
  {
    auto s = std::move(sender);
    // There's a race here. We need to ensure the value gets sent.
    while (s.send(4).has_value())
      ;
  }

  thread.join();
}

TEST(ChannelsTest, multiple_senders) {
  auto [sender, receiver] = makeBoundedMpscChannel<size_t>(2);
  auto receive_thread = std::thread([receiver = std::move(receiver)]() mutable {
    // All received values are indexes into the received vector
    std::vector<bool> received(5, false);
    for (auto i = 0; i < 5; i++) {
      received[receiver.recv(1s).value()] = true;
    }
    for (const auto& val : received) {
      ASSERT_TRUE(val);
    }
  });

  auto send_thread_1 = std::thread([sender = sender]() mutable {
    while (sender.send(0).has_value())
      ;
    while (sender.send(1).has_value())
      ;
  });
  auto send_thread_2 = std::thread([sender = sender]() mutable {
    while (sender.send(2).has_value())
      ;
    while (sender.send(3).has_value())
      ;
    while (sender.send(4).has_value())
      ;
  });

  receive_thread.join();

  // Receiver should be destroyed here.
  ASSERT_THROW(sender.send(5), NoReceiversError);

  send_thread_1.join();
  send_thread_2.join();
}

}  // namespace concord::channel
