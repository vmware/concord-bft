// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <future>
#include <list>

#include "log/logger.hpp"
#include "thin-replica-server/subscription_buffer.hpp"

using namespace std::chrono_literals;

namespace {

using concord::kvbc::categorization::ImmutableInput;
using concord::kvbc::categorization::ImmutableValueUpdate;
using concord::kvbc::categorization::EventGroup;
using concord::kvbc::categorization::Event;
using concord::thin_replica::ConsumerTooSlow;
using concord::thin_replica::SubBufferList;
using concord::thin_replica::SubUpdate;
using concord::thin_replica::SubEventGroupUpdate;
using concord::thin_replica::SubUpdateBuffer;

// A producer should be able to "add" updates whether there are consumers or
// not. Meaning, the producer does not get interrupted/disturbed if no
// consumers are present.
TEST(trs_sub_buffer_test, no_consumer) {
  SubBufferList sub_list;
  ImmutableInput input;
  ImmutableValueUpdate val;
  val.data = "value";
  input.kv = {{"key", val}};
  SubUpdate update{1337, "CID", input};
  for (unsigned i = 0; i < 100; ++i) {
    EXPECT_NO_THROW(sub_list.updateSubBuffers(update));
  }
}

TEST(trs_sub_buffer_test, no_consumer_eg) {
  SubBufferList sub_list;
  EventGroup event_group;
  Event event;
  event.data = "value";
  event.tags = {"tag1", "tag2"};
  event_group.events.emplace_back(event);
  SubEventGroupUpdate update{1337, event_group};
  for (unsigned i = 0; i < 100; ++i) {
    EXPECT_NO_THROW(sub_list.updateEventGroupSubBuffers(update));
  }
}

TEST(trs_sub_buffer_test, subscriber_size) {
  SubBufferList sub_list;

  ASSERT_EQ(sub_list.Size(), 0);

  std::list<std::shared_ptr<SubUpdateBuffer>> sub_buffers;
  for (int i = 1; i <= 64; ++i) {
    sub_buffers.push_back(std::make_shared<SubUpdateBuffer>(1));
    sub_list.addBuffer(sub_buffers.back());
    ASSERT_EQ(sub_list.Size(), i);
  }
  for (int i = 64; i > 0; --i) {
    sub_list.removeBuffer(sub_buffers.front());
    sub_buffers.pop_front();
    ASSERT_EQ(sub_list.Size(), i - 1);
  }
}

// A producer should be able to "add" an update even if the underlying queue is
// full. Practically, this means that an update gets dropped. This will result
// in updates with gaps for the consumer. Eventually, the TRC will receive this
// gap and terminate the subscription due to a mismatch. Test that we throw an
// exception to inform the consumer.
TEST(trs_sub_buffer_test, throw_exception_if_consumer_too_slow) {
  SubBufferList sub_list;
  ImmutableInput input;
  ImmutableValueUpdate val;
  val.data = "value";
  input.kv = {{"key", val}};
  SubUpdate update{1337, "CID", input};
  auto updates = std::make_shared<SubUpdateBuffer>(10);

  sub_list.addBuffer(updates);
  for (unsigned i = 0; i < 11; ++i) {
    sub_list.updateSubBuffers(update);
  }

  SubUpdate consumer_update;
  EXPECT_THROW(updates->Pop(consumer_update), ConsumerTooSlow);
}

TEST(trs_sub_buffer_test, throw_exception_if_consumer_too_slow_eg) {
  SubBufferList sub_list;
  EventGroup event_group;
  Event event;
  event.data = "value";
  event.tags = {"tag1", "tag2"};
  event_group.events.emplace_back(event);
  SubEventGroupUpdate update{1337, event_group};
  auto updates = std::make_shared<SubUpdateBuffer>(10);

  sub_list.addBuffer(updates);
  for (unsigned i = 0; i < 11; ++i) {
    sub_list.updateEventGroupSubBuffers(update);
  }

  SubEventGroupUpdate consumer_update;
  EXPECT_THROW(updates->PopEventGroup(consumer_update), ConsumerTooSlow);
}

TEST(trs_sub_buffer_test, block_consumer_if_no_updates_available) {
  SubBufferList sub_list;
  std::atomic_bool reader_started;
  auto updates = std::make_shared<SubUpdateBuffer>(10);
  auto reader = std::async(std::launch::async, [&] {
    SubUpdate update;
    reader_started = true;
    updates->Pop(update);
    ASSERT_EQ(update.block_id, 1337);
  });

  sub_list.addBuffer(updates);

  // Let's make sure that the reader thread starts first
  while (!reader_started)
    ;

  ImmutableInput input;
  ImmutableValueUpdate val;
  val.data = "value";
  input.kv = {{"key", val}};
  SubUpdate update{1337, "CID", input};
  sub_list.updateSubBuffers(update);
}

TEST(trs_sub_buffer_test, block_consumer_if_no_updates_available_eg) {
  SubBufferList sub_list;
  std::atomic_bool reader_started;
  auto updates = std::make_shared<SubUpdateBuffer>(10);
  auto reader = std::async(std::launch::async, [&] {
    SubEventGroupUpdate update;
    reader_started = true;
    updates->PopEventGroup(update);
    ASSERT_EQ(update.event_group_id, 1337);
  });

  sub_list.addBuffer(updates);

  // Let's make sure that the reader thread starts first
  while (!reader_started)
    ;

  EventGroup event_group;
  Event event;
  event.data = "value";
  event.tags = {"tag1", "tag2"};
  event_group.events.emplace_back(event);
  SubEventGroupUpdate update{1337, event_group};
  sub_list.updateEventGroupSubBuffers(update);
}

TEST(trs_sub_buffer_test, happy_path_w_two_consumers) {
  SubBufferList sub_list;
  ImmutableInput input;
  ImmutableValueUpdate val;
  val.data = "value";
  input.kv = {{"key", val}};
  SubUpdate update{0, "CID", input};
  auto updates1 = std::make_shared<SubUpdateBuffer>(10);
  auto updates2 = std::make_shared<SubUpdateBuffer>(10);
  int num_updates = 10;

  auto reader_fn = [](std::shared_ptr<SubUpdateBuffer> q, int max) {
    SubUpdate update;
    int counter = 0;
    do {
      q->Pop(update);
      ASSERT_EQ(update.block_id, counter);
    } while (++counter < max);
  };
  auto reader1 = std::async(std::launch::async, reader_fn, updates1, num_updates);
  auto reader2 = std::async(std::launch::async, reader_fn, updates2, num_updates);

  sub_list.addBuffer(updates1);
  sub_list.addBuffer(updates2);

  for (int i = 0; i < num_updates; ++i) {
    update.block_id = i;
    sub_list.updateSubBuffers(update);
  }
}

TEST(trs_sub_buffer_test, happy_path_w_two_consumers_eg) {
  SubBufferList sub_list;
  EventGroup event_group;
  Event event;
  event.data = "value";
  event.tags = {"tag1", "tag2"};
  event_group.events.emplace_back(event);
  SubEventGroupUpdate update{0, event_group};
  auto updates1 = std::make_shared<SubUpdateBuffer>(10);
  auto updates2 = std::make_shared<SubUpdateBuffer>(10);
  int num_updates = 10;

  auto reader_fn = [](std::shared_ptr<SubUpdateBuffer> q, int max) {
    SubEventGroupUpdate update_;
    int counter = 0;
    do {
      q->PopEventGroup(update_);
      ASSERT_EQ(update_.event_group_id, counter);
    } while (++counter < max);
  };
  auto reader1 = std::async(std::launch::async, reader_fn, updates1, num_updates);
  auto reader2 = std::async(std::launch::async, reader_fn, updates2, num_updates);

  sub_list.addBuffer(updates1);
  sub_list.addBuffer(updates2);

  for (int i = 0; i < num_updates; ++i) {
    update.event_group_id = i;
    sub_list.updateEventGroupSubBuffers(update);
  }
}

TEST(trs_sub_buffer_test, waiting_for_updates) {
  auto updates = std::make_shared<SubUpdateBuffer>(10);
  auto updates_eg = std::make_shared<SubUpdateBuffer>(10);

  SubUpdate out;
  SubEventGroupUpdate out_eg;

  SubBufferList sub_list;
  sub_list.addBuffer(updates);
  sub_list.addBuffer(updates_eg);

  ASSERT_FALSE(updates->waitUntilNonEmpty(10ms));
  ASSERT_FALSE(updates_eg->waitForEventGroupUntilNonEmpty(10ms));
  ASSERT_FALSE(updates->TryPop(out, 10ms));
  ASSERT_FALSE(updates_eg->TryPopEventGroup(out_eg, 10ms));

  std::atomic_bool reader_started;
  auto reader = std::async(std::launch::async, [&] {
    reader_started = true;

    updates->waitUntilNonEmpty();
    ASSERT_FALSE(updates->Empty());
    ASSERT_TRUE(updates->waitUntilNonEmpty(10ms));

    updates_eg->waitForEventGroupUntilNonEmpty();
    ASSERT_FALSE(updates_eg->Empty());
    ASSERT_TRUE(updates_eg->waitForEventGroupUntilNonEmpty(10ms));

    updates->Pop(out);
    ASSERT_TRUE(updates->TryPop(out, 10ms));
    updates_eg->PopEventGroup(out_eg);
    ASSERT_TRUE(updates_eg->TryPopEventGroup(out_eg, 10ms));
  });

  // Let's make sure that the reader thread starts first
  while (!reader_started)
    ;

  SubUpdate update{};
  SubEventGroupUpdate update_eg{};

  // The reader is calling pop twice for each update type
  sub_list.updateSubBuffers(update);
  sub_list.updateSubBuffers(update);
  sub_list.updateEventGroupSubBuffers(update_eg);
  sub_list.updateEventGroupSubBuffers(update_eg);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
