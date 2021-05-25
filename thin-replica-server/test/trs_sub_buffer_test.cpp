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
#include <future>
#include <list>
#include "Logger.hpp"
#include "thin-replica-server/subscription_buffer.hpp"

namespace {

using std::make_pair;

using concord::kvbc::categorization::ImmutableInput;
using concord::kvbc::categorization::ImmutableValueUpdate;
using concord::thin_replica::ConsumerTooSlow;
using concord::thin_replica::SubBufferList;
using concord::thin_replica::SubUpdate;
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

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto logger = logging::getLogger("trs_sub_buffer_test");
  return RUN_ALL_TESTS();
}
