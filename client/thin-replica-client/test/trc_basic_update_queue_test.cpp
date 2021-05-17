// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "client/thin-replica-client/thin_replica_client.hpp"

#include "gtest/gtest.h"

using namespace std::chrono_literals;

using std::make_unique;
using std::pair;
using std::ref;
using std::string;
using std::thread;
using std::to_string;
using std::unique_ptr;
using std::vector;
using std::chrono::milliseconds;
using std::chrono_literals::operator""ms;
using std::this_thread::sleep_for;

using thin_replica_client::BasicUpdateQueue;
using thin_replica_client::Update;

const milliseconds kBriefDelayDuration = 10ms;
const uint64_t kNumUpdatesToTest = (uint64_t)1 << 18;
const size_t kRacingThreadsToTest = 4;

unique_ptr<Update> MakeUniqueUpdate(uint64_t block_id, const vector<pair<string, string>> kv_pairs) {
  auto update = make_unique<Update>();
  update->block_id = block_id;
  update->kv_pairs = kv_pairs;
  return update;
}

namespace {

TEST(trc_basic_update_queue_test, test_constructor) {
  unique_ptr<BasicUpdateQueue> queue;
  EXPECT_NO_THROW(queue.reset(new BasicUpdateQueue())) << "BasicUpdateQueue's default constructor failed.";
}

TEST(trc_basic_update_queue_test, test_destructor) {
  auto queue = make_unique<BasicUpdateQueue>();
  EXPECT_NO_THROW(queue.reset()) << "BasicUpdateQueue's destructor failed to destruct an empty queue.";

  queue.reset(new BasicUpdateQueue());
  queue->Push(MakeUniqueUpdate(0, vector<pair<string, string>>{{"a", "a"}}));
  queue->Push(MakeUniqueUpdate(1, vector<pair<string, string>>{{"key", "value"}}));
  EXPECT_NO_THROW(queue.reset()) << "BasicUpdateQueue's destructor failed to destruct a non-empty queue.";
}

TEST(trc_basic_update_queue_test, test_release_consumers) {
  auto queue = make_unique<BasicUpdateQueue>();
  EXPECT_NO_THROW(queue->ReleaseConsumers()) << "BasicUpdateQueue::ReleaseConsumers fails when called on a queue with "
                                                "no consumer threads waiting on it.";
  queue.reset(new BasicUpdateQueue());
  thread consumer0([&]() { queue->Pop(); });
  thread consumer1([&]() { queue->Pop(); });
  sleep_for(kBriefDelayDuration);
  EXPECT_NO_THROW(queue->ReleaseConsumers()) << "BasicUpdateQueue::ReleaseConsumers fails when called on a queue with "
                                                "consumers waiting.";
  consumer0.join();
  consumer1.join();
}

TEST(trc_basic_update_queue_test, test_clear) {
  BasicUpdateQueue queue;
  EXPECT_NO_THROW(queue.Clear()) << "BasicUpdateQueue::Clear fails when called on an already-empty queue.";
  queue.Push(MakeUniqueUpdate(0, vector<pair<string, string>>{{"a", "a"}}));
  queue.Push(MakeUniqueUpdate(1, vector<pair<string, string>>{{"b", "b"}}));
  EXPECT_NO_THROW(queue.Clear()) << "BasicUpdateQueue::Clear fails when called with a non-empty queue.";
  EXPECT_EQ(queue.Size(), 0) << "A BasicUpdateQueue appears to have non-0 size after a call to Clear.";
  EXPECT_FALSE((bool)(queue.TryPop())) << "A BasicUpdateQueue appears to pop a non-null element after a call to "
                                          "Clear.";
}

TEST(trc_basic_update_queue_test, test_push) {
  BasicUpdateQueue queue;
  EXPECT_NO_THROW(queue.Push(MakeUniqueUpdate(0, vector<pair<string, string>>{})))
      << "BasicUpdateQueue::Push fails when pushing to an empty queue.";
  for (uint64_t i = 1; i <= kNumUpdatesToTest; ++i) {
    EXPECT_NO_THROW(queue.Push(MakeUniqueUpdate(i, {{"key" + to_string(i), "value" + to_string(i)}})))
        << "Push fails when pushing to a queue with " << to_string(i) << " existing updates.";
  }
}

TEST(trc_basic_update_queue_test, test_pop) {
  BasicUpdateQueue queue;
  unique_ptr<Update> update;
  thread consumer([&]() {
    EXPECT_NO_THROW(update = queue.Pop()) << "BasicUpdateQueue::Pop call initiated on an empty queue failed with "
                                             "an exception.";
    ASSERT_TRUE((bool)update) << "BasicUpdateQueue::Pop call initiated on an empty queue returned a "
                                 "null pointer in the absence of any call to ReleaseConsumers, and "
                                 "despite a subsequent push call prior to any competing Popping "
                                 "calls.";
    EXPECT_EQ(update->block_id, 0) << "BasicUpdateQueue::Pop call returned an update with a Block ID "
                                      "not "
                                      "matching the only update the Pop call should have had access to.";
    EXPECT_EQ(update->kv_pairs.size(), 0) << "BasicUpdateQueue::Pop call returned an update with a set of KV "
                                             "Pairs not matching the only update the Pop call should have had "
                                             "access to.";
  });
  sleep_for(kBriefDelayDuration);
  queue.Push(MakeUniqueUpdate(0, vector<pair<string, string>>{}));
  consumer.join();
  queue.Push(MakeUniqueUpdate(1, vector<pair<string, string>>{{"key1", "value1"}}));
  queue.Push(MakeUniqueUpdate(2, vector<pair<string, string>>{{"key2", "value2"}}));
  EXPECT_NO_THROW(update = queue.Pop()) << "BasicUpdateQueue::Pop call initiated on a queue with 2 updates "
                                           "failed with an exception.";
  EXPECT_TRUE((bool)update) << "BasicUpdateQueue::Pop call initiated on a queue with 2 updates "
                               "returned a null pointer in the absence of any ReleaseConsumers call.";
  EXPECT_NO_THROW(update = queue.Pop()) << "BasicUpdateQueue::Pop call initiated on a queue with 1 update failed "
                                           "with an exception.";
  EXPECT_TRUE((bool)update) << "BasicUpdateQueue::Pop call initiated on a queue with 1 update "
                               "returned a null pointer in the absence of any ReleaseConsumers call.";

  EXPECT_EQ(queue.Size(), 0) << "A BasicUpdateQueue was unexpectedly found to have a non-zero size "
                                "after a matching number of pushes and pops.";
  queue.Push(MakeUniqueUpdate(3, vector<pair<string, string>>{{"key3", "value3"}}));
  queue.Push(MakeUniqueUpdate(4, vector<pair<string, string>>{{"key4", "value4"}}));
  EXPECT_NO_THROW(update = queue.TryPop()) << "BasicUpdateQueue::TryPop call initiated on a queue with 2 updates "
                                              "failed with an exception.";
  EXPECT_TRUE((bool)update) << "BasicUpdateQueue::TryPop call initiated on a queue with 2 updates "
                               "returned a null pointer in the absence of any concurrent popping "
                               "calls.";
  EXPECT_NO_THROW(update = queue.TryPop()) << "BasicUpdateQueue::TryPop call initiated on a queue with 1 update "
                                              "failed with an exception.";
  EXPECT_TRUE((bool)update) << "BasicUpdateQueue::TryPop call initiated on a "
                               "queue with 1 update returned a null pointer in "
                               "the absence of any concurrent popping calls.";

  EXPECT_EQ(queue.Size(), 0) << "A BasicUpdateQueue was unexpectedly found to have a non-zero size "
                                "after a matching number of pushes and pops.";
  EXPECT_NO_THROW(update = queue.TryPop()) << "BasicUpdateQueue::TryPop call on an empty queue failed with an "
                                              "exception.";
  EXPECT_FALSE((bool)update) << "BasicUpdateQueue::TryPop call on an empty queue in the absence of "
                                "any concurrent or subsequent Push calls returned a non-null update.";
  consumer = thread([&]() {
    update = queue.Pop();
    EXPECT_FALSE((bool)update) << "BasicUpdateQueue::Pop call initiated on an empty queue followed by "
                                  "a call to ReleaseConsumers and in the absence of any concurrent or "
                                  "subsequent Push calls returned a non-null update.";
  });
  sleep_for(kBriefDelayDuration);
  queue.ReleaseConsumers();
  consumer.join();
}

TEST(test_basic_update_queue_test, test_size) {
  BasicUpdateQueue queue;
  EXPECT_EQ(queue.Size(), 0) << "BasicUpdateQueue::Size gives the wrong size for an empty queue.";
  for (int i = 0; i < kNumUpdatesToTest; ++i) {
    queue.Push(MakeUniqueUpdate(i, vector<pair<string, string>>{}));
    EXPECT_EQ(queue.Size(), (i + 1)) << "BasicUpdateQueue::Size gives the wrong size for a queue to which "
                                     << to_string(i + 1) << " updates have been pushed.";
  }
}

TEST(trc_basic_update_queue_test, test_ordering) {
  BasicUpdateQueue queue;
  thread producer([&]() {
    for (uint64_t i = 0; i < kNumUpdatesToTest; ++i) {
      EXPECT_NO_THROW(queue.Push(MakeUniqueUpdate(i, vector<pair<string, string>>{})))
          << "BasicUpdateQueue::Push call failed with an exception.";
    }
  });
  thread consumer([&]() {
    for (uint64_t i = 0; i < kNumUpdatesToTest; ++i) {
      unique_ptr<Update> update(nullptr);
      if (i % 2 == 0) {
        EXPECT_NO_THROW(update = queue.Pop()) << "BasicUpdateQueue::Pop call failed with an exception.";
        ASSERT_TRUE((bool)update) << "BasicUpdateQueue::Pop returned a null pointer in the absence "
                                     "of a concurrent or subsequent call to ReleaseConsumers.";
      } else {
        while (!update) {
          EXPECT_NO_THROW(update = queue.TryPop()) << "BasicUpdateQueue::TryPop call failed with an exception.";
        }
      }
      EXPECT_EQ(update->block_id, i) << "A BasicUpdateQueue either returned an update that returned an "
                                        "update that was never pushed to it to ordered popping calls or "
                                        "returned updates to them in an order not matching a known "
                                        "order in which those updates were pushed by ordered pushing  "
                                        "calls.";
    }
  });
  producer.join();
  consumer.join();
}

// Functions test_no_update_duplication_or_loss (below) creates threads from.
void TestNoUpdateDuplicationOrLossProducer(BasicUpdateQueue& queue, uint64_t producer_index) {
  for (uint64_t i = producer_index; i < kNumUpdatesToTest; i += kRacingThreadsToTest) {
    EXPECT_NO_THROW(queue.Push(MakeUniqueUpdate(
        i,
        vector<pair<string, string>>{{"key" + to_string(i), "value" + to_string(i)}, {"const_key", "const_value"}})));
  }
}

void TestNoUpdateDuplicationOrLossConsumer(BasicUpdateQueue& queue,
                                           vector<uint64_t>& observed_updates,
                                           uint64_t consumer_index) {
  for (uint64_t i = consumer_index; i < kNumUpdatesToTest; i += kRacingThreadsToTest) {
    unique_ptr<Update> update(nullptr);
    if (((i / kRacingThreadsToTest) % 2) == 0) {
      EXPECT_NO_THROW(update = queue.Pop()) << "BasicUpdateQueue::Pop call failed with an exception.";
      ASSERT_TRUE((bool)update) << "BasicUpdateQueue::Pop returned a null pointer in the absence of "
                                   "a concurrent or subsequent call to ReleaseConsumers.";
    } else {
      while (!update) {
        EXPECT_NO_THROW(update = queue.TryPop()) << "BasicUpdateQueue::TryPop call failed with an exception.";
      }
    }
    uint64_t block_id = update->block_id;
    observed_updates.push_back(block_id);
    EXPECT_EQ(update->kv_pairs.size(), 2) << "A BasicUpdateQueue popping call returned an update with an "
                                             "unexpected number of key value pairs";
    if (update->kv_pairs.size() == 2) {
      EXPECT_EQ(update->kv_pairs[0].first, "key" + to_string(block_id))
          << "A BasicUpdateQueue popping call returned an update containing "
             "an unexpected key (or keys in the wrong order).";
      EXPECT_EQ(update->kv_pairs[0].second, "value" + to_string(block_id))
          << "A BasicUpdateQueue popping call returned an update containing "
             "an unexpected value (or values in the wrong order).";
      EXPECT_EQ(update->kv_pairs[1].first, "const_key")
          << "A BasicUpdateQueue popping call returned an update containing "
             "an unexpected key (or keys in the wrong order).";
      EXPECT_EQ(update->kv_pairs[1].second, "const_value")
          << "A BasicUpdateQueue popping call returned an update containing "
             "an unexpected value (or values in the wrong order).";
    }
  }
}

TEST(trc_basic_update_queue_test, test_no_update_duplication_or_loss) {
  BasicUpdateQueue queue;
  vector<vector<uint64_t>> observed_updates_by_thread(kRacingThreadsToTest);
  vector<thread> threads;
  for (uint64_t i = 0; i < kRacingThreadsToTest; ++i) {
    threads.emplace_back(TestNoUpdateDuplicationOrLossProducer, ref(queue), i);
  }
  for (uint64_t i = 0; i < kRacingThreadsToTest; ++i) {
    threads.emplace_back(TestNoUpdateDuplicationOrLossConsumer, ref(queue), ref(observed_updates_by_thread[i]), i);
  }
  vector<bool> updates_observed(kNumUpdatesToTest, false);
  for (auto& thread : threads) {
    thread.join();
  }
  for (const auto& observed_by_thread : observed_updates_by_thread) {
    for (const auto& update_observed : observed_by_thread) {
      EXPECT_LT(update_observed, updates_observed.size())
          << "A BasicUpdateQueue popped an updated not matching any update "
             "that was pushed to the queue.";
      if (update_observed < updates_observed.size()) {
        EXPECT_FALSE(updates_observed[update_observed]) << "A BasicUpdateQueue appears to have duplicated an update.";
        updates_observed[update_observed] = true;
      }
    }
  }
  for (const auto& update_observed : updates_observed) {
    EXPECT_TRUE(update_observed) << "A BasicUpdateQueue appears to have lost an update.";
  }
}

}  // anonymous namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
