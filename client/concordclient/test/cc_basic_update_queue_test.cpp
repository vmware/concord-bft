// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "client/concordclient/remote_update_queue.hpp"
#include "client/concordclient/concord_client_exceptions.hpp"

#include <thread>
#include <chrono>
#include <vector>
#include <memory>
#include <string>

#include "gtest/gtest.h"

using std::make_unique;
using std::pair;
using std::ref;
using std::string;
using std::thread;
using std::to_string;
using std::unique_ptr;
using std::vector;
using std::chrono::milliseconds;
using std::this_thread::sleep_for;

using namespace std::chrono_literals;

using concord::client::concordclient::BasicUpdateQueue;
using concord::client::concordclient::RemoteData;
using concord::client::concordclient::Update;
using concord::client::concordclient::OutOfRangeSubscriptionRequest;

const milliseconds kBriefDelayDuration = 10ms;
const uint64_t kNumUpdatesToTest = (uint64_t)1 << 18;
const size_t kRacingThreadsToTest = 4;

unique_ptr<RemoteData> MakeUniqueUpdate(uint64_t block_id, const vector<pair<string, string>>& kv_pairs) {
  auto update = make_unique<RemoteData>();
  auto& legacy_event = std::get<Update>(*update);
  legacy_event.block_id = block_id;
  legacy_event.kv_pairs = kv_pairs;
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
  queue->push(MakeUniqueUpdate(0, vector<pair<string, string>>{{"a", "a"}}));
  queue->push(MakeUniqueUpdate(1, vector<pair<string, string>>{{"key", "value"}}));
  EXPECT_NO_THROW(queue.reset()) << "BasicUpdateQueue's destructor failed to destruct a non-empty queue.";
}

TEST(trc_basic_update_queue_test, test_release_consumers) {
  auto queue = make_unique<BasicUpdateQueue>();
  EXPECT_NO_THROW(queue->releaseConsumers()) << "BasicUpdateQueue::releaseConsumers fails when called on a queue with "
                                                "no consumer threads waiting on it.";
  queue.reset(new BasicUpdateQueue());
  thread consumer0([&]() { queue->pop(); });
  thread consumer1([&]() { queue->pop(); });
  sleep_for(kBriefDelayDuration);
  EXPECT_NO_THROW(queue->releaseConsumers()) << "BasicUpdateQueue::releaseConsumers fails when called on a queue with "
                                                "consumers waiting.";
  consumer0.join();
  consumer1.join();
}

TEST(trc_basic_update_queue_test, test_pop_with_exception) {
  auto queue = make_unique<BasicUpdateQueue>();
  auto test_exception = std::make_exception_ptr(OutOfRangeSubscriptionRequest());
  queue->push(MakeUniqueUpdate(1, vector<pair<string, string>>{{"key1", "value1"}}));
  queue->push(MakeUniqueUpdate(2, vector<pair<string, string>>{{"key2", "value2"}}));
  queue->pop();
  EXPECT_NO_THROW(queue->setException(test_exception));
  EXPECT_THROW(queue->pop();, OutOfRangeSubscriptionRequest);
  EXPECT_NO_THROW(queue->pop()) << "BasicUpdateQueue::pop call failed with an exception.";
}

TEST(trc_basic_update_queue_test, test_try_pop_with_exception) {
  auto queue = make_unique<BasicUpdateQueue>();
  auto test_exception = std::make_exception_ptr(OutOfRangeSubscriptionRequest());
  queue->push(MakeUniqueUpdate(1, vector<pair<string, string>>{{"key1", "value1"}}));
  queue->push(MakeUniqueUpdate(2, vector<pair<string, string>>{{"key2", "value2"}}));
  queue->tryPop();
  EXPECT_NO_THROW(queue->setException(test_exception));
  EXPECT_THROW(queue->tryPop();, OutOfRangeSubscriptionRequest);
  EXPECT_NO_THROW(queue->tryPop()) << "BasicUpdateQueue::tryPop call failed with an exception.";
}

TEST(trc_basic_update_queue_test, test_clear) {
  BasicUpdateQueue queue;
  EXPECT_NO_THROW(queue.clear()) << "BasicUpdateQueue::clear fails when called on an already-empty queue.";
  queue.push(MakeUniqueUpdate(0, vector<pair<string, string>>{{"a", "a"}}));
  queue.push(MakeUniqueUpdate(1, vector<pair<string, string>>{{"b", "b"}}));
  EXPECT_NO_THROW(queue.clear()) << "BasicUpdateQueue::clear fails when called with a non-empty queue.";
  EXPECT_EQ(queue.size(), 0) << "A BasicUpdateQueue appears to have non-0 size after a call to clear.";
  EXPECT_FALSE((bool)(queue.tryPop())) << "A BasicUpdateQueue appears to pop a non-null element after a call to "
                                          "clear.";
}

TEST(trc_basic_update_queue_test, test_push) {
  BasicUpdateQueue queue;
  EXPECT_NO_THROW(queue.push(MakeUniqueUpdate(0, vector<pair<string, string>>{})))
      << "BasicUpdateQueue::push fails when pushing to an empty queue.";
  for (uint64_t i = 1; i <= kNumUpdatesToTest; ++i) {
    EXPECT_NO_THROW(queue.push(MakeUniqueUpdate(i, {{"key" + to_string(i), "value" + to_string(i)}})))
        << "push fails when pushing to a queue with " << to_string(i) << " existing updates.";
  }
}

TEST(trc_basic_update_queue_test, test_pop) {
  BasicUpdateQueue queue;
  unique_ptr<RemoteData> update;
  thread consumer([&]() {
    EXPECT_NO_THROW(update = queue.pop()) << "BasicUpdateQueue::pop call initiated on an empty queue failed with "
                                             "an exception.";
    ASSERT_TRUE((bool)update) << "BasicUpdateQueue::pop call initiated on an empty queue returned a "
                                 "null pointer in the absence of any call to releaseConsumers, and "
                                 "despite a subsequent push call prior to any competing popping "
                                 "calls.";
    auto& legacy_event = std::get<Update>(*update);
    EXPECT_EQ(legacy_event.block_id, 0) << "BasicUpdateQueue::pop call returned an update with a Block ID "
                                           "not "
                                           "matching the only update the pop call should have had access to.";
    EXPECT_EQ(legacy_event.kv_pairs.size(), 0) << "BasicUpdateQueue::pop call returned an update with a set of KV "
                                                  "Pairs not matching the only update the pop call should have had "
                                                  "access to.";
  });
  sleep_for(kBriefDelayDuration);
  queue.push(MakeUniqueUpdate(0, vector<pair<string, string>>{}));
  consumer.join();
  queue.push(MakeUniqueUpdate(1, vector<pair<string, string>>{{"key1", "value1"}}));
  queue.push(MakeUniqueUpdate(2, vector<pair<string, string>>{{"key2", "value2"}}));
  EXPECT_NO_THROW(update = queue.pop()) << "BasicUpdateQueue::pop call initiated on a queue with 2 updates "
                                           "failed with an exception.";
  EXPECT_TRUE((bool)update) << "BasicUpdateQueue::pop call initiated on a queue with 2 updates "
                               "returned a null pointer in the absence of any releaseConsumers call.";
  EXPECT_NO_THROW(update = queue.pop()) << "BasicUpdateQueue::pop call initiated on a queue with 1 update failed "
                                           "with an exception.";
  EXPECT_TRUE((bool)update) << "BasicUpdateQueue::pop call initiated on a queue with 1 update "
                               "returned a null pointer in the absence of any releaseConsumers call.";

  EXPECT_EQ(queue.size(), 0) << "A BasicUpdateQueue was unexpectedly found to have a non-zero size "
                                "after a matching number of pushes and pops.";
  queue.push(MakeUniqueUpdate(3, vector<pair<string, string>>{{"key3", "value3"}}));
  queue.push(MakeUniqueUpdate(4, vector<pair<string, string>>{{"key4", "value4"}}));
  EXPECT_NO_THROW(update = queue.tryPop()) << "BasicUpdateQueue::tryPop call initiated on a queue with 2 updates "
                                              "failed with an exception.";
  EXPECT_TRUE((bool)update) << "BasicUpdateQueue::tryPop call initiated on a queue with 2 updates "
                               "returned a null pointer in the absence of any concurrent popping "
                               "calls.";
  EXPECT_NO_THROW(update = queue.tryPop()) << "BasicUpdateQueue::tryPop call initiated on a queue with 1 update "
                                              "failed with an exception.";
  EXPECT_TRUE((bool)update) << "BasicUpdateQueue::tryPop call initiated on a "
                               "queue with 1 update returned a null pointer in "
                               "the absence of any concurrent popping calls.";

  EXPECT_EQ(queue.size(), 0) << "A BasicUpdateQueue was unexpectedly found to have a non-zero size "
                                "after a matching number of pushes and pops.";
  EXPECT_NO_THROW(update = queue.tryPop()) << "BasicUpdateQueue::tryPop call on an empty queue failed with an "
                                              "exception.";
  EXPECT_FALSE((bool)update) << "BasicUpdateQueue::tryPop call on an empty queue in the absence of "
                                "any concurrent or subsequent push calls returned a non-null update.";
  consumer = thread([&]() {
    update = queue.pop();
    EXPECT_FALSE((bool)update) << "BasicUpdateQueue::pop call initiated on an empty queue followed by "
                                  "a call to releaseConsumers and in the absence of any concurrent or "
                                  "subsequent push calls returned a non-null update.";
  });
  sleep_for(kBriefDelayDuration);
  queue.releaseConsumers();
  consumer.join();
}

TEST(test_basic_update_queue_test, test_size) {
  BasicUpdateQueue queue;
  EXPECT_EQ(queue.size(), 0) << "BasicUpdateQueue::size gives the wrong size for an empty queue.";
  for (uint64_t i = 0; i < kNumUpdatesToTest; ++i) {
    queue.push(MakeUniqueUpdate(i, vector<pair<string, string>>{}));
    EXPECT_EQ(queue.size(), (i + 1)) << "BasicUpdateQueue::size gives the wrong size for a queue to which "
                                     << to_string(i + 1) << " updates have been pushed.";
  }
}

TEST(trc_basic_update_queue_test, test_ordering) {
  BasicUpdateQueue queue;
  thread producer([&]() {
    for (uint64_t i = 0; i < kNumUpdatesToTest; ++i) {
      EXPECT_NO_THROW(queue.push(MakeUniqueUpdate(i, vector<pair<string, string>>{})))
          << "BasicUpdateQueue::push call failed with an exception.";
    }
  });
  thread consumer([&]() {
    for (uint64_t i = 0; i < kNumUpdatesToTest; ++i) {
      unique_ptr<RemoteData> update(nullptr);
      if (i % 2 == 0) {
        EXPECT_NO_THROW(update = queue.pop()) << "BasicUpdateQueue::pop call failed with an exception.";
        ASSERT_TRUE((bool)update) << "BasicUpdateQueue::pop returned a null pointer in the absence "
                                     "of a concurrent or subsequent call to releaseConsumers.";
      } else {
        while (!update) {
          EXPECT_NO_THROW(update = queue.tryPop()) << "BasicUpdateQueue::tryPop call failed with an exception.";
        }
      }
      EXPECT_EQ(std::get<Update>(*update).block_id, i)
          << "A BasicUpdateQueue either returned an update that returned an "
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
    EXPECT_NO_THROW(queue.push(MakeUniqueUpdate(
        i,
        vector<pair<string, string>>{{"key" + to_string(i), "value" + to_string(i)}, {"const_key", "const_value"}})));
  }
}

void TestNoUpdateDuplicationOrLossConsumer(BasicUpdateQueue& queue,
                                           vector<uint64_t>& observed_updates,
                                           uint64_t consumer_index) {
  for (uint64_t i = consumer_index; i < kNumUpdatesToTest; i += kRacingThreadsToTest) {
    unique_ptr<RemoteData> update(nullptr);
    if (((i / kRacingThreadsToTest) % 2) == 0) {
      EXPECT_NO_THROW(update = queue.pop()) << "BasicUpdateQueue::pop call failed with an exception.";
      ASSERT_TRUE((bool)update) << "BasicUpdateQueue::pop returned a null pointer in the absence of "
                                   "a concurrent or subsequent call to releaseConsumers.";
    } else {
      while (!update) {
        EXPECT_NO_THROW(update = queue.tryPop()) << "BasicUpdateQueue::tryPop call failed with an exception.";
      }
    }
    auto& legacy_event = std::get<Update>(*update);
    uint64_t block_id = legacy_event.block_id;
    observed_updates.push_back(block_id);
    EXPECT_EQ(legacy_event.kv_pairs.size(), 2) << "A BasicUpdateQueue popping call returned an update with an "
                                                  "unexpected number of key value pairs";
    if (legacy_event.kv_pairs.size() == 2) {
      EXPECT_EQ(legacy_event.kv_pairs[0].first, "key" + to_string(block_id))
          << "A BasicUpdateQueue popping call returned an update containing "
             "an unexpected key (or keys in the wrong order).";
      EXPECT_EQ(legacy_event.kv_pairs[0].second, "value" + to_string(block_id))
          << "A BasicUpdateQueue popping call returned an update containing "
             "an unexpected value (or values in the wrong order).";
      EXPECT_EQ(legacy_event.kv_pairs[1].first, "const_key")
          << "A BasicUpdateQueue popping call returned an update containing "
             "an unexpected key (or keys in the wrong order).";
      EXPECT_EQ(legacy_event.kv_pairs[1].second, "const_value")
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
