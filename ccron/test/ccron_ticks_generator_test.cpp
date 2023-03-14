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
#include "ccron/ticks_generator.hpp"
#include "Replica.hpp"
#include "ticks_generator_mocks.hpp"

#include <iterator>
#include <memory>
#include <utility>
#include <variant>
namespace {

using namespace concord::cron;
using namespace std::chrono_literals;
using bftEngine::impl::TickInternalMsg;
using bftEngine::TICK_FLAG;

class ccron_ticks_generator_test : public ::testing::Test {
 protected:
  const std::uint32_t kComponentId1 = 1;
  const std::uint32_t kComponentId2 = 2;

  std::shared_ptr<test::InternalBFTClientMock> bft_client_ = std::make_shared<test::InternalBFTClientMock>();
  test::PendingRequestMock pending_req_;
  std::shared_ptr<test::IncomingMsgsStorageMock> msgs_ = std::make_shared<test::IncomingMsgsStorageMock>();
  std::shared_ptr<test::TicksGeneratorForTest> gen_ =
      test::TicksGeneratorForTest::create(bft_client_, pending_req_, msgs_);

  static std::chrono::steady_clock::time_point now() { return std::chrono::steady_clock::now(); }
};

TEST_F(ccron_ticks_generator_test, no_ticks_if_not_started) {
  gen_->evaluateTimers(now() + 10s);
  gen_->evaluateTimers(now() + 11s);

  ASSERT_TRUE(msgs_->internal_msgs_.empty());
}

TEST_F(ccron_ticks_generator_test, internal_tick_msg_is_generated) {
  gen_->start(kComponentId1, 1s);
  gen_->start(kComponentId2, 2s);

  gen_->evaluateTimers(now() + 1s);
  gen_->evaluateTimers(now() + 2s);

  // Expect the following order: Tick{kComponentId1}, Tick{kComponentId1}, Tick{kComponentId2}
  ASSERT_EQ(3, msgs_->internal_msgs_.size());
  ASSERT_TRUE(std::holds_alternative<TickInternalMsg>(msgs_->internal_msgs_[0]));
  ASSERT_EQ(kComponentId1, std::get<TickInternalMsg>(msgs_->internal_msgs_[0]).component_id);

  ASSERT_TRUE(std::holds_alternative<TickInternalMsg>(msgs_->internal_msgs_[1]));
  ASSERT_EQ(kComponentId1, std::get<TickInternalMsg>(msgs_->internal_msgs_[1]).component_id);

  ASSERT_TRUE(std::holds_alternative<TickInternalMsg>(msgs_->internal_msgs_[2]));
  ASSERT_EQ(kComponentId2, std::get<TickInternalMsg>(msgs_->internal_msgs_[2]).component_id);
}

TEST_F(ccron_ticks_generator_test, start_updates_if_called_again) {
  gen_->start(kComponentId1, 1s);

  // Should generate a tick.
  gen_->evaluateTimers(now() + 1s);
  ASSERT_EQ(1, msgs_->internal_msgs_.size());

  // Update the period to 4s.
  gen_->start(kComponentId1, 4s);

  // Should not generate a tick as we have updated the period.
  gen_->evaluateTimers(now() + 2s);
  gen_->evaluateTimers(now() + 3s);
  ASSERT_EQ(1, msgs_->internal_msgs_.size());

  // Should generate a tick with the new period.
  gen_->evaluateTimers(now() + 4s);
  ASSERT_EQ(2, msgs_->internal_msgs_.size());

  // Should not generate a tick.
  gen_->evaluateTimers(now() + 7s);
  ASSERT_EQ(2, msgs_->internal_msgs_.size());

  // Should generate a tick.
  gen_->evaluateTimers(now() + 8s);
  ASSERT_EQ(3, msgs_->internal_msgs_.size());
}

TEST_F(ccron_ticks_generator_test, stop_tick) {
  gen_->start(kComponentId1, 1s);
  gen_->start(kComponentId2, 2s);

  // Evaluate twice and fire kComponentId1 twice and kComponentId2 once.
  gen_->evaluateTimers(now() + 1s);
  gen_->evaluateTimers(now() + 2s);
  ASSERT_EQ(3, msgs_->internal_msgs_.size());

  // Stop kComponentId1 and evaluate at 2s in the future, expecting that kComponentId2 will fire once more.
  gen_->stop(kComponentId1);
  gen_->evaluateTimers(now() + 4s);
  ASSERT_EQ(4, msgs_->internal_msgs_.size());

  // Stop kComponentId2 and expect that no timer will fire further.
  gen_->stop(kComponentId2);
  gen_->evaluateTimers(now() + 6s);
  gen_->evaluateTimers(now() + 8s);
  ASSERT_EQ(4, msgs_->internal_msgs_.size());
}

TEST_F(ccron_ticks_generator_test, is_generating) {
  ASSERT_FALSE(gen_->isGenerating(kComponentId1));
  gen_->start(kComponentId1, 1s);
  ASSERT_TRUE(gen_->isGenerating(kComponentId1));
  gen_->stop(kComponentId1);
  ASSERT_FALSE(gen_->isGenerating(kComponentId1));
}

TEST_F(ccron_ticks_generator_test, is_start_0_stopping) {
  ASSERT_FALSE(gen_->isGenerating(kComponentId1));
  gen_->start(kComponentId1, 1s);
  ASSERT_TRUE(gen_->isGenerating(kComponentId1));
  gen_->start(kComponentId1, 0s);
  ASSERT_FALSE(gen_->isGenerating(kComponentId1));
}

TEST_F(ccron_ticks_generator_test, client_request_msg_is_generated) {
  // Generate an internal tick.
  gen_->onInternalTick({kComponentId1});

  // Make sure we have converted the internal tick into a ClientRequestMsg tick.
  ASSERT_EQ(1, bft_client_->requests_.size());
  const auto seq_num1 = bft_client_->requests_.begin()->first;
  const auto request1 = bft_client_->requests_.begin()->second;
  ASSERT_EQ(TICK_FLAG, request1.flags);
  ASSERT_EQ(TicksGenerator::kTickCid, request1.cid);
  auto payload1 = ClientReqMsgTickPayload{};
  deserialize(request1.contents, payload1);
  ASSERT_EQ(kComponentId1, payload1.component_id);

  // Make sure that we don't generate more ClientRequestMsg ticks until the one for component ID 1 is in the external
  // message queue.
  gen_->onInternalTick({kComponentId1});
  ASSERT_EQ(1, bft_client_->requests_.size());
  ASSERT_EQ(request1, bft_client_->requests_[0]);

  // Simulate the replica consuming the tick in the external message queue by:
  //  - "removing" the tick from the external message queue and verifying no tick is added
  //  - calling the onMessage<ClientRequestMsg>() method and marking the request pending in ReplicaImp
  // At the end, verify no ClientRequestMsg ticks are generated.
  gen_->onTickPoppedFromExtQueue(kComponentId1);
  pending_req_.addPending(bft_client_->getClientId(), seq_num1);
  ASSERT_TRUE(pending_req_.isPending(bft_client_->getClientId(), seq_num1));
  ASSERT_EQ(1, bft_client_->requests_.size());
  ASSERT_EQ(request1, bft_client_->requests_[0]);

  // ClientRequestMsg ticks for another component are generated.
  gen_->onInternalTick({kComponentId2});
  ASSERT_EQ(2, bft_client_->requests_.size());
  const auto seq_num2 = std::prev(bft_client_->requests_.cend())->first;
  const auto request2 = std::prev(bft_client_->requests_.cend())->second;
  ASSERT_EQ(TICK_FLAG, request2.flags);
  ASSERT_EQ(TicksGenerator::kTickCid, request2.cid);
  ASSERT_EQ(seq_num2, seq_num1 + 1);
  auto payload2 = ClientReqMsgTickPayload{};
  deserialize(request2.contents, payload2);
  ASSERT_EQ(kComponentId2, payload2.component_id);

  // Again, make sure that we don't generate more ClientRequestMsg ticks until the one for component ID 1 is pending,
  // even after ticks for another component ID have been generated.
  gen_->onInternalTick({kComponentId1});
  ASSERT_EQ(2, bft_client_->requests_.size());

  // Simulate that a successful ClientRequestMsg tick has been committed.
  pending_req_.removePending(bft_client_->getClientId(), seq_num1);
  ASSERT_FALSE(pending_req_.isPending(bft_client_->getClientId(), seq_num1));

  // Generate a second internal tick for component ID 1.
  gen_->onInternalTick({kComponentId1});
  ASSERT_EQ(3, bft_client_->requests_.size());
  const auto seq_num3 = std::prev(bft_client_->requests_.cend())->first;
  const auto request3 = std::prev(bft_client_->requests_.cend())->second;
  ASSERT_EQ(TICK_FLAG, request3.flags);
  ASSERT_EQ(TicksGenerator::kTickCid, request3.cid);
  ASSERT_EQ(seq_num3, seq_num2 + 1);
  auto payload3 = ClientReqMsgTickPayload{};
  deserialize(request3.contents, payload3);
  ASSERT_EQ(kComponentId1, payload3.component_id);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
