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

#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace {

using namespace concord::cron;
using namespace std::chrono_literals;
using bftEngine::impl::TickInternalMsg;
using bftEngine::TICK_FLAG;

struct IncomingMsgsStorageMock : public IncomingMsgsStorage {
  void start() override{};
  void stop() override{};

  bool isRunning() const override { return true; };

  void pushExternalMsg(std::unique_ptr<MessageBase> msg) override {}
  void pushExternalMsgRaw(char* msg, size_t& size) override {}
  void pushInternalMsg(InternalMessage&& msg) override { internal_msgs_.emplace_back(std::move(msg)); }

  std::vector<InternalMessage> internal_msgs_;
};

struct InternalBFTClientMock : public IInternalBFTClient {
  NodeIdType getClientId() const override { return 42; }

  // Returns the sent client request sequence number.
  uint64_t sendRequest(uint64_t flags, uint32_t size, const char* request, const std::string& cid) override {
    auto seq_num = 0;
    if (!requests_.empty()) {
      seq_num = std::prev(requests_.cend())->first + 1;
    }
    auto* p = reinterpret_cast<const uint8_t*>(request);
    requests_[seq_num] = Request{flags, std::vector<uint8_t>{p, p + size}, cid};
    return seq_num;
  }

  uint32_t numOfConnectedReplicas(uint32_t clusterSize) override { return clusterSize; }

  bool isUdp() const override { return false; }
  bool isNodeConnected(uint32_t) override { return false; }
  struct Request {
    uint64_t flags{0};
    std::vector<uint8_t> contents;
    std::string cid;

    bool operator==(const Request& r) const { return (flags == r.flags && contents == r.contents && cid == r.cid); }
  };

  // sequence number -> request
  std::map<uint64_t, Request> requests_;
};

struct PendingRequestMock : public IPendingRequest {
  struct PendingRequest {
    NodeIdType client_id_{0};
    ReqId req_seq_num_{0};

    bool operator==(const PendingRequest& r) const {
      return (client_id_ == r.client_id_ && req_seq_num_ == r.req_seq_num_);
    }
  };

  bool isPending(NodeIdType client_id, ReqId req_seq_num) const override {
    for (const auto& r : pending_) {
      if (r == PendingRequest{client_id, req_seq_num}) {
        return true;
      }
    }
    return false;
  }

  void addPending(NodeIdType client_id, ReqId req_seq_num) {
    pending_.push_back(PendingRequest{client_id, req_seq_num});
  }

  void removePending(NodeIdType client_id, ReqId req_seq_num) {
    pending_.erase(std::remove(pending_.begin(), pending_.end(), PendingRequest{client_id, req_seq_num}),
                   pending_.end());
  }

  std::vector<PendingRequest> pending_;
};

struct TicksGeneratorForTest : public TicksGenerator {
  TicksGeneratorForTest(const std::shared_ptr<bftEngine::impl::IInternalBFTClient>& bft_client,
                        const IPendingRequest& pending_req,
                        const std::shared_ptr<IncomingMsgsStorage>& msgs_storage)
      : TicksGenerator{bft_client, pending_req, msgs_storage, TicksGenerator::DoNotStartThread{}} {}
  void evaluateTimers(const std::chrono::steady_clock::time_point& now) { TicksGenerator::evaluateTimers(now); }
};

class ccron_ticks_generator_test : public ::testing::Test {
 protected:
  const std::uint32_t kComponentId1 = 1;
  const std::uint32_t kComponentId2 = 2;

  std::shared_ptr<InternalBFTClientMock> bft_client_ = std::make_shared<InternalBFTClientMock>();
  PendingRequestMock pending_req_;
  std::shared_ptr<IncomingMsgsStorageMock> msgs_ = std::make_shared<IncomingMsgsStorageMock>();
  TicksGeneratorForTest gen_{bft_client_, pending_req_, msgs_};

  static std::chrono::steady_clock::time_point now() { return std::chrono::steady_clock::now(); }
};

TEST_F(ccron_ticks_generator_test, no_ticks_if_not_started) {
  gen_.evaluateTimers(now() + 10s);
  gen_.evaluateTimers(now() + 11s);

  ASSERT_TRUE(msgs_->internal_msgs_.empty());
}

TEST_F(ccron_ticks_generator_test, internal_tick_msg_is_generated) {
  gen_.start(kComponentId1, 1s);
  gen_.start(kComponentId2, 2s);

  gen_.evaluateTimers(now() + 1s);
  gen_.evaluateTimers(now() + 2s);

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
  gen_.start(kComponentId1, 1s);

  // Should generate a tick.
  gen_.evaluateTimers(now() + 1s);
  ASSERT_EQ(1, msgs_->internal_msgs_.size());

  // Update the period to 4s.
  gen_.start(kComponentId1, 4s);

  // Should not generate a tick as we have updated the period.
  gen_.evaluateTimers(now() + 2s);
  gen_.evaluateTimers(now() + 3s);
  ASSERT_EQ(1, msgs_->internal_msgs_.size());

  // Should generate a tick with the new period.
  gen_.evaluateTimers(now() + 4s);
  ASSERT_EQ(2, msgs_->internal_msgs_.size());

  // Should not generate a tick.
  gen_.evaluateTimers(now() + 7s);
  ASSERT_EQ(2, msgs_->internal_msgs_.size());

  // Should generate a tick.
  gen_.evaluateTimers(now() + 8s);
  ASSERT_EQ(3, msgs_->internal_msgs_.size());
}

TEST_F(ccron_ticks_generator_test, stop_tick) {
  gen_.start(kComponentId1, 1s);
  gen_.start(kComponentId2, 2s);

  // Evaluate twice and fire kComponentId1 twice and kComponentId2 once.
  gen_.evaluateTimers(now() + 1s);
  gen_.evaluateTimers(now() + 2s);
  ASSERT_EQ(3, msgs_->internal_msgs_.size());

  // Stop kComponentId1 and evaluate at 2s in the future, expecting that kComponentId2 will fire once more.
  gen_.stop(kComponentId1);
  gen_.evaluateTimers(now() + 4s);
  ASSERT_EQ(4, msgs_->internal_msgs_.size());

  // Stop kComponentId2 and expect that no timer will fire further.
  gen_.stop(kComponentId2);
  gen_.evaluateTimers(now() + 6s);
  gen_.evaluateTimers(now() + 8s);
  ASSERT_EQ(4, msgs_->internal_msgs_.size());
}

TEST_F(ccron_ticks_generator_test, client_request_msg_is_generated) {
  // Generate an internal tick.
  gen_.onInternalTick({kComponentId1});

  // Make sure we have converted the internal tick into a ClientRequestMsg tick.
  ASSERT_EQ(1, bft_client_->requests_.size());
  const auto seq_num1 = bft_client_->requests_.begin()->first;
  const auto request1 = bft_client_->requests_.begin()->second;
  ASSERT_EQ(TICK_FLAG, request1.flags);
  ASSERT_EQ(TicksGenerator::kTickCid, request1.cid);
  auto payload1 = ClientReqMsgTickPayload{};
  deserialize(request1.contents, payload1);
  ASSERT_EQ(kComponentId1, payload1.component_id);

  // Simulate the onMessage<ClientRequestMsg>() method marking the request pending in ReplicaImp.
  pending_req_.addPending(bft_client_->getClientId(), seq_num1);
  ASSERT_TRUE(pending_req_.isPending(bft_client_->getClientId(), seq_num1));

  // Make sure that we don't generate more ClientRequestMsg ticks until the one for component ID 1 is no longer pending.
  gen_.onInternalTick({kComponentId1});
  ASSERT_EQ(1, bft_client_->requests_.size());
  ASSERT_EQ(request1, bft_client_->requests_[0]);

  // ClientRequestMsg ticks for another component are generated.
  gen_.onInternalTick({kComponentId2});
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
  gen_.onInternalTick({kComponentId1});
  ASSERT_EQ(2, bft_client_->requests_.size());
  ASSERT_EQ(request1, bft_client_->requests_[0]);

  // Simulate that a successful ClientRequestMsg tick has been processed.
  pending_req_.removePending(bft_client_->getClientId(), seq_num1);
  ASSERT_FALSE(pending_req_.isPending(bft_client_->getClientId(), seq_num1));

  // Generate a second internal tick.
  gen_.onInternalTick({kComponentId1});
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
