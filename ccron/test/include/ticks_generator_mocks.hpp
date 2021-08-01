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

#pragma once

#include "ccron/ticks_generator.hpp"

#include "IncomingMsgsStorage.hpp"
#include "InternalBFTClient.hpp"
#include "IPendingRequest.hpp"

#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace concord::cron::test {

struct IncomingMsgsStorageMock : public bftEngine::impl::IncomingMsgsStorage {
  void start() override{};
  void stop() override{};

  bool isRunning() const override { return true; };

  bool pushExternalMsg(std::unique_ptr<MessageBase> msg) override { return true; }
  bool pushExternalMsg(std::unique_ptr<MessageBase> msg, Callback onMsgPopped) override { return true; }
  bool pushExternalMsgRaw(char* msg, size_t size) override { return true; }
  bool pushExternalMsgRaw(char* msg, size_t size, Callback onMsgPopped) override { return true; }
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

}  // namespace concord::cron::test
