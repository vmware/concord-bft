// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the 'License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "gtest/gtest.h"
#include "assertUtils.hpp"
#include "state_client.hpp"
#include "bftclient/fake_comm.h"
#include "state_client.hpp"
#include "concord.cmf.hpp"
#include "config.hpp"
#include "client_reconfiguration_engine.hpp"
#include <unordered_map>
#include <optional>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

using namespace cre::state;
using ReplicaId_t = bft::client::ReplicaId;

namespace {
std::string metrics_component = "client_reconfiguration_engine";
std::string invalids_counter = "invalid_handlers";
std::string errors_counter = "errored_handlers";
std::string last_known_block_gauge = "last_known_block";

template <typename T>
bool hasValue(const State& state) {
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(state.data, crep);
  return std::holds_alternative<T>(crep.response);
}

template <typename T>
T getData(const State& state) {
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(state.data, crep);
  return std::get<T>(crep.response);
}

class KeyExchangeHandler : public IStateHandler {
 public:
  KeyExchangeHandler(uint16_t id) : clientId_{id} {}
  bool validate(const State& state) override { return hasValue<concord::messages::ClientKeyExchange>(state); }
  bool execute(const State& state, State& out) override {
    concord::messages::ClientKeyExchange command = getData<concord::messages::ClientKeyExchange>(state);
    if (std::find(command.clients.begin(), command.clients.end(), clientId_) == command.clients.end()) return false;
    LOG_INFO(getLogger(), "generating new key paris for client");
    // generate new key paris here
    concord::messages::ReconfigurationRequest rreq;
    concord::messages::ClientExchangePublicKey creq;
    creq.sender_id = clientId_;
    creq.pub_key = "test_pub_key";
    rreq.command = creq;
    std::vector<uint8_t> req_buf;
    concord::messages::serialize(req_buf, rreq);
    out = {state.block, req_buf};
    exchanges_++;
    return true;
  }
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("cre.stateHandler.KeyExchangeHandler"));
    return logger_;
  }
  uint16_t clientId_;
  uint32_t exchanges_{0};
};

class PublicKeyExchangeHandler : public IStateHandler {
 public:
  bool validate(const State& state) override { return hasValue<concord::messages::ClientExchangePublicKey>(state); }
  bool execute(const State&, State&) override {
    LOG_INFO(getLogger(), "restart client components");
    exchanges_++;
    return true;
  }
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("cre.stateHandler.PublicKeyExchange"));
    return logger_;
  }
  uint32_t exchanges_{0};
};

class ClientApiTestFixture : public ::testing::Test {
 public:
  void init(uint32_t number_of_ops) {
    for (uint32_t i = 0; i < number_of_ops; i++) {
      blockchain_.emplace_back(
          concord::messages::ClientReconfigurationStateReply{i + 1, concord::messages::ClientKeyExchange{{5}}});
    }
  }
  ClientConfig test_config_ = {ClientId{5},
                               {ReplicaId_t{0}, ReplicaId_t{1}, ReplicaId_t{2}, ReplicaId_t{3}},
                               {},
                               1,
                               0,
                               RetryTimeoutConfig{},
                               std::nullopt};

  cre::config::Config cre_config{5, 10};

  void HandleClientStateRequests(const MsgFromClient& msg, IReceiver* client_receiver) {
    auto* orig_msg = reinterpret_cast<const bftEngine::ClientRequestMsgHeader*>(msg.data.data());
    if (!(orig_msg->flags & RECONFIG_FLAG)) {
      auto reply = createReply(msg);
      client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(reply.data()), reply.size());
      return;
    }
    concord::messages::ReconfigurationRequest rreq;
    auto start_request_pos = sizeof(bftEngine::ClientRequestMsgHeader) + orig_msg->spanContextSize;
    std::vector<uint8_t> data(msg.data.data() + start_request_pos,
                              msg.data.data() + start_request_pos + orig_msg->requestLength);
    deserialize(data, rreq);
    if (std::holds_alternative<concord::messages::ClientReconfigurationStateRequest>(rreq.command)) {
      concord::messages::ReconfigurationResponse rres;
      uint32_t index = number_of_messages_ / 4;
      if (index < blockchain_.size() - 1) {
        number_of_messages_++;
      }
      concord::messages::ClientReconfigurationStateReply update{0, {}};
      if (!blockchain_.empty()) update = blockchain_[index];
      concord::messages::serialize(rres.additional_data, update);
      rres.success = true;
      std::vector<uint8_t> msg_buf;
      concord::messages::serialize(msg_buf, rres);
      auto rep = createReply(msg, msg_buf);
      client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(rep.data()), rep.size());
    } else if (std::holds_alternative<concord::messages::ClientExchangePublicKey>(rreq.command)) {
      auto update = std::get<concord::messages::ClientExchangePublicKey>(rreq.command);
      if (number_of_updates_ % 4 == 0) {
        blockchain_.push_back({blockchain_.size(), update});
      }
      number_of_updates_++;
      concord::messages::ReconfigurationResponse rres;
      rres.success = true;
      std::vector<uint8_t> msg_buf;
      concord::messages::serialize(msg_buf, rres);
      auto rep = createReply(msg, msg_buf);
      client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(rep.data()), rep.size());
    } else if (std::holds_alternative<concord::messages::ClientReconfigurationLastUpdate>(rreq.command)) {
      if (!blockchain_.empty()) {
        concord::messages::ClientReconfigurationStateReply crep = blockchain_[0];
        crep.block_id = 0;
        concord::messages::ReconfigurationResponse rres;
        concord::messages::serialize(rres.additional_data, crep);
        rres.success = true;
        std::vector<uint8_t> msg_buf;
        concord::messages::serialize(msg_buf, rres);
        auto rep = createReply(msg, msg_buf);
        client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(rep.data()), rep.size());
      } else {
        concord::messages::ClientReconfigurationStateReply crep = {0, {}};
        concord::messages::ReconfigurationResponse rres;
        concord::messages::serialize(rres.additional_data, crep);
        rres.success = true;
        std::vector<uint8_t> msg_buf;
        concord::messages::serialize(msg_buf, rres);
        auto reply = createReply(msg, msg_buf);
        client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(reply.data()), reply.size());
        return;
      }
    }
  }
  std::vector<concord::messages::ClientReconfigurationStateReply> blockchain_;
  uint32_t number_of_messages_{0};
  uint32_t number_of_updates_{0};
};

/*
 * This tests only the start & stop of the whole framework. As there is no updates, we shouldn't have any at the end
 * of the test
 */
TEST_F(ClientApiTestFixture, basic_test) {
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(
      [&](const MsgFromClient& msg, IReceiver* client_receiver) { HandleClientStateRequests(msg, client_receiver); }));
  cre::state::IStateClient* state_client = new cre::state::PollBasedStateClient(
      new bft::client::Client(std::move(comm), test_config_), cre_config.interval_timeout_ms_, 0, cre_config.id_);
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  cre::ClientReconfigurationEngine cre(cre_config, state_client, aggregator);
  cre.start();
  std::this_thread::sleep_for(1s);
  ASSERT_EQ(aggregator->GetGauge(metrics_component, last_known_block_gauge).Get(), 0);
  cre.stop();
}
/*
 * Test a single key exchange command.
 */
TEST_F(ClientApiTestFixture, single_key_exchange_command) {
  init(1);
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(
      [&](const MsgFromClient& msg, IReceiver* client_receiver) { HandleClientStateRequests(msg, client_receiver); }));
  cre::state::IStateClient* state_client = new cre::state::PollBasedStateClient(
      new bft::client::Client(std::move(comm), test_config_), cre_config.interval_timeout_ms_, 0, cre_config.id_);
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  cre::ClientReconfigurationEngine cre(cre_config, state_client, aggregator);
  auto keyExchangeHandler = std::make_shared<KeyExchangeHandler>(cre_config.id_);
  cre.registerHandler(keyExchangeHandler);
  cre.start();
  std::this_thread::sleep_for(1s);
  ASSERT_EQ(aggregator->GetGauge(metrics_component, last_known_block_gauge).Get(), 2);
  ASSERT_EQ(keyExchangeHandler->exchanges_, 1);
  cre.stop();
}

/*
 * Test a single 2-phases key exchange command.
 */
TEST_F(ClientApiTestFixture, single_key_two_phases_exchange_command) {
  init(1);
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(
      [&](const MsgFromClient& msg, IReceiver* client_receiver) { HandleClientStateRequests(msg, client_receiver); }));
  cre::state::IStateClient* state_client = new cre::state::PollBasedStateClient(
      new bft::client::Client(std::move(comm), test_config_), cre_config.interval_timeout_ms_, 0, cre_config.id_);
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  cre::ClientReconfigurationEngine cre(cre_config, state_client, aggregator);
  auto keyExchangeHandler = std::make_shared<KeyExchangeHandler>(cre_config.id_);
  auto clientPubKeyExchangeHandler = std::make_shared<PublicKeyExchangeHandler>();
  cre.registerHandler(keyExchangeHandler);
  cre.registerHandler(clientPubKeyExchangeHandler);
  cre.start();
  std::this_thread::sleep_for(1s);
  ASSERT_EQ(aggregator->GetGauge(metrics_component, last_known_block_gauge).Get(), 2);
  ASSERT_EQ(keyExchangeHandler->exchanges_, 1);
  ASSERT_EQ(clientPubKeyExchangeHandler->exchanges_, 1);
  cre.stop();
}

}  // namespace

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}