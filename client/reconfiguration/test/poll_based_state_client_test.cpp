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
#include "bftclient/fake_comm.h"
#include "concord.cmf.hpp"
#include "client/reconfiguration/config.hpp"
#include "client/reconfiguration/client_reconfiguration_engine.hpp"
#include "client/reconfiguration/poll_based_state_client.hpp"
#include <unordered_map>
#include <optional>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

using namespace concord::client::reconfiguration;
using ReplicaId_t = bft::client::ReplicaId;

namespace {
std::string metrics_component = "client_reconfiguration_engine";
std::string invalids_counter = "invalid_handlers";
std::string errors_counter = "errored_handlers";
std::string last_known_block_gauge = "last_known_block";

template <typename T>
bool hasValue(const State& state) {
  concord::messages::ClientStateReply crep;
  concord::messages::deserialize(state.data, crep);
  return std::holds_alternative<T>(crep.response);
}

template <typename T>
T getData(const State& state) {
  concord::messages::ClientStateReply crep;
  concord::messages::deserialize(state.data, crep);
  return std::get<T>(crep.response);
}

class KeyExchangeHandler : public IStateHandler {
 public:
  KeyExchangeHandler(uint16_t id) : clientId_{id} {}
  bool validate(const State& state) const override {
    return hasValue<concord::messages::ClientKeyExchangeCommand>(state);
  }
  bool execute(const State& state, WriteState& out) override {
    concord::messages::ClientKeyExchangeCommand command = getData<concord::messages::ClientKeyExchangeCommand>(state);
    if (std::find(command.target_clients.begin(), command.target_clients.end(), clientId_) ==
        command.target_clients.end())
      return false;
    LOG_INFO(getLogger(), "generating new key paris for client");
    // generate new key paris here
    concord::messages::ReconfigurationRequest rreq;
    concord::messages::ClientExchangePublicKey creq;
    creq.sender_id = clientId_;
    creq.pub_key = "test_pub_key";
    rreq.command = creq;
    std::vector<uint8_t> req_buf;
    concord::messages::serialize(req_buf, rreq);
    out = {req_buf, [&]() { LOG_INFO(getLogger(), "successful write!"); }};
    exchanges_++;
    return true;
  }
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.client.reconfiguration.test.KeyExchangeHandler"));
    return logger_;
  }
  uint16_t clientId_;
  std::atomic_uint32_t exchanges_{0};
};

class ClientScaleHandler : public IStateHandler {
 public:
  ClientScaleHandler(uint16_t id) : clientId_{id} {}
  bool validate(const State& state) const override {
    return hasValue<concord::messages::ClientsAddRemoveExecuteCommand>(state);
  }
  bool execute(const State& state, WriteState& out) override {
    concord::messages::ClientsAddRemoveExecuteCommand command =
        getData<concord::messages::ClientsAddRemoveExecuteCommand>(state);
    LOG_INFO(getLogger(), "executing scale command for client");
    concord::messages::ReconfigurationRequest rreq;
    concord::messages::ClientsAddRemoveUpdateCommand creq;
    creq.sender_id = clientId_;
    creq.config_descriptor = command.config_descriptor;
    rreq.command = creq;
    std::vector<uint8_t> req_buf;
    concord::messages::serialize(req_buf, rreq);
    out = {req_buf, [&]() { LOG_INFO(getLogger(), "successful write!"); }};
    return true;
  }
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.client.reconfiguration.test.ScaleHandler"));
    return logger_;
  }
  uint16_t clientId_;
};

class ClientApiTestFixture : public ::testing::Test {
 public:
  void init(uint32_t number_of_ops) {
    for (uint32_t i = 0; i < number_of_ops; i++) {
      blockchain_.emplace_back(concord::messages::ClientKeyExchangeCommand{{5}});
    }
  }
  void initScaleCmd() {
    addRemoveExecuteCmd_.config_descriptor = "TestConfig";
    addRemoveExecuteCmd_.token = "TestToken";
    addRemoveExecuteCmd_.restart = true;
  }
  ClientConfig test_config_ = {ClientId{5},
                               {ReplicaId_t{0}, ReplicaId_t{1}, ReplicaId_t{2}, ReplicaId_t{3}},
                               {},
                               1,
                               0,
                               RetryTimeoutConfig{},
                               std::nullopt};

  Config cre_config{5, 10};

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
      concord::messages::ClientReconfigurationStateReply srep;
      uint64_t i{1};
      for (const auto& s : blockchain_) {
        concord::messages::ClientStateReply rep;
        rep.block_id = i;
        rep.response = s;
        srep.states.push_back(rep);
        i++;
      }
      if (!addRemoveExecuteCmd_.config_descriptor.empty()) {
        concord::messages::ClientStateReply rep;
        rep.block_id = 1;
        rep.response = addRemoveExecuteCmd_;
        srep.states.push_back(rep);
      }
      concord::messages::serialize(rres.additional_data, srep);
      rres.success = true;
      std::vector<uint8_t> msg_buf;
      concord::messages::serialize(msg_buf, rres);
      auto rep = createReply(msg, msg_buf);
      client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(rep.data()), rep.size());
    } else if (std::holds_alternative<concord::messages::ClientExchangePublicKey>(rreq.command)) {
      concord::messages::ReconfigurationResponse rres;
      pub_keys_.push_back(std::get<concord::messages::ClientExchangePublicKey>(rreq.command));
      rres.success = true;
      std::vector<uint8_t> msg_buf;
      concord::messages::serialize(msg_buf, rres);
      auto rep = createReply(msg, msg_buf);
      client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(rep.data()), rep.size());
    }
  }
  std::vector<concord::messages::ClientKeyExchangeCommand> blockchain_;
  std::vector<concord::messages::ClientExchangePublicKey> pub_keys_;
  concord::messages::ClientsAddRemoveExecuteCommand addRemoveExecuteCmd_;
};

/*
 * This tests only the start & stop of the whole framework. As there is no updates, we shouldn't have any at the end
 * of the test
 */
TEST_F(ClientApiTestFixture, basic_test) {
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(
      [&](const MsgFromClient& msg, IReceiver* client_receiver) { HandleClientStateRequests(msg, client_receiver); }));
  IStateClient* state_client = new PollBasedStateClient(
      new bft::client::Client(std::move(comm), test_config_), cre_config.interval_timeout_ms_, 0, cre_config.id_);
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  ClientReconfigurationEngine cre(cre_config, state_client, aggregator);
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
  IStateClient* state_client = new PollBasedStateClient(
      new bft::client::Client(std::move(comm), test_config_), cre_config.interval_timeout_ms_, 0, cre_config.id_);
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  ClientReconfigurationEngine cre(cre_config, state_client, aggregator);
  auto keyExchangeHandler = std::make_shared<KeyExchangeHandler>(cre_config.id_);
  cre.registerHandler(keyExchangeHandler);
  cre.start();
  while (keyExchangeHandler->exchanges_ < 1 ||
         aggregator->GetGauge(metrics_component, last_known_block_gauge).Get() < 1) {
  }
  ASSERT_GE(aggregator->GetGauge(metrics_component, last_known_block_gauge).Get(), 1);
  ASSERT_GE(keyExchangeHandler->exchanges_, 1);
  cre.stop();
}

/*
 * Test a single 2-phases key exchange command.
 */
TEST_F(ClientApiTestFixture, single_key_two_phases_exchange_command) {
  init(1);
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(
      [&](const MsgFromClient& msg, IReceiver* client_receiver) { HandleClientStateRequests(msg, client_receiver); }));
  IStateClient* state_client = new PollBasedStateClient(
      new bft::client::Client(std::move(comm), test_config_), cre_config.interval_timeout_ms_, 0, cre_config.id_);
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  ClientReconfigurationEngine cre(cre_config, state_client, aggregator);
  auto keyExchangeHandler = std::make_shared<KeyExchangeHandler>(cre_config.id_);
  cre.registerHandler(keyExchangeHandler);
  cre.start();
  while (aggregator->GetGauge(metrics_component, last_known_block_gauge).Get() < 1 ||
         keyExchangeHandler->exchanges_ < 1 || pub_keys_.size() < 1) {
  }
  ASSERT_EQ(aggregator->GetGauge(metrics_component, last_known_block_gauge).Get(), 1);
  ASSERT_EQ(keyExchangeHandler->exchanges_, 1);
  ASSERT_GE(pub_keys_.size(), 1);
  cre.stop();
}

/*
 * Test a multiple 2-phases key exchange command.
 */
TEST_F(ClientApiTestFixture, multiple_key_two_phases_exchange_command) {
  init(10);
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(
      [&](const MsgFromClient& msg, IReceiver* client_receiver) { HandleClientStateRequests(msg, client_receiver); }));
  IStateClient* state_client = new PollBasedStateClient(
      new bft::client::Client(std::move(comm), test_config_), cre_config.interval_timeout_ms_, 0, cre_config.id_);
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  ClientReconfigurationEngine cre(cre_config, state_client, aggregator);
  auto keyExchangeHandler = std::make_shared<KeyExchangeHandler>(cre_config.id_);
  cre.registerHandler(keyExchangeHandler);
  cre.start();
  while (aggregator->GetGauge(metrics_component, last_known_block_gauge).Get() < 10 ||
         keyExchangeHandler->exchanges_ < 10 || pub_keys_.size() < 10) {
  }
  ASSERT_EQ(aggregator->GetGauge(metrics_component, last_known_block_gauge).Get(), 10);
  ASSERT_EQ(keyExchangeHandler->exchanges_, 10);
  ASSERT_GE(pub_keys_.size(), 10);
  cre.stop();
}
/*
 * Test a client scale command.
 */
TEST_F(ClientApiTestFixture, client_scale_command) {
  initScaleCmd();
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(
      [&](const MsgFromClient& msg, IReceiver* client_receiver) { HandleClientStateRequests(msg, client_receiver); }));
  IStateClient* state_client = new PollBasedStateClient(
      new bft::client::Client(std::move(comm), test_config_), cre_config.interval_timeout_ms_, 0, cre_config.id_);
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  ClientReconfigurationEngine cre(cre_config, state_client, aggregator);
  auto scaleHandler = std::make_shared<ClientScaleHandler>(cre_config.id_);
  cre.registerHandler(scaleHandler);
  cre.start();
  while (aggregator->GetGauge(metrics_component, last_known_block_gauge).Get() < 1) {
  }
  ASSERT_GE(aggregator->GetGauge(metrics_component, last_known_block_gauge).Get(), 1);
  cre.stop();
}

}  // namespace

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}