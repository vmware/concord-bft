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
class ClientApiTestFixture : public ::testing::Test {
 public:
  ClientApiTestFixture() {
    srand(time(NULL));
    // In the futuer, we will build a state that represnts all possible requests.
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
      auto req = std::get<concord::messages::ClientReconfigurationStateRequest>(rreq.command);
      concord::messages::ReconfigurationResponse rres;
      concord::messages::ClientReconfigurationStateReply crep = {req.last_known_block + 1,
                                                                 concord::messages::WedgeCommand{}};
      concord::messages::serialize(rres.additional_data, crep);
      rres.success = true;
      std::vector<uint8_t> msg_buf;
      concord::messages::serialize(msg_buf, rres);
      auto rep = createReply(msg, msg_buf);
      client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(rep.data()), rep.size());
    } else if (std::holds_alternative<concord::messages::ClientReconfigurationStateUpdate>(rreq.command)) {
      auto update = std::get<concord::messages::ClientReconfigurationStateUpdate>(rreq.command);
      if (reconfiguration_state_.find(update.sender_id) == reconfiguration_state_.end()) {
        reconfiguration_state_[update.sender_id] = std::vector<concord::messages::ClientReconfigurationStateUpdate>();
        reconfiguration_state_[update.sender_id].push_back(update);
        concord::messages::ReconfigurationResponse rres;
        rres.success = true;
        std::vector<uint8_t> msg_buf;
        concord::messages::serialize(msg_buf, rres);
        auto rep = createReply(msg, msg_buf);
        client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(rep.data()), rep.size());
      }
    }
  }
  uint64_t latest_block_id{0};
  std::unordered_map<uint64_t, std::vector<concord::messages::ClientReconfigurationStateUpdate>> reconfiguration_state_;
};

TEST_F(ClientApiTestFixture, basic_test) {
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(
      [&](const MsgFromClient& msg, IReceiver* client_receiver) { HandleClientStateRequests(msg, client_receiver); }));
  std::shared_ptr<bft::client::Client> bftclient = std::make_shared<bft::client::Client>(std::move(comm), test_config_);
  cre::state::IStateClient* state_client =
      new cre::state::PollBasedStateClient(bftclient, cre_config.interval_timeout_ms_, 0, cre_config.id_);
  std::shared_ptr<concordMetrics::Aggregator> aggregator = std::make_shared<concordMetrics::Aggregator>();
  cre::ClientReconfigurationEngine cre(cre_config, state_client, aggregator);
  cre.start();
  std::this_thread::sleep_for(1s);
  ASSERT_GT(aggregator->GetGauge(metrics_component, last_known_block_gauge).Get(), 0);
  cre.stop();
  bftclient->stop();
}
}  // namespace

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}