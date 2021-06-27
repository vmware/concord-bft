// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "state_client.hpp"
#include "concord.cmf.hpp"
#include "bftclient/quorums.h"

namespace cre::state {
State state::PollBasedStateClient::getNextState(uint64_t lastKnownBlockId) {
  std::unique_lock<std::mutex> lk(lock_);
  while (!stopped && updates_.empty()) {
    new_updates_.wait_for(lk, 1s);
  }
  if (stopped) return {lastKnownBlockId, {}};
  auto ret = updates_.front();
  updates_.pop();
  return ret;
}

PollBasedStateClient::PollBasedStateClient(std::shared_ptr<bft::client::Client> client,
                                           uint64_t interval_timeout_ms,
                                           uint64_t last_known_block,
                                           const uint16_t id)
    : bftclient_{client},
      id_{id},
      interval_timeout_ms_{interval_timeout_ms},
      last_known_block_{last_known_block},
      sn_gen_(bft::client::ClientId{id}) {}

State PollBasedStateClient::getNewStateImpl(uint64_t lastKnownBlockId) {
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::ClientReconfigurationStateRequest creq{id_, lastKnownBlockId};
  rreq.command = creq;
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, rreq);
  auto sig = bftclient_->signMessage(data_vec);
  rreq.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  bft::client::RequestConfig request_config;
  request_config.reconfiguration = true;
  request_config.correlation_id = "ClientReconfigurationStateRequest-" + std::to_string(lastKnownBlockId);
  request_config.sequence_number = sn_gen_.unique();
  bft::client::ReadConfig read_config{request_config, bft::client::LinearizableQuorum{}};
  bft::client::Msg msg;
  concord::messages::serialize(msg, rreq);
  auto rep = bftclient_->send(read_config, std::move(msg));

  // To have a valid quorum, we expect to have the reply in the additional_data of te reply.
  concord::messages::ReconfigurationResponse rres;
  concord::messages::deserialize(rep.matched_data, rres);
  if (!rres.success) {
    throw std::runtime_error{"unable to have a quorum, please check the replicas liveness"};
  }
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(rres.additional_data, crep);
  return {crep.block_id, rres.additional_data};
}

PollBasedStateClient::~PollBasedStateClient() {
  if (stopped) return;
  stopped = true;
  try {
    consumer_.join();
  } catch (...) {
  }
}
void PollBasedStateClient::start() {
  stopped = false;
  consumer_ = std::thread([&]() {
    while (!stopped) {
      std::this_thread::sleep_for(std::chrono::microseconds(interval_timeout_ms_));
      if (stopped) return;
      std::lock_guard<std::mutex> lk(lock_);
      auto new_state = getNewStateImpl(last_known_block_);
      if (new_state.block > last_known_block_) {
        updates_.push(new_state);
        last_known_block_ = new_state.block;
        new_updates_.notify_all();
      }
    }
  });
}
void PollBasedStateClient::stop() {
  if (stopped) return;
  stopped = true;
  try {
    consumer_.join();
  } catch (...) {
  }
}
State PollBasedStateClient::getLatestClientUpdate(uint16_t clientId) {
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::ClientReconfigurationLastUpdate creq{id_};
  rreq.command = creq;
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, rreq);
  auto sig = bftclient_->signMessage(data_vec);
  rreq.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  bft::client::RequestConfig request_config;
  request_config.reconfiguration = true;
  request_config.correlation_id = "ClientReconfigurationLastUpdate-" + std::to_string(id_);
  request_config.sequence_number = sn_gen_.unique();
  bft::client::ReadConfig read_config{request_config, bft::client::LinearizableQuorum{}};
  bft::client::Msg msg;
  concord::messages::serialize(msg, rreq);
  auto rep = bftclient_->send(read_config, std::move(msg));

  // To have a valid quorum, we expect to have the reply in the additional_data of te reply.
  concord::messages::ReconfigurationResponse rres;
  concord::messages::deserialize(rep.matched_data, rres);
  if (!rres.success) {
    throw std::runtime_error{"unable to have a quorum, please check the replicas liveness"};
  }
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(rres.additional_data, crep);
  return {crep.block_id, rres.additional_data};
}
}  // namespace cre::state