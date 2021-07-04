// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "concord.cmf.hpp"
#include "bftclient/quorums.h"
#include "poll_based_state_client.hpp"
namespace cre {
concord::messages::ReconfigurationResponse sendReconfigurationRequest(bft::client::Client& client,
                                                                      concord::messages::ReconfigurationRequest rreq,
                                                                      const std::string& cid,
                                                                      uint64_t sn,
                                                                      bool read_request) {
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, rreq);
  auto sig = client.signMessage(data_vec);
  rreq.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  bft::client::RequestConfig request_config;
  request_config.reconfiguration = true;
  request_config.correlation_id = cid;
  request_config.sequence_number = sn;
  bft::client::Msg msg;
  concord::messages::serialize(msg, rreq);
  bft::client::Reply rep;
  if (read_request) {
    bft::client::ReadConfig read_config{request_config, bft::client::LinearizableQuorum{}};
    rep = client.send(read_config, std::move(msg));
  } else {
    bft::client::WriteConfig write_config{request_config, bft::client::LinearizableQuorum{}};
    rep = client.send(write_config, std::move(msg));
  }
  concord::messages::ReconfigurationResponse rres;
  concord::messages::deserialize(rep.matched_data, rres);
  if (!rres.success) {
    throw std::runtime_error{"unable to have a quorum, please check the replicas liveness"};
  }
  return rres;
}
State PollBasedStateClient::getNextState(uint64_t lastKnownBlockId) {
  std::unique_lock<std::mutex> lk(lock_);
  while (!stopped && updates_.empty()) {
    new_updates_.wait_for(lk, 1s);
  }
  if (stopped) return {lastKnownBlockId, {}};
  auto ret = updates_.front();
  updates_.pop();
  return ret;
}

PollBasedStateClient::PollBasedStateClient(bft::client::Client* client,
                                           uint64_t interval_timeout_ms,
                                           uint64_t last_known_block,
                                           const uint16_t id)
    : bftclient_{client},
      id_{id},
      interval_timeout_ms_{interval_timeout_ms},
      last_known_block_{last_known_block},
      sn_gen_(bft::client::ClientId{id}) {}

State PollBasedStateClient::getStateUpdate(uint64_t lastKnownBlockId) {
  std::lock_guard<std::mutex> lock(bftclient_lock_);
  concord::messages::ClientReconfigurationStateRequest creq{id_, lastKnownBlockId};
  concord::messages::ReconfigurationRequest rreq;
  rreq.command = creq;
  auto sn = sn_gen_.unique();
  auto rres = sendReconfigurationRequest(*bftclient_, rreq, "getStateUpdate-" + std::to_string(sn), sn, true);
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(rres.additional_data, crep);
  return {crep.block_id, rres.additional_data};
}

PollBasedStateClient::~PollBasedStateClient() {
  std::lock_guard<std::mutex> lock(lock_);
  bftclient_->stop();
  if (!stopped) {
    stopped = true;
    try {
      consumer_.join();
    } catch (std::exception& e) {
      LOG_ERROR(getLogger(), e.what());
    }
  }
}
void PollBasedStateClient::start(uint64_t lastKnownBlock) {
  last_known_block_ = lastKnownBlock;
  stopped = false;
  consumer_ = std::thread([&]() {
    while (!stopped) {
      std::this_thread::sleep_for(std::chrono::milliseconds(interval_timeout_ms_));
      if (stopped) return;
      auto new_state = getStateUpdate(last_known_block_);
      std::lock_guard<std::mutex> lk(lock_);
      if (new_state.blockid > last_known_block_) {
        updates_.push(new_state);
        last_known_block_ = new_state.blockid;
        new_updates_.notify_one();
      }
    }
  });
}
void PollBasedStateClient::stop() {
  if (stopped) return;
  stopped = true;
  try {
    consumer_.join();
  } catch (std::exception& e) {
    LOG_ERROR(getLogger(), e.what());
  }
}
State PollBasedStateClient::getLatestClientUpdate(uint16_t clientId) {
  std::lock_guard<std::mutex> lock(bftclient_lock_);
  concord::messages::ClientReconfigurationLastUpdate creq{id_};
  concord::messages::ReconfigurationRequest rreq;
  rreq.command = creq;
  auto sn = sn_gen_.unique();
  auto rres = sendReconfigurationRequest(*bftclient_, rreq, "getLatestClientUpdate-" + std::to_string(sn), sn, true);
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(rres.additional_data, crep);
  return {crep.block_id, rres.additional_data};
}
bool PollBasedStateClient::updateStateOnChain(const State& state) {
  std::lock_guard<std::mutex> lock(bftclient_lock_);
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::deserialize(state.data, rreq);
  auto sn = sn_gen_.unique();
  auto rres = sendReconfigurationRequest(*bftclient_, rreq, "updateStateOnChain-" + std::to_string(sn), sn, false);
  return rres.success;
}
}  // namespace cre