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
#include "client/reconfiguration/poll_based_state_client.hpp"
namespace concord::client::reconfiguration {
concord::messages::ReconfigurationResponse PollBasedStateClient::sendReconfigurationRequest(
    concord::messages::ReconfigurationRequest& rreq, const string& cid, uint64_t sn, bool read_request) const {
  std::lock_guard<std::mutex> lock(bftclient_lock_);
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, rreq);
  auto sig = bftclient_->signMessage(data_vec);
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
    rep = bftclient_->send(read_config, std::move(msg));
  } else {
    bft::client::WriteConfig write_config{request_config, bft::client::LinearizableQuorum{}};
    rep = bftclient_->send(write_config, std::move(msg));
  }
  concord::messages::ReconfigurationResponse rres;
  concord::messages::deserialize(rep.matched_data, rres);
  return rres;
}
State PollBasedStateClient::getNextState(uint64_t lastKnownBlockId) const {
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

State PollBasedStateClient::getStateUpdate(uint64_t lastKnownBlockId) const {
  concord::messages::ClientReconfigurationStateRequest creq{id_, lastKnownBlockId};
  concord::messages::ReconfigurationRequest rreq;
  rreq.sender = id_;
  rreq.command = creq;
  auto sn = sn_gen_.unique();
  auto rres = sendReconfigurationRequest(rreq, "getStateUpdate-" + std::to_string(sn), sn, true);
  if (!rres.success) {
    LOG_WARN(getLogger(), "invalid response from replicas " << KVLOG(lastKnownBlockId));
    return {0, {}};
  }
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(rres.additional_data, crep);
  return {crep.block_id, rres.additional_data};
}

PollBasedStateClient::~PollBasedStateClient() {
  if (!stopped) {
    stopped = true;
    {
      std::lock_guard<std::mutex> lock(bftclient_lock_);
      bftclient_->stop();
    }
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
  {
    std::lock_guard<std::mutex> lock(bftclient_lock_);
    bftclient_->stop();
  }
  try {
    consumer_.join();
  } catch (std::exception& e) {
    LOG_ERROR(getLogger(), e.what());
  }
}
State PollBasedStateClient::getLatestClientUpdate(uint16_t clientId) const {
  concord::messages::ClientReconfigurationLastUpdate creq{id_};
  concord::messages::ReconfigurationRequest rreq;
  rreq.sender = id_;
  rreq.command = creq;
  auto sn = sn_gen_.unique();
  auto rres = sendReconfigurationRequest(rreq, "getLatestClientUpdate-" + std::to_string(sn), sn, true);
  if (!rres.success) {
    LOG_WARN(getLogger(), "invalid response from replicas " << KVLOG(clientId));
    return {0, {}};
  }
  concord::messages::ClientReconfigurationStateReply crep;
  concord::messages::deserialize(rres.additional_data, crep);
  return {crep.block_id, rres.additional_data};
}
bool PollBasedStateClient::updateStateOnChain(const WriteState& state) {
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::deserialize(state.data, rreq);
  rreq.sender = id_;
  auto sn = sn_gen_.unique();
  auto rres = sendReconfigurationRequest(rreq, "updateStateOnChain-" + std::to_string(sn), sn, false);
  if (rres.success && state.callBack != nullptr) state.callBack();
  return rres.success;
}
}  // namespace concord::client::reconfiguration