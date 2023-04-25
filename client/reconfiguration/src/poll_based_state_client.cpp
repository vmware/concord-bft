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
    concord::messages::ReconfigurationRequest& rreq, const std::string& cid, uint64_t sn, bool read_request) const {
  std::lock_guard<std::mutex> lock(bftclient_lock_);
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, rreq);
  auto sig = bftclient_->signMessage(data_vec);
  rreq.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  bft::client::RequestConfig request_config;
  request_config.reconfiguration = true;
  request_config.correlation_id = cid;
  request_config.sequence_number = sn;
  request_config.max_reply_size = 1024 * 1024;  // Set the max reply size to 1MB
  bft::client::Msg msg;
  concord::messages::serialize(msg, rreq);
  bft::client::Reply rep;
  concord::messages::ReconfigurationResponse rres;
  try {
    if (read_request) {
      // TODO: State transfer can work with f + 1 as long as there are no byzantine replicas
      bft::client::ReadConfig read_config;
      if (use_byzantine_quorum_) {
        read_config = bft::client::ReadConfig{request_config, bft::client::ByzantineSafeQuorum{}};
      } else {
        read_config = bft::client::ReadConfig{request_config, bft::client::LinearizableQuorum{}};
      }

      rep = bftclient_->send(read_config, std::move(msg));
    } else {
      bft::client::WriteConfig write_config{request_config, bft::client::LinearizableQuorum{}};
      rep = bftclient_->send(write_config, std::move(msg));
    }
    concord::messages::deserialize(rep.matched_data, rres);
  } catch (std::exception& e) {
    LOG_WARN(getLogger(), "error while initiating bft request " << e.what());
    rres.success = false;
    return rres;
  }
  return rres;
}
State PollBasedStateClient::getNextState() const {
  std::unique_lock<std::mutex> lk(lock_);
  while (!stopped && updates_.empty()) {
    new_updates_.wait_for(lk, 1s);
  }
  if (stopped) return {0, {}};
  auto ret = updates_.front();
  updates_.pop();
  return ret;
}

PollBasedStateClient::PollBasedStateClient(bft::client::Client* client,
                                           uint64_t interval_timeout_ms,
                                           uint64_t last_known_block,
                                           const uint16_t id,
                                           bool use_byzantine_quorum)
    : bftclient_{client},
      id_{id},
      interval_timeout_ms_{interval_timeout_ms},
      last_known_block_{last_known_block},
      sn_gen_(bft::client::ClientId{id}),
      use_byzantine_quorum_{use_byzantine_quorum} {}

std::vector<State> PollBasedStateClient::getStateUpdate(bool& succ) const {
  concord::messages::ClientReconfigurationStateRequest creq{id_};
  concord::messages::ReconfigurationRequest rreq;
  rreq.sender = id_;
  rreq.command = creq;
  auto sn = sn_gen_.unique();
  LOG_DEBUG(getLogger(), "sending reconfig request" << KVLOG(sn));
  auto rres = sendReconfigurationRequest(rreq, "getStateUpdate-" + std::to_string(sn), sn, true);
  if (!rres.success) {
    LOG_WARN(getLogger(), "invalid response from replicas");
    succ = false;
    return {};
  }
  concord::messages::ClientReconfigurationStateReply crep;
  try {
    concord::messages::deserialize(rres.additional_data, crep);
  } catch (const std::exception& e) {
    LOG_ERROR(getLogger(), e.what());
    succ = false;
    return {};
  }
  succ = true;
  std::vector<State> res;
  for (const auto& s : crep.states) {
    std::vector<uint8_t> data_buf;
    concord::messages::serialize(data_buf, s);
    State new_state = {s.block_id, std::move(data_buf)};
    res.push_back(new_state);
  }
  return res;
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
void PollBasedStateClient::start() {
  stopped = false;
  consumer_ = std::thread([&]() {
    LOG_INFO(getLogger(), "Client started" << KVLOG(interval_timeout_ms_));
    while (!stopped) {
      {
        std::unique_lock<std::mutex> lk(resume_lock_);
        while (halted_) {
          resume_cond_.wait(lk);
        }
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(interval_timeout_ms_));
      if (stopped) break;
      std::lock_guard<std::mutex> lk(lock_);
      bool succ;
      LOG_DEBUG(getLogger(), "Getting state update");
      auto new_state = getStateUpdate(succ);
      uint64_t max_update_block{0};
      for (const auto& s : new_state) {
        if (s.blockid > last_known_block_) {
          updates_.push(s);
          if (s.blockid > max_update_block) max_update_block = s.blockid;
        }
      }
      new_updates_.notify_one();
      if (max_update_block > last_known_block_) last_known_block_ = max_update_block;
    }
    LOG_INFO(getLogger(), "Client stopped");
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

bool PollBasedStateClient::updateState(const WriteState& state) {
  concord::messages::ReconfigurationRequest rreq;
  concord::messages::deserialize(state.data, rreq);
  rreq.sender = id_;
  auto sn = sn_gen_.unique();
  auto rres = sendReconfigurationRequest(rreq, "updateState-" + std::to_string(sn), sn, false);
  if (rres.success && state.callBack != nullptr) state.callBack();
  return rres.success;
}
void PollBasedStateClient::halt() {
  LOG_INFO(getLogger(), "Halting client");
  std::unique_lock<std::mutex> lk(resume_lock_);
  halted_ = true;
}
void PollBasedStateClient::resume() {
  LOG_INFO(getLogger(), "Resuming client");
  std::unique_lock<std::mutex> lk(resume_lock_);
  halted_ = false;
  resume_cond_.notify_one();
}
}  // namespace concord::client::reconfiguration