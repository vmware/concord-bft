// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "ccron/ticks_generator.hpp"

#include "ccron_msgs.cmf.hpp"
#include "messages/TickInternalMsg.hpp"
#include "Replica.hpp"

#include <cstdint>
#include <utility>
#include <vector>

namespace concord::cron {

using concordUtil::Timers;
using bftEngine::impl::TickInternalMsg;

TicksGenerator::TicksGenerator(const std::shared_ptr<bftEngine::impl::IInternalBFTClient>& bft_client,
                               const IPendingRequest& pending_req,
                               const std::shared_ptr<IncomingMsgsStorage>& msgs_storage,
                               const std::chrono::seconds& poll_period)
    : bft_client_{bft_client}, pending_req_{&pending_req}, msgs_storage_{msgs_storage}, poll_period_{poll_period} {
  thread_ = std::thread{[this]() { run(); }};
}

TicksGenerator::TicksGenerator(const std::shared_ptr<bftEngine::impl::IInternalBFTClient>& bft_client,
                               const IPendingRequest& pending_req,
                               const std::shared_ptr<IncomingMsgsStorage>& msgs_storage,
                               DoNotStartThread)
    : bft_client_{bft_client}, pending_req_{&pending_req}, msgs_storage_{msgs_storage} {}

TicksGenerator::~TicksGenerator() {
  stop_ = true;
  if (thread_.joinable()) thread_.join();
}

void TicksGenerator::start(std::uint32_t component_id, const std::chrono::seconds& period) {
  auto it = timer_handles_.find(component_id);
  if (it != timer_handles_.cend()) {
    timers_.reset(it->second, period);
  } else {
    const auto h = timers_.add(period, Timers::Timer::RECURRING, [this, component_id](Timers::Handle) {
      msgs_storage_->pushInternalMsg(TickInternalMsg{component_id});
    });
    timer_handles_.emplace(component_id, h);
  }
}

void TicksGenerator::stop(std::uint32_t component_id) {
  auto it = timer_handles_.find(component_id);
  if (it != timer_handles_.cend()) {
    timers_.cancel(it->second);
    timer_handles_.erase(it);
  }
}

void TicksGenerator::onInternalTick(const bftEngine::impl::TickInternalMsg& internal_tick) {
  auto it = component_pending_req_seq_nums_.find(internal_tick.component_id);
  if (it == component_pending_req_seq_nums_.cend()) {
    sendClientRequestMsgTick(internal_tick.component_id);
  } else {
    if (!pending_req_->isPending(bft_client_->getClientId(), it->second)) {
      component_pending_req_seq_nums_.erase(it);
      sendClientRequestMsgTick(internal_tick.component_id);
    }
  }
}

void TicksGenerator::run() {
  while (!stop_) {
    timers_.evaluate();
    std::this_thread::sleep_for(poll_period_);
  }
}

void TicksGenerator::sendClientRequestMsgTick(std::uint32_t component_id) {
  auto payload = std::vector<std::uint8_t>{};
  serialize(payload, ClientReqMsgTickPayload{component_id});
  const auto seq_num = bft_client_->sendRequest(
      bftEngine::TICK_FLAG, payload.size(), reinterpret_cast<const char*>(payload.data()), kTickCid);
  component_pending_req_seq_nums_[component_id] = seq_num;
}

void TicksGenerator::evaluateTimers(const std::chrono::steady_clock::time_point& now) { timers_.evaluate(now); }

}  // namespace concord::cron
