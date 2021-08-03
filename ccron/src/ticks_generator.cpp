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

std::shared_ptr<TicksGenerator> TicksGenerator::create(
    const std::shared_ptr<bftEngine::impl::IInternalBFTClient>& bft_client,
    const IPendingRequest& pending_req,
    const std::shared_ptr<IncomingMsgsStorage>& msgs_storage,
    const std::chrono::seconds& poll_period) {
  return std::shared_ptr<TicksGenerator>{new TicksGenerator{bft_client, pending_req, msgs_storage, poll_period}};
}

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
  auto lock = std::scoped_lock{mtx_};
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
  auto lock = std::scoped_lock{mtx_};
  auto it = timer_handles_.find(component_id);
  if (it != timer_handles_.cend()) {
    timers_.cancel(it->second);
    timer_handles_.erase(it);
  }
}

bool TicksGenerator::isGenerating(std::uint32_t component_id) const {
  auto lock = std::scoped_lock{mtx_};
  return (timer_handles_.find(component_id) != timer_handles_.cend());
}

void TicksGenerator::onInternalTick(const bftEngine::impl::TickInternalMsg& internal_tick) {
  // No need to lock the mutex as this method is expected to be called from the messaging thread only.
  auto ext_queue_it = pending_ticks_in_ext_queue_.find(internal_tick.component_id);
  auto req_it = pending_tick_requests_.find(internal_tick.component_id);
  // The path of a tick request is the following:
  //
  // -----------------------------------------------------------------------------------------------------------
  //                                                         onTickPoppedFromExtQueue()
  //                                                                    |
  //                                                                    v
  //  ticks gen -> internal queue -> onInternalTick() -> external queue -> onMessage() -> consensus -> execute()
  //                                                                            ^
  //                                                                            |
  //                                                             ClientsManager::addPendingRequest()
  // -----------------------------------------------------------------------------------------------------------
  //
  // The first time this method is called for a particular component ID, we don't have anything in the external queue or
  // in the ClientsManager. Therefore, nothing is pending and we can omit the isPending() check and directly send.
  // If not in the external queue, it might be pending or not and, therefore, we use isPending() to check.
  if (ext_queue_it == pending_ticks_in_ext_queue_.cend() && req_it == pending_tick_requests_.cend()) {
    sendClientRequestMsgTick(internal_tick.component_id);
  } else if (ext_queue_it == pending_ticks_in_ext_queue_.cend()) {
    // If the tick for the given component ID is not in the external queue, check if it is pending. If yes, don't send.
    if (!pending_req_->isPending(bft_client_->getClientId(), req_it->second)) {
      sendClientRequestMsgTick(internal_tick.component_id);
    }
  }
}

void TicksGenerator::onTickPoppedFromExtQueue(std::uint32_t component_id) {
  // No need to lock the mutex as this method is expected to be called from the messaging thread only.
  pending_ticks_in_ext_queue_.erase(component_id);
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
  const auto seq_num =
      bft_client_->sendRequest(bftEngine::TICK_FLAG,
                               payload.size(),
                               reinterpret_cast<const char*>(payload.data()),
                               kTickCid,
                               [p = shared_from_this(), component_id]() { p->onTickPoppedFromExtQueue(component_id); });
  pending_tick_requests_[component_id] = seq_num;
  pending_ticks_in_ext_queue_.insert(component_id);
}

void TicksGenerator::evaluateTimers(const std::chrono::steady_clock::time_point& now) { timers_.evaluate(now); }

}  // namespace concord::cron
