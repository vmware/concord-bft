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

#pragma once

#include "IPendingRequest.hpp"
#include "InternalBFTClient.hpp"
#include "messages/SignatureInternalMsgs.hpp"
#include "Timers.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace concord::cron {

// Generates ticks periodically and, therefore, triggers the cron table even if there is no traffic in the system.
class TicksGenerator {
 public:
  TicksGenerator(const std::shared_ptr<bftEngine::impl::IInternalBFTClient> &bft_client,
                 const IPendingRequest &pending_req,
                 const std::shared_ptr<IncomingMsgsStorage> &msgs_storage,
                 const std::chrono::seconds &poll_period);
  ~TicksGenerator();

  static inline const std::string kTickCid{"__concord__internal__tick__"};

 public:
  // The start(), stop() and isGenerating() methods can be called from multiple threads without explicit synchronization
  // by users.

  // Start tick generation for `component_id` with the given `period`.
  // Updates the period if generation for `component_id` has already been started.
  void start(std::uint32_t component_id, const std::chrono::seconds &period);
  void stop(std::uint32_t component_id);
  bool isGenerating(std::uint32_t component_id) const;

 public:
  // Called by the main replica thread *only* on receiving a tick as an internal message.
  // Do not call from other threads.
  void onInternalTick(const bftEngine::impl::TickInternalMsg &);

 private:
  // Called in an internal thread to generate ticks.
  void run();

  // Sends a ClientRequestMsg with a ClientReqMsgTickPayload.
  void sendClientRequestMsgTick(std::uint32_t component_id);

 private:
  const std::shared_ptr<bftEngine::impl::IInternalBFTClient> bft_client_;
  const IPendingRequest *pending_req_{nullptr};
  const std::shared_ptr<IncomingMsgsStorage> msgs_storage_;

  // Keep a map of pending tick request sequence numbers per component ID:
  // component_id -> pending tick request sequence number
  // Needed for throttling ticks per component.
  std::map<std::uint32_t, std::uint64_t> component_pending_req_seq_nums_;

  std::thread thread_;
  std::atomic_bool stop_{false};
  const std::chrono::seconds poll_period_{1};

  concordUtil::Timers timers_;
  // The mutex ensures multiple user threads can call public methods and modify the timer handles map. It doesn't
  // protect timers themselves as there is no need - they can be called concurrently.
  mutable std::mutex mtx_;
  // component_id -> timer handler
  std::map<std::uint32_t, concordUtil::Timers::Handle> timer_handles_;

  // Following methods are for testing only, do not use in production.
 protected:
  void evaluateTimers(const std::chrono::steady_clock::time_point &now);

  struct DoNotStartThread {};
  TicksGenerator(const std::shared_ptr<bftEngine::impl::IInternalBFTClient> &bft_client,
                 const IPendingRequest &pending_req,
                 const std::shared_ptr<IncomingMsgsStorage> &msgs_storage,
                 DoNotStartThread);
};

}  // namespace concord::cron
