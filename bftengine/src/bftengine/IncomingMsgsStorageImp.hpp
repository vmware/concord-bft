// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "IncomingMsgsStorage.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "Timers.hpp"
#include "diagnostics.h"
#include "performance_handler.h"

#include <queue>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>

namespace bftEngine::impl {

class IncomingMsgsStorageImp : public IncomingMsgsStorage {
 public:
  explicit IncomingMsgsStorageImp(std::shared_ptr<MsgHandlersRegistrator>& msgHandlersPtr,
                                  std::chrono::milliseconds msgWaitTimeout,
                                  uint16_t replicaId);
  ~IncomingMsgsStorageImp() override;

  void start() override;
  void stop() override;

  // Can be called by any thread
  void pushExternalMsg(std::unique_ptr<MessageBase> msg) override;

  // Can be called by any thread
  void pushInternalMsg(InternalMessage&& msg) override;

  [[nodiscard]] bool isRunning() const override { return dispatcherThread_.joinable(); }

  auto& timers() { return timers_; }

 private:
  void dispatchMessages(std::promise<void>& signalStarted);
  IncomingMsg getMsgForProcessing();
  IncomingMsg popThreadLocal();

 private:
  const uint64_t minTimeBetweenOverflowWarningsMilli_ = 5 * 1000;
  const uint16_t maxNumberOfPendingExternalMsgs_ = 20000;

  uint16_t replicaId_;

  std::mutex lock_;
  std::condition_variable condVar_;

  std::shared_ptr<MsgHandlersRegistrator> msgHandlers_;
  std::chrono::milliseconds msgWaitTimeout_;

  // New messages are pushed to ptrProtectedQueue.... ; protected by lock
  std::queue<std::unique_ptr<MessageBase>>* ptrProtectedQueueForExternalMessages_;
  std::queue<InternalMessage>* ptrProtectedQueueForInternalMessages_;

  // Time of last queue overflow; protected by lock
  Time lastOverflowWarning_;

  // Messages are fetched from ptrThreadLocalQueue...; should be accessed only by the dispatching thread
  std::queue<std::unique_ptr<MessageBase>>* ptrThreadLocalQueueForExternalMessages_;
  std::queue<InternalMessage>* ptrThreadLocalQueueForInternalMessages_;

  std::thread dispatcherThread_;
  std::promise<void> signalStarted_;
  std::atomic<bool> stopped_ = false;
  concordUtil::Timers timers_;

  // 5 seconds
  static constexpr int64_t MAX_VALUE_NANOSECONDS = 1000 * 1000 * 1000 * 5l;
  using Recorder = concord::diagnostics::Recorder;
  struct Recorders {
    Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.registerComponent(
          "incomingMsgsStorageImp", {external_queue_len_at_swap, internal_queue_len_at_swap, get_msg_for_processing});
    }
    DEFINE_SHARED_RECORDER(external_queue_len_at_swap, 1, 10000, 3, concord::diagnostics::Unit::COUNT);

    DEFINE_SHARED_RECORDER(internal_queue_len_at_swap, 1, 10000, 3, concord::diagnostics::Unit::COUNT);

    DEFINE_SHARED_RECORDER(
        get_msg_for_processing, 1, MAX_VALUE_NANOSECONDS, 3, concord::diagnostics::Unit::NANOSECONDS);
  };
  Recorders histograms_;
};

}  // namespace bftEngine::impl
