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

#include <queue>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>

namespace bftEngine::impl {

// This is needed because we can't safely cast unique_ptrs to void pointers
// We also can't use a union because it would require custom deleters and
// could possibly result in unsafe destruction.
//
// As a result, we have the overhead of an extra pointer here. However, the
// structure is on the stack and only one is active at a time in the main loop,
// so the overhead is negligible. One of the unique_ptrs will be moved out for
// use depending upon the tag.
//
// This is probably a good use case for std::variant, but we are on c++11 and
// variant is only available in c++17.

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
  void pushInternalMsg(std::unique_ptr<InternalMessage> msg) override;

  [[nodiscard]] bool isRunning() const override { return dispatcherThread_.joinable(); }

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
  std::queue<std::unique_ptr<InternalMessage>>* ptrProtectedQueueForInternalMessages_;

  // Time of last queue overflow; protected by lock
  Time lastOverflowWarning_;

  // Messages are fetched from ptrThreadLocalQueue...; should be accessed only by the dispatching thread
  std::queue<std::unique_ptr<MessageBase>>* ptrThreadLocalQueueForExternalMessages_;
  std::queue<std::unique_ptr<InternalMessage>>* ptrThreadLocalQueueForInternalMessages_;

  std::thread dispatcherThread_;
  std::promise<void> signalStarted_;
  std::atomic<bool> stopped_ = false;
};

}  // namespace bftEngine::impl
