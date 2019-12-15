// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

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
  explicit IncomingMsgsStorageImp();
  ~IncomingMsgsStorageImp() override;

  // can be called by any thread
  void pushExternalMsg(std::unique_ptr<MessageBase> msg) override;

  // can be called by any thread
  void pushInternalMsg(std::unique_ptr<InternalMessage> msg) override;

  // should only be called by the main thread
  IncomingMsg popMsgForProcessing(std::chrono::milliseconds timeout) override;

 private:
  IncomingMsg popThreadLocal();

 private:
  const uint64_t minTimeBetweenOverflowWarningsMilli_ = 5 * 1000;  // 5 seconds
  const uint16_t maxNumberOfPendingExternalMsgs_ = 20000;

  std::mutex lock_;
  std::condition_variable condVar_;

  // new messages are pushed to ptrProtectedQueue.... ; Protected by lock
  std::queue<std::unique_ptr<MessageBase>>* ptrProtectedQueueForExternalMessages_;
  std::queue<std::unique_ptr<InternalMessage>>* ptrProtectedQueueForInternalMessages_;

  // time of last queue overflow  Protected by lock
  Time lastOverflowWarning_;

  // messages are fetched from ptrThreadLocalQueue... ; should only be accessed by the main thread
  std::queue<std::unique_ptr<MessageBase>>* ptrThreadLocalQueueForExternalMessages_;
  std::queue<std::unique_ptr<InternalMessage>>* ptrThreadLocalQueueForInternalMessages_;
};

}  // namespace bftEngine::impl
