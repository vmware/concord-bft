// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <queue>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>
#include "TimeUtils.hpp"

namespace bftEngine {
namespace impl {

class MessageBase;
class InternalMessage;

using std::queue;

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

class IncomingMsg {
 public:
  enum { EXTERNAL, INTERNAL, INVALID } tag;

  IncomingMsg() : tag(IncomingMsg::INVALID) {}
  IncomingMsg(std::unique_ptr<MessageBase> m) : tag(IncomingMsg::EXTERNAL), external(std::move(m)) {}
  IncomingMsg(std::unique_ptr<InternalMessage> m) : tag(IncomingMsg::INTERNAL), internal(std::move(m)) {}

  std::unique_ptr<MessageBase> external;
  std::unique_ptr<InternalMessage> internal;
};

class IncomingMsgsStorage {
 public:
  const uint64_t minTimeBetweenOverflowWarningsMilli = 5 * 1000;  // 5 seconds

  IncomingMsgsStorage(uint16_t maxNumOfPendingExternalMsgs);
  ~IncomingMsgsStorage();

  // can be called by any thread
  void pushExternalMsg(std::unique_ptr<MessageBase> m);

  // can be called by any thread
  void pushInternalMsg(std::unique_ptr<InternalMessage> m);

  // should only be called by the main thread
  IncomingMsg pop(std::chrono::milliseconds timeout);

  // should only be called by the main thread.
  bool empty();

 protected:
  IncomingMsg popThreadLocal();

  const uint16_t maxNumberOfPendingExternalMsgs;

  std::mutex lock;
  std::condition_variable condVar;

  // new messages are pushed to ptrProtectedQueue.... ; Protected by lock
  queue<std::unique_ptr<MessageBase>>* ptrProtectedQueueForExternalMessages;
  queue<std::unique_ptr<InternalMessage>>* ptrProtectedQueueForInternalMessages;

  // time of last queue overflow  Protected by lock
  Time lastOverflowWarning;

  // messages are fetched from ptrThreadLocalQueue... ; should only be accessed
  // by the main thread
  queue<std::unique_ptr<MessageBase>>* ptrThreadLocalQueueForExternalMessages;
  queue<std::unique_ptr<InternalMessage>>* ptrThreadLocalQueueForInternalMessages;
};

}  // namespace impl
}  // namespace bftEngine
