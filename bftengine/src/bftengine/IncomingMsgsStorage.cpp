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

#include "IncomingMsgsStorage.hpp"
#include "messages/MessageBase.hpp"
#include "Logger.hpp"
#include <chrono>

using std::queue;
using namespace std::chrono;

namespace bftEngine {
namespace impl {

IncomingMsgsStorage::IncomingMsgsStorage(uint16_t maxNumOfPendingExternalMsgs)
    : maxNumberOfPendingExternalMsgs{maxNumOfPendingExternalMsgs} {
  ptrProtectedQueueForExternalMessages = new queue<std::unique_ptr<MessageBase>>();
  ptrProtectedQueueForInternalMessages = new queue<std::unique_ptr<InternalMessage>>();

  lastOverflowWarning = MinTime;

  ptrThreadLocalQueueForExternalMessages = new queue<std::unique_ptr<MessageBase>>();
  ptrThreadLocalQueueForInternalMessages = new queue<std::unique_ptr<InternalMessage>>();
}

IncomingMsgsStorage::~IncomingMsgsStorage() {
  delete ptrProtectedQueueForExternalMessages;
  delete ptrProtectedQueueForInternalMessages;
  delete ptrThreadLocalQueueForExternalMessages;
  delete ptrThreadLocalQueueForInternalMessages;
}

// can be called by any thread
void IncomingMsgsStorage::pushExternalMsg(std::unique_ptr<MessageBase> m) {
  std::unique_lock<std::mutex> mlock(lock);
  {
    if (ptrProtectedQueueForExternalMessages->size() >= maxNumberOfPendingExternalMsgs) {
      Time now = getMonotonicTime();
      if ((now - lastOverflowWarning) > (milliseconds(minTimeBetweenOverflowWarningsMilli))) {
        LOG_WARN_F(GL,
                   "More than %d pending messages in queue -  may ignore some "
                   "of the messages!",
                   (int)maxNumberOfPendingExternalMsgs);

        lastOverflowWarning = now;
      }
    } else {
      ptrProtectedQueueForExternalMessages->push(std::move(m));
      condVar.notify_one();
    }
  }
}

// can be called by any thread
void IncomingMsgsStorage::pushInternalMsg(std::unique_ptr<InternalMessage> m) {
  std::unique_lock<std::mutex> mlock(lock);
  {
    ptrProtectedQueueForInternalMessages->push(std::move(m));
    condVar.notify_one();
  }
}

// should only be called by the main thread
IncomingMsg IncomingMsgsStorage::pop(std::chrono::milliseconds timeout) {
  auto msg = popThreadLocal();
  if (msg.tag != IncomingMsg::INVALID) {
    return msg;
  }

  {
    std::unique_lock<std::mutex> mlock(lock);

    {
      if (ptrProtectedQueueForExternalMessages->empty() && ptrProtectedQueueForInternalMessages->empty())
        condVar.wait_for(mlock, timeout);

      // no new message
      if (ptrProtectedQueueForExternalMessages->empty() && ptrProtectedQueueForInternalMessages->empty()) {
        return IncomingMsg();
      }

      // swap queues

      std::queue<std::unique_ptr<MessageBase>>* t1 = ptrThreadLocalQueueForExternalMessages;
      ptrThreadLocalQueueForExternalMessages = ptrProtectedQueueForExternalMessages;
      ptrProtectedQueueForExternalMessages = t1;

      std::queue<std::unique_ptr<InternalMessage>>* t2 = ptrThreadLocalQueueForInternalMessages;
      ptrThreadLocalQueueForInternalMessages = ptrProtectedQueueForInternalMessages;
      ptrProtectedQueueForInternalMessages = t2;
    }
  }

  return popThreadLocal();
}

// should only be called by the main thread.
bool IncomingMsgsStorage::empty() {
  if (!ptrThreadLocalQueueForExternalMessages->empty() || !ptrThreadLocalQueueForInternalMessages->empty())
    return false;

  {
    std::unique_lock<std::mutex> mlock(lock);
    { return (ptrProtectedQueueForExternalMessages->empty() && ptrProtectedQueueForInternalMessages->empty()); }
  }
}

IncomingMsg IncomingMsgsStorage::popThreadLocal() {
  if (!ptrThreadLocalQueueForInternalMessages->empty()) {
    auto item = IncomingMsg(std::move(ptrThreadLocalQueueForInternalMessages->front()));
    ptrThreadLocalQueueForInternalMessages->pop();
    return item;
  } else if (!ptrThreadLocalQueueForExternalMessages->empty()) {
    auto item = IncomingMsg(std::move(ptrThreadLocalQueueForExternalMessages->front()));
    ptrThreadLocalQueueForExternalMessages->pop();
    return item;
  } else {
    return IncomingMsg();
  }
}

}  // namespace impl
}  // namespace bftEngine
