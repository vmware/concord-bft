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
#include "MessageBase.hpp"
#include "Logger.hpp"

using std::queue;

namespace bftEngine {
namespace impl {

IncomingMsgsStorage::IncomingMsgsStorage(uint16_t maxNumOfPendingExternalMsgs)
    : maxNumberOfPendingExternalMsgs{maxNumOfPendingExternalMsgs} {
  ptrProtectedQueueForExternalMessages = new queue<MessageBase*>();
  ptrProtectedQueueForInternalMessages = new queue<InternalMessage*>();

  lastOverflowWarning = MinTime;

  ptrThreadLocalQueueForExternalMessages = new queue<MessageBase*>();
  ptrThreadLocalQueueForInternalMessages = new queue<InternalMessage*>();
}

IncomingMsgsStorage::~IncomingMsgsStorage() {
  delete ptrProtectedQueueForExternalMessages;
  delete ptrProtectedQueueForInternalMessages;
  delete ptrThreadLocalQueueForExternalMessages;
  delete ptrThreadLocalQueueForInternalMessages;
}

// can be called by any thread
void IncomingMsgsStorage::pushExternalMsg(MessageBase* m) {
  std::unique_lock<std::mutex> mlock(lock);
  {
    if (ptrProtectedQueueForExternalMessages->size() >=
        maxNumberOfPendingExternalMsgs) {
      Time n = getMonotonicTime();
      if (subtract(n, lastOverflowWarning) >
          ((TimeDeltaMirco)minTimeBetweenOverflowWarningsMilli * 1000)) {
        LOG_WARN_F(GL,
                   "More than %d pending messages in queue -  may ignore some "
                   "of the messages!",
                   (int)maxNumberOfPendingExternalMsgs);

        lastOverflowWarning = n;
      }

      // ignore message
      delete m;
    } else {
      ptrProtectedQueueForExternalMessages->push(m);
      condVar.notify_one();
    }
  }
}

// can be called by any thread
void IncomingMsgsStorage::pushInternalMsg(InternalMessage* m) {
  std::unique_lock<std::mutex> mlock(lock);
  {
    ptrProtectedQueueForInternalMessages->push(m);
    condVar.notify_one();
  }
}

// should only be called by the main thread
bool IncomingMsgsStorage::pop(void*& item,
                              bool& external,
                              std::chrono::milliseconds timeout) {
  if (popThreadLocal(item, external)) {
    return true;
  }

  {
    std::unique_lock<std::mutex> mlock(lock);

    {
      if (ptrProtectedQueueForExternalMessages->empty() &&
          ptrProtectedQueueForInternalMessages->empty())
        condVar.wait_for(mlock, timeout);

      // no new message
      if (ptrProtectedQueueForExternalMessages->empty() &&
          ptrProtectedQueueForInternalMessages->empty())
        return false;

      // swap queues

      std::queue<MessageBase*>* t1 = ptrThreadLocalQueueForExternalMessages;
      ptrThreadLocalQueueForExternalMessages =
          ptrProtectedQueueForExternalMessages;
      ptrProtectedQueueForExternalMessages = t1;

      std::queue<InternalMessage*>* t2 = ptrThreadLocalQueueForInternalMessages;
      ptrThreadLocalQueueForInternalMessages =
          ptrProtectedQueueForInternalMessages;
      ptrProtectedQueueForInternalMessages = t2;
    }
  }

  return popThreadLocal(item, external);
}

// should only be called by the main thread.
bool IncomingMsgsStorage::empty() {
  if (!ptrThreadLocalQueueForExternalMessages->empty() ||
      !ptrThreadLocalQueueForInternalMessages->empty())
    return false;

  {
    std::unique_lock<std::mutex> mlock(lock);
    {
      return (ptrProtectedQueueForExternalMessages->empty() &&
              ptrProtectedQueueForInternalMessages->empty());
    }
  }
}

bool IncomingMsgsStorage::popThreadLocal(void*& item, bool& external) {
  if (!ptrThreadLocalQueueForInternalMessages->empty()) {
    InternalMessage* iMsg = ptrThreadLocalQueueForInternalMessages->front();
    ptrThreadLocalQueueForInternalMessages->pop();
    item = (void*)iMsg;
    external = false;
    return true;
  } else if (!ptrThreadLocalQueueForExternalMessages->empty()) {
    MessageBase* eMsg = ptrThreadLocalQueueForExternalMessages->front();
    ptrThreadLocalQueueForExternalMessages->pop();
    item = (void*)eMsg;
    external = true;
    return true;
  } else {
    return false;
  }
}

}  // namespace impl
}  // namespace bftEngine
