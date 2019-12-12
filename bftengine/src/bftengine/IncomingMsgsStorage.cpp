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
#include "Logger.hpp"

using std::queue;
using namespace std::chrono;

namespace bftEngine::impl {

IncomingMsgsStorage::IncomingMsgsStorage(uint16_t maxNumOfPendingExternalMsgs)
    : maxNumberOfPendingExternalMsgs_{maxNumOfPendingExternalMsgs} {
  ptrProtectedQueueForExternalMessages_ = new queue<std::unique_ptr<MessageBase>>();
  ptrProtectedQueueForInternalMessages_ = new queue<std::unique_ptr<InternalMessage>>();

  lastOverflowWarning_ = MinTime;

  ptrThreadLocalQueueForExternalMessages_ = new queue<std::unique_ptr<MessageBase>>();
  ptrThreadLocalQueueForInternalMessages_ = new queue<std::unique_ptr<InternalMessage>>();
}

IncomingMsgsStorage::~IncomingMsgsStorage() {
  delete ptrProtectedQueueForExternalMessages_;
  delete ptrProtectedQueueForInternalMessages_;
  delete ptrThreadLocalQueueForExternalMessages_;
  delete ptrThreadLocalQueueForInternalMessages_;
}

// can be called by any thread
void IncomingMsgsStorage::pushExternalMsg(std::unique_ptr<MessageBase> m) {
  std::unique_lock<std::mutex> mlock(lock_);
  {
    if (ptrProtectedQueueForExternalMessages_->size() >= maxNumberOfPendingExternalMsgs_) {
      Time now = getMonotonicTime();
      if ((now - lastOverflowWarning_) > (milliseconds(minTimeBetweenOverflowWarningsMilli_))) {
        LOG_WARN_F(GL,
                   "More than %d pending messages in queue -  may ignore some "
                   "of the messages!",
                   (int)maxNumberOfPendingExternalMsgs_);
        lastOverflowWarning_ = now;
      }
    } else {
      ptrProtectedQueueForExternalMessages_->push(std::move(m));
      condVar_.notify_one();
    }
  }
}

// can be called by any thread
void IncomingMsgsStorage::pushInternalMsg(std::unique_ptr<InternalMessage> msg) {
  std::unique_lock<std::mutex> mlock(lock_);
  {
    ptrProtectedQueueForInternalMessages_->push(std::move(msg));
    condVar_.notify_one();
  }
}

// should only be called by the main thread
IncomingMsg IncomingMsgsStorage::popInternalOrExternalMsg(std::chrono::milliseconds timeout) {
  auto msg = popThreadLocal();
  if (msg.tag != IncomingMsg::INVALID) return msg;

  {
    std::unique_lock<std::mutex> mlock(lock_);
    {
      if (ptrProtectedQueueForExternalMessages_->empty() && ptrProtectedQueueForInternalMessages_->empty())
        condVar_.wait_for(mlock, timeout);

      // no new message
      if (ptrProtectedQueueForExternalMessages_->empty() && ptrProtectedQueueForInternalMessages_->empty()) {
        return IncomingMsg();
      }

      // swap queues
      std::queue<std::unique_ptr<MessageBase>>* t1 = ptrThreadLocalQueueForExternalMessages_;
      ptrThreadLocalQueueForExternalMessages_ = ptrProtectedQueueForExternalMessages_;
      ptrProtectedQueueForExternalMessages_ = t1;

      std::queue<std::unique_ptr<InternalMessage>>* t2 = ptrThreadLocalQueueForInternalMessages_;
      ptrThreadLocalQueueForInternalMessages_ = ptrProtectedQueueForInternalMessages_;
      ptrProtectedQueueForInternalMessages_ = t2;
    }
  }

  return popThreadLocal();
}

IncomingMsg IncomingMsgsStorage::popThreadLocal() {
  if (!ptrThreadLocalQueueForInternalMessages_->empty()) {
    auto item = IncomingMsg(std::move(ptrThreadLocalQueueForInternalMessages_->front()));
    ptrThreadLocalQueueForInternalMessages_->pop();
    return item;
  } else if (!ptrThreadLocalQueueForExternalMessages_->empty()) {
    auto item = IncomingMsg(std::move(ptrThreadLocalQueueForExternalMessages_->front()));
    ptrThreadLocalQueueForExternalMessages_->pop();
    return item;
  } else {
    return IncomingMsg();
  }
}

}  // namespace bftEngine::impl
