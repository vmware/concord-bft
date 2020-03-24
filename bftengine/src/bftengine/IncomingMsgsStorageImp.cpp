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

#include "IncomingMsgsStorageImp.hpp"
#include "messages/InternalMessage.hpp"
#include "TimersSingleton.hpp"
#include "Logger.hpp"
#include <future>

using std::queue;
using namespace std::chrono;

namespace bftEngine::impl {

IncomingMsgsStorageImp::IncomingMsgsStorageImp(std::shared_ptr<MsgHandlersRegistrator>& msgHandlersPtr,
                                               std::chrono::milliseconds msgWaitTimeout,
                                               uint16_t replicaId)
    : IncomingMsgsStorage(), msgHandlers_(msgHandlersPtr), msgWaitTimeout_(msgWaitTimeout) {
  replicaId_ = replicaId;
  ptrProtectedQueueForExternalMessages_ = new queue<std::unique_ptr<MessageBase>>();
  ptrProtectedQueueForInternalMessages_ = new queue<std::unique_ptr<InternalMessage>>();
  lastOverflowWarning_ = MinTime;
  ptrThreadLocalQueueForExternalMessages_ = new queue<std::unique_ptr<MessageBase>>();
  ptrThreadLocalQueueForInternalMessages_ = new queue<std::unique_ptr<InternalMessage>>();
}

IncomingMsgsStorageImp::~IncomingMsgsStorageImp() {
  delete ptrProtectedQueueForExternalMessages_;
  delete ptrProtectedQueueForInternalMessages_;
  delete ptrThreadLocalQueueForExternalMessages_;
  delete ptrThreadLocalQueueForInternalMessages_;
}

void IncomingMsgsStorageImp::start() {
  if (!dispatcherThread_.joinable()) {
    std::future<void> futureObj = signalStarted_.get_future();
    dispatcherThread_ = std::thread([=] { dispatchMessages(signalStarted_); });
    // Wait until thread starts
    futureObj.get();
  };
}

void IncomingMsgsStorageImp::stop() {
  if (dispatcherThread_.joinable()) {
    stopped_ = true;
    dispatcherThread_.join();
    LOG_INFO_F(GL, "Dispatching thread stopped");
  }
}

// can be called by any thread
void IncomingMsgsStorageImp::pushExternalMsg(std::unique_ptr<MessageBase> msg) {
  std::unique_lock<std::mutex> mlock(lock_);
  if (ptrProtectedQueueForExternalMessages_->size() >= maxNumberOfPendingExternalMsgs_) {
    Time now = getMonotonicTime();
    if ((now - lastOverflowWarning_) > (milliseconds(minTimeBetweenOverflowWarningsMilli_))) {
      LOG_WARN_F(GL,
                 "More than %d pending messages in queue - may ignore some of the messages!",
                 (int)maxNumberOfPendingExternalMsgs_);
      lastOverflowWarning_ = now;
    }
  } else {
    ptrProtectedQueueForExternalMessages_->push(std::move(msg));
    condVar_.notify_one();
  }
}

// can be called by any thread
void IncomingMsgsStorageImp::pushInternalMsg(std::unique_ptr<InternalMessage> msg) {
  std::unique_lock<std::mutex> mlock(lock_);
  ptrProtectedQueueForInternalMessages_->push(std::move(msg));
  condVar_.notify_one();
}

// should only be called by the dispatching thread
IncomingMsg IncomingMsgsStorageImp::getMsgForProcessing() {
  auto msg = popThreadLocal();
  if (msg.tag != IncomingMsg::INVALID) return msg;
  {
    std::unique_lock<std::mutex> mlock(lock_);
    {
      if (ptrProtectedQueueForExternalMessages_->empty() && ptrProtectedQueueForInternalMessages_->empty())
        condVar_.wait_for(mlock, msgWaitTimeout_);

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

IncomingMsg IncomingMsgsStorageImp::popThreadLocal() {
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

void IncomingMsgsStorageImp::dispatchMessages(std::promise<void>& signalStarted) {
  signalStarted.set_value();
  std::stringstream rid;
  rid << replicaId_;
  MDC_PUT(GL, "rid", rid.str());
  while (!stopped_) {
    auto msg = getMsgForProcessing();
    TimersSingleton::getInstance().evaluate();

    MessageBase* message = nullptr;
    MsgHandlerCallback msgHandlerCallback = nullptr;
    switch (msg.tag) {
      case IncomingMsg::INVALID:
        LOG_TRACE_F(GL, "Invalid message - ignore");
        break;
      case IncomingMsg::EXTERNAL:
        // TODO: (AJS) Don't turn this back into a raw pointer.
        // Pass the smart pointer through the message handlers so they take ownership.
        message = msg.external.release();
        msgHandlerCallback = msgHandlers_->getCallback(message->type());
        if (msgHandlerCallback != nullptr) {
          try {
            msgHandlerCallback(message);
          } catch (std::exception& e) {
            LOG_WARN(GL, e.what());
            delete message;
          }
        } else {
          LOG_WARN_F(GL, "Unknown message - delete");
          delete message;
        }
        break;
      case IncomingMsg::INTERNAL:
        msg.internal->handle();
    };
  }
}

}  // namespace bftEngine::impl
