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
#include "Logger.hpp"
#include <future>

using std::queue;
using namespace std::chrono;
using namespace concord::diagnostics;

namespace bftEngine::impl {

IncomingMsgsStorageImp::IncomingMsgsStorageImp(std::shared_ptr<MsgHandlersRegistrator>& msgHandlersPtr,
                                               std::chrono::milliseconds msgWaitTimeout,
                                               uint16_t replicaId)
    : IncomingMsgsStorage(),
      msgHandlers_(msgHandlersPtr),
      msgWaitTimeout_(msgWaitTimeout),
      take_lock_recorder_(histograms_.take_lock),
      wait_for_cv_recorder_(histograms_.wait_for_cv) {
  replicaId_ = replicaId;
  ptrProtectedQueueForExternalMessages_ = new queue<std::unique_ptr<MessageBase>>();
  ptrProtectedQueueForInternalMessages_ = new queue<InternalMessage>();
  lastOverflowWarning_ = MinTime;
  ptrThreadLocalQueueForExternalMessages_ = new queue<std::unique_ptr<MessageBase>>();
  ptrThreadLocalQueueForInternalMessages_ = new queue<InternalMessage>();
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
    LOG_INFO(GL, "Dispatching thread stopped");
  }
}

// can be called by any thread
void IncomingMsgsStorageImp::pushExternalMsg(std::unique_ptr<MessageBase> msg) {
  std::unique_lock<std::mutex> mlock(lock_);
  if (ptrProtectedQueueForExternalMessages_->size() >= maxNumberOfPendingExternalMsgs_) {
    Time now = getMonotonicTime();
    auto msg_type = static_cast<MsgCode::Type>(msg->type());
    if ((now - lastOverflowWarning_) > (milliseconds(minTimeBetweenOverflowWarningsMilli_))) {
      LOG_WARN(GL, "Queue Full. Dropping some msgs." << KVLOG(maxNumberOfPendingExternalMsgs_, msg_type));
      lastOverflowWarning_ = now;
    }
    dropped_msgs++;
  } else {
    histograms_.dropped_msgs_in_a_row->record(dropped_msgs);
    dropped_msgs = 0;
    ptrProtectedQueueForExternalMessages_->push(std::move(msg));
    condVar_.notify_one();
  }
}

void IncomingMsgsStorageImp::pushExternalMsgRaw(char* msg, size_t& size) {
  size_t actualSize = 0;
  MessageBase* mb = MessageBase::deserializeMsg(msg, size, actualSize);
  pushExternalMsg(std::unique_ptr<MessageBase>(mb));
}

// can be called by any thread
void IncomingMsgsStorageImp::pushInternalMsg(InternalMessage&& msg) {
  std::unique_lock<std::mutex> mlock(lock_);
  ptrProtectedQueueForInternalMessages_->push(std::move(msg));
  condVar_.notify_one();
}

// should only be called by the dispatching thread
IncomingMsg IncomingMsgsStorageImp::getMsgForProcessing() {
  auto msg = popThreadLocal();
  if (msg.tag != IncomingMsg::INVALID) return msg;
  {
    take_lock_recorder_.start();
    std::unique_lock<std::mutex> mlock(lock_);
    take_lock_recorder_.end();
    {
      if (ptrProtectedQueueForExternalMessages_->empty() && ptrProtectedQueueForInternalMessages_->empty()) {
        wait_for_cv_recorder_.start();
        condVar_.wait_for(mlock, msgWaitTimeout_);
        wait_for_cv_recorder_.end();
      }

      // no new message
      if (ptrProtectedQueueForExternalMessages_->empty() && ptrProtectedQueueForInternalMessages_->empty()) {
        return IncomingMsg();
      }

      // swap queues
      std::queue<std::unique_ptr<MessageBase>>* t1 = ptrThreadLocalQueueForExternalMessages_;
      ptrThreadLocalQueueForExternalMessages_ = ptrProtectedQueueForExternalMessages_;
      ptrProtectedQueueForExternalMessages_ = t1;
      histograms_.external_queue_len_at_swap->record(ptrThreadLocalQueueForExternalMessages_->size());

      auto* t2 = ptrThreadLocalQueueForInternalMessages_;
      ptrThreadLocalQueueForInternalMessages_ = ptrProtectedQueueForInternalMessages_;
      ptrProtectedQueueForInternalMessages_ = t2;
      histograms_.internal_queue_len_at_swap->record(ptrThreadLocalQueueForInternalMessages_->size());
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
  MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(replicaId_));
  MDC_PUT(MDC_THREAD_KEY, "message-processing");
  try {
    while (!stopped_) {
      auto msg = getMsgForProcessing();
      {
        TimeRecorder scoped_timer(*histograms_.evaluate_timers);
        timers_.evaluate();
      }

      MessageBase* message = nullptr;
      MsgHandlerCallback msgHandlerCallback = nullptr;
      switch (msg.tag) {
        case IncomingMsg::INVALID:
          LOG_TRACE(GL, "Invalid message - ignore");
          break;
        case IncomingMsg::EXTERNAL:
          // TODO: (AJS) Don't turn this back into a raw pointer.
          // Pass the smart pointer through the message handlers so they take ownership.
          message = msg.external.release();
          msgHandlerCallback = msgHandlers_->getCallback(message->type());
          if (msgHandlerCallback) {
            msgHandlerCallback(message);
          } else {
            LOG_WARN(
                GL,
                "Received unknown external Message: " << KVLOG(message->type(), message->senderId(), message->size()));
            delete message;
          }
          break;
        case IncomingMsg::INTERNAL:
          msgHandlers_->handleInternalMsg(std::move(msg.internal));
      };
    }
  } catch (const std::exception& e) {
    LOG_FATAL(GL, "Exception: " << e.what() << "exiting ...");
    std::terminate();
  }
}

}  // namespace bftEngine::impl
