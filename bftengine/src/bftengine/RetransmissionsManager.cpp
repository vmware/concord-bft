// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <forward_list>
#include <unordered_map>
#include <algorithm>
#include <queue>
#include <cmath>
#include <chrono>

#include "RetransmissionsManager.hpp"
#include "messages/MessageBase.hpp"
#include "SimpleThreadPool.hpp"
#include "RetransmissionLogic.hpp"
#include "RetransmissionProcessingJob.hpp"
#include "assertUtils.hpp"

using namespace std;
using namespace std::chrono;

namespace bftEngine::impl {

RetransmissionsManager::RetransmissionsManager(InternalReplicaApi* replica,
                                               util::SimpleThreadPool* threadPool,
                                               shared_ptr<MsgsCommunicator>& msgsCommunicator,
                                               uint16_t maxOutNumOfSeqNumbers,
                                               SeqNum lastStableSeqNum)
    : replica_{replica},
      threadPool_{threadPool},
      msgsCommunicator_{msgsCommunicator},
      maxOutSeqNumbers_{maxOutNumOfSeqNumbers},
      internalLogicInfo_{new RetransmissionLogic(maxOutNumOfSeqNumbers)} {
  Assert(threadPool != nullptr);
  Assert(maxOutNumOfSeqNumbers > 0);

  lastStable_ = lastStableSeqNum;
}

RetransmissionsManager::RetransmissionsManager()
    : replica_{nullptr}, threadPool_{nullptr}, maxOutSeqNumbers_{0}, internalLogicInfo_{nullptr}, lastStable_{0} {}

RetransmissionsManager::~RetransmissionsManager() {
  if (threadPool_ == nullptr) return;  // if disabled

  // TODO(GG): make sure that threadPool has been stopped

  delete setOfEvents_;
  delete (RetransmissionLogic*)internalLogicInfo_;
}

void RetransmissionsManager::onSend(uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType, bool ignorePreviousAcks) {
  if (threadPool_ == nullptr) return;  // if disabled

  if ((msgSeqNum <= lastStable_) || (msgSeqNum > lastStable_ + maxOutSeqNumbers_)) return;

  RetransmissionEvent event;
  event.eventType =
      (!ignorePreviousAcks ? RetransmissionEventType::SENT : RetransmissionEventType::SENT_AND_IGNORE_PREV);
  event.time = getMonotonicTime();
  event.replicaId = replicaId;
  event.msgSeqNum = msgSeqNum;
  event.msgType = msgType;

  add(event);
}

void RetransmissionsManager::onAck(uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType) {
  if (threadPool_ == nullptr) return;  // if disabled

  if ((msgSeqNum <= lastStable_) || (msgSeqNum > lastStable_ + maxOutSeqNumbers_)) return;

  RetransmissionEvent event;
  event.eventType = RetransmissionEventType::ACK;
  event.time = getMonotonicTime();
  event.replicaId = replicaId;
  event.msgSeqNum = msgSeqNum;
  event.msgType = msgType;

  add(event);
}

void RetransmissionsManager::add(const RetransmissionEvent& e) {
  if (setOfEvents_ == nullptr) setOfEvents_ = new std::vector<RetransmissionEvent>();

  setOfEvents_->push_back(e);

  if (setOfEvents_->size() < PARM::sufficientNumberOfMsgsToStartBkProcess) return;

  bool succeeded = tryToStartProcessing();

  if (!succeeded && (setOfEvents_->size() >= PARM::maxNumberOfMsgsInThreadLocalQueue)) {
    // TODO(GG): print warning

    setOfEvents_->clear();
  }
}

void RetransmissionsManager::setLastStable(SeqNum newLastStableSeqNum) {
  if (threadPool_ == nullptr) return;  // if disabled

  bool clearEvents = ((lastStable_ + maxOutSeqNumbers_) <= newLastStableSeqNum);

  if (lastStable_ < newLastStableSeqNum) lastStable_ = newLastStableSeqNum;

  if (clearEvents) {
    needToClearPendingRetransmissions_ = true;
    if (setOfEvents_ != nullptr) setOfEvents_->clear();
  }
}

void RetransmissionsManager::setLastView(ViewNum newView) {
  if (threadPool_ == nullptr) return;  // if disabled

  if (lastView_ < newView) {
    needToClearPendingRetransmissions_ = true;
    lastView_ = newView;
    if (setOfEvents_ != nullptr) setOfEvents_->clear();
  }
}

bool RetransmissionsManager::tryToStartProcessing() {
  if (threadPool_ == nullptr) return false;  // if disabled
  if (backgroundProcessing_) return false;

  std::vector<RetransmissionEvent>* eventsToProcess = nullptr;
  if (setOfEvents_ != nullptr && !setOfEvents_->empty()) {
    eventsToProcess = setOfEvents_;
    setOfEvents_ = nullptr;
  }

  // send work to thread pool
  auto* logic = (RetransmissionLogic*)internalLogicInfo_;
  RetransmissionProcessingJob* j = new RetransmissionProcessingJob(replica_,
                                                                   this,
                                                                   msgsCommunicator_,
                                                                   eventsToProcess,
                                                                   logic,
                                                                   lastStable_,
                                                                   lastView_,
                                                                   needToClearPendingRetransmissions_);
  threadPool_->add(j);

  backgroundProcessing_ = true;
  needToClearPendingRetransmissions_ = false;
  return true;
}

void RetransmissionsManager::onProcessingComplete() {
  Assert(threadPool_ != nullptr);
  Assert(backgroundProcessing_);

  backgroundProcessing_ = false;
}

}  // namespace bftEngine::impl
