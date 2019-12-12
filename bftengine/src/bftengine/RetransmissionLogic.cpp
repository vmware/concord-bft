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

#include "RetransmissionLogic.hpp"
#include <assertUtils.hpp>

using namespace std;
using namespace std::chrono;

namespace bftEngine::impl {

RetransmissionLogic::~RetransmissionLogic() {
  for (auto&& i : trackedItemsMap_) {
    TrackedItem* t = i.second;
    delete t;
  }
  for (auto&& i : replicasMap_) {
    ReplicaRetransmissionInfo* t = i.second;
    delete t;
  }
}

void RetransmissionLogic::setLastStable(SeqNum newLastStableSeqNum) {
  if (lastStable_ < newLastStableSeqNum) lastStable_ = newLastStableSeqNum;
}

// NB: should be used before moving to a new view
void RetransmissionLogic::clearPendingRetransmissions() {
  pendingRetransmissionsHeap_ = std::
      priority_queue<PendingRetransmission, std::vector<PendingRetransmission>, PendingRetransmission::Comparator>();
  Assert(pendingRetransmissionsHeap_.empty());
}

void RetransmissionLogic::processSend(
    Time time, uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType, bool ignorePreviousAcks) {
  if ((msgSeqNum <= lastStable_) || (msgSeqNum > lastStable_ + maxOutSeqNumbers_)) return;

  TrackedItem* trackedItem = getTrackedItem(replicaId, msgType, msgSeqNum);

  if (trackedItem->seqNumber != msgSeqNum || ignorePreviousAcks) {
    trackedItem->seqNumber = msgSeqNum;
    trackedItem->ackOrAbort = false;
    trackedItem->numOfTransmissions = 0;
    trackedItem->timeOfTransmission = time;
    // TODO(GG): if ignorePreviousAcks==true, we may already have related retransmissions in
    // pendingRetransmissionsHeap (make sure that this is okay)
  } else if (trackedItem->ackOrAbort)
    return;

  trackedItem->numOfTransmissions++;
  if (trackedItem->numOfTransmissions >= PARM::maxTransmissionsPerMsg) {
    trackedItem->ackOrAbort = true;

    if (PARM::maxTransmissionsPerMsg >= 2) {
      Time tmp = trackedItem->timeOfTransmission;
      if ((tmp < time)) {
        uint64_t responseTimeMilli = duration_cast<milliseconds>(time - tmp).count();
        ReplicaRetransmissionInfo* repInfo = getReplicaInfo(replicaId, msgType);
        repInfo->add(responseTimeMilli);
      }
    }
    return;
  }

  ReplicaRetransmissionInfo* repInfo = getReplicaInfo(replicaId, msgType);
  uint64_t waitTimeMilli = repInfo->getRetransmissionTimeMilli();
  // maxTimeBetweenRetransmissionsMilli is treated as "infinite"
  if (waitTimeMilli >= PARM::maxTimeBetweenRetransmissionsMilli) {
    trackedItem->ackOrAbort = true;
    return;
  }

  Time expTime = time + milliseconds(waitTimeMilli);
  PendingRetransmission pr{expTime, trackedItem};
  if (pendingRetransmissionsHeap_.size() >= PARM::maxNumberOfConcurrentManagedTransmissions) makeRoom();
  pendingRetransmissionsHeap_.push(pr);
}

void RetransmissionLogic::processAck(Time time, uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType) {
  if ((msgSeqNum <= lastStable_) || (msgSeqNum > lastStable_ + maxOutSeqNumbers_)) return;

  TrackedItem* trackedItem = getTrackedItem(replicaId, msgType, msgSeqNum);
  Time tmp = trackedItem->timeOfTransmission;

  if ((trackedItem->seqNumber == msgSeqNum) && (!trackedItem->ackOrAbort) && (!(time < tmp))) {
    trackedItem->ackOrAbort = true;
    uint64_t responseTimeMilli = duration_cast<milliseconds>(time - tmp).count();
    ReplicaRetransmissionInfo* repInfo = getReplicaInfo(replicaId, msgType);
    repInfo->add(responseTimeMilli);
  }
}

void RetransmissionLogic::getSuggestedRetransmissions(Time currentTime,
                                                      std::forward_list<RetSuggestion>& outSuggestedRetransmissions) {
  Assert(outSuggestedRetransmissions.empty());

  if (pendingRetransmissionsHeap_.empty()) return;

  PendingRetransmission currentItem = pendingRetransmissionsHeap_.top();
  while (true) {
    if ((currentTime < currentItem.expirationTime)) break;

    pendingRetransmissionsHeap_.pop();
    TrackedItem* t = currentItem.item;

    if (!t->ackOrAbort && (t->seqNumber > lastStable_) && (t->seqNumber <= lastStable_ + maxOutSeqNumbers_)) {
      RetSuggestion retSuggestion{t->replicaId, t->msgType, t->seqNumber};
      outSuggestedRetransmissions.push_front(retSuggestion);
    }

    if (pendingRetransmissionsHeap_.empty()) break;
    currentItem = pendingRetransmissionsHeap_.top();
  }
}

RetransmissionLogic::TrackedItem* RetransmissionLogic::getTrackedItem(uint16_t replicaId,
                                                                      uint16_t msgType,
                                                                      SeqNum msgSeqNum) {
  const uint16_t seqNumIdx = msgSeqNum % (this->maxOutSeqNumbers_ * 2);

  const uint64_t a = replicaId;
  const uint64_t b = msgType;
  const uint64_t c = seqNumIdx;
  const uint64_t itemId = (a << 32) | (b << 16) | c;

  auto it = trackedItemsMap_.find(itemId);

  TrackedItem* trackedItem = nullptr;

  if (it == trackedItemsMap_.end()) {
    trackedItem = new TrackedItem(replicaId, msgType);
    trackedItemsMap_.insert({itemId, trackedItem});
  } else {
    trackedItem = it->second;
  }

  return trackedItem;
}

ReplicaRetransmissionInfo* RetransmissionLogic::getReplicaInfo(uint16_t replicaId, uint16_t msgType) {
  const uint64_t a = replicaId;
  const uint64_t b = msgType;
  const uint64_t itemId = (a << 16) | b;

  auto it = replicasMap_.find(itemId);
  ReplicaRetransmissionInfo* replicaInfo = nullptr;
  if (it == replicasMap_.end()) {
    replicaInfo = new ReplicaRetransmissionInfo();
    replicasMap_.insert({itemId, replicaInfo});
  } else {
    replicaInfo = it->second;
  }

  return replicaInfo;
}

void RetransmissionLogic::makeRoom() {
  uint16_t itemIgnored = 0;
  while (pendingRetransmissionsHeap_.size() >= PARM::maxNumberOfConcurrentManagedTransmissions) {
    const PendingRetransmission oldItem = pendingRetransmissionsHeap_.top();
    pendingRetransmissionsHeap_.pop();
    if (!oldItem.item->ackOrAbort && oldItem.item->seqNumber > lastStable_) itemIgnored++;
  }
  if (itemIgnored > 0) {
    // TODO(GG): warning
  }
}

}  // namespace bftEngine::impl