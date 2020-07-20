// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <forward_list>
#include <unordered_map>
#include <algorithm>
#include <queue>
#include <cmath>  // sqrt
#include <chrono>

#include "messages/RetranProcResultInternalMsg.hpp"
#include "RetransmissionsManager.hpp"
#include "SimpleThreadPool.hpp"
#include "IncomingMsgsStorage.hpp"
#include "RollingAvgAndVar.hpp"
#include "assertUtils.hpp"

using namespace std::chrono;

namespace bftEngine {
namespace impl {

///////////////////////////////////////////////////////////////////////////////
// RetransmissionsLogic
///////////////////////////////////////////////////////////////////////////////

#define PARM RetransmissionsParams

class RetransmissionsLogic {
 public:
  RetransmissionsLogic(uint16_t maxOutNumOfSeqNumbers) : maxOutSeqNumbers{maxOutNumOfSeqNumbers}, lastStable{0} {}

  ~RetransmissionsLogic() {
    for (auto&& i : trackedItemsMap) {
      TrackedItem* t = i.second;
      delete t;
    }

    for (auto&& i : replicasMap) {
      ReplicaInfo* t = i.second;
      delete t;
    }
  }

  void setLastStable(SeqNum newLastStableSeqNum) {
    if (lastStable < newLastStableSeqNum) lastStable = newLastStableSeqNum;
  }

  void clearPendingRetransmissions()  // NB: should be used before moving to a new view
  {
    pendingRetransmissionsHeap =
        std::priority_queue<PendingRetran, std::vector<PendingRetran>, PendingRetran::Comparator>();
    ConcordAssert(pendingRetransmissionsHeap.empty());
  }

  void processSend(Time time, uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType, bool ignorePreviousAcks) {
    if ((msgSeqNum <= lastStable) || (msgSeqNum > lastStable + maxOutSeqNumbers)) return;

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

          ReplicaInfo* repInfo = getReplicaInfo(replicaId, msgType);

          repInfo->add(responseTimeMilli);
        }
      }

      return;
    }

    ReplicaInfo* repInfo = getReplicaInfo(replicaId, msgType);

    uint64_t waitTimeMilli = repInfo->getRetransmissionTimeMilli();

    if (waitTimeMilli >= PARM::maxTimeBetweenRetranMilli)  // maxTimeBetweenRetranMilli is treated as "infinite"
    {
      trackedItem->ackOrAbort = true;
      return;
    }

    Time expTime = time + milliseconds(waitTimeMilli);

    PendingRetran pr{expTime, trackedItem};

    if (pendingRetransmissionsHeap.size() >= PARM::maxNumberOfConcurrentManagedTransmissions) makeRoom();

    pendingRetransmissionsHeap.push(pr);
  }

  void processAck(Time time, uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType) {
    if ((msgSeqNum <= lastStable) || (msgSeqNum > lastStable + maxOutSeqNumbers)) return;

    TrackedItem* trackedItem = getTrackedItem(replicaId, msgType, msgSeqNum);

    Time tmp = trackedItem->timeOfTransmission;

    if ((trackedItem->seqNumber == msgSeqNum) && (trackedItem->ackOrAbort == false) && (!(time < tmp))) {
      trackedItem->ackOrAbort = true;

      uint64_t responseTimeMilli = duration_cast<milliseconds>(time - tmp).count();

      ReplicaInfo* repInfo = getReplicaInfo(replicaId, msgType);

      repInfo->add(responseTimeMilli);
    }
  }

  void getSuggestedRetransmissions(Time currentTime, std::forward_list<RetSuggestion>& outSuggestedRetransmissions) {
    ConcordAssert(outSuggestedRetransmissions.empty());

    if (pendingRetransmissionsHeap.empty()) return;

    PendingRetran currentItem = pendingRetransmissionsHeap.top();

    while (true) {
      if ((currentTime < currentItem.expirationTime)) break;

      pendingRetransmissionsHeap.pop();

      TrackedItem* t = currentItem.item;

      if (!t->ackOrAbort && (t->seqNumber > lastStable) && (t->seqNumber <= lastStable + maxOutSeqNumbers)) {
        RetSuggestion retSuggestion{t->replicaId, t->msgType, t->seqNumber};

        outSuggestedRetransmissions.push_front(retSuggestion);
      }

      if (pendingRetransmissionsHeap.empty()) break;

      currentItem = pendingRetransmissionsHeap.top();
    }
  }

 protected:
  struct TrackedItem {
    const uint16_t replicaId;
    const uint16_t msgType;

    TrackedItem(uint16_t rId, uint16_t mType) : replicaId{rId}, msgType{mType} {}

    SeqNum seqNumber = 0;

    bool ackOrAbort = false;
    uint16_t numOfTransmissions;  // valid only if ackOrAbort==false
    Time timeOfTransmission;      // valid only if ackOrAbort==false
  };

  class ReplicaInfo {
   public:
    ReplicaInfo() : retranTimeMilli{PARM::defaultTimeBetweenRetranMilli} {}

    void add(uint64_t responseTimeMilli) {
      const uint64_t hours24 =
          24 * 60 * 60 * 1000;  // TODO(GG): ?? we only wanted safe conversion from uint64_t to double
      if (responseTimeMilli > hours24) responseTimeMilli = hours24;

      const double v = (double)responseTimeMilli;
      avgAndVarOfAckTime.add(v);

      const int numOfVals = avgAndVarOfAckTime.numOfElements();

      if (numOfVals % PARM::evalPeriod == 0) {
        const uint64_t maxVal =
            std::min((uint64_t)PARM::maxTimeBetweenRetranMilli, retranTimeMilli * PARM::maxIncreasingFactor);
        const uint64_t minVal =
            std::max((uint64_t)PARM::minTimeBetweenRetranMilli, retranTimeMilli / PARM::maxDecreasingFactor);
        ConcordAssert(minVal <= maxVal);

        const double avg = avgAndVarOfAckTime.avg();
        const double var = avgAndVarOfAckTime.var();
        const double sd = ((var > 0) ? sqrt(var) : 0);
        const uint64_t newRetran = (uint64_t)avg + 2 * ((uint64_t)sd);

        if (newRetran > maxVal)
          retranTimeMilli = maxVal;
        else if (newRetran < minVal)
          retranTimeMilli = minVal;
        else
          retranTimeMilli = newRetran;

        if (numOfVals >= PARM::resetPoint) avgAndVarOfAckTime.reset();
      }
    }

    uint64_t getRetransmissionTimeMilli() {
      if (retranTimeMilli == PARM::maxTimeBetweenRetranMilli)
        return UINT64_MAX;
      else
        return retranTimeMilli;
    }

   private:
    RollingAvgAndVar avgAndVarOfAckTime;

    uint64_t retranTimeMilli;
  };

  inline TrackedItem* getTrackedItem(uint16_t replicaId, uint16_t msgType, SeqNum msgSeqNum) {
    const uint16_t seqNumIdx = msgSeqNum % (this->maxOutSeqNumbers * 2);

    const uint64_t a = replicaId;
    const uint64_t b = msgType;
    const uint64_t c = seqNumIdx;
    const uint64_t itemId = (a << 32) | (b << 16) | c;

    std::unordered_map<uint64_t, TrackedItem*>::const_iterator it = trackedItemsMap.find(itemId);

    TrackedItem* trackedItem = nullptr;

    if (it == trackedItemsMap.end()) {
      trackedItem = new TrackedItem(replicaId, msgType);
      trackedItemsMap.insert({itemId, trackedItem});
    } else {
      trackedItem = it->second;
    }

    return trackedItem;
  }

  inline ReplicaInfo* getReplicaInfo(uint16_t replicaId, uint16_t msgType) {
    const uint64_t a = replicaId;
    const uint64_t b = msgType;
    const uint64_t itemId = (a << 16) | b;

    std::unordered_map<uint64_t, ReplicaInfo*>::const_iterator it = replicasMap.find(itemId);

    ReplicaInfo* replicaInfo = nullptr;

    if (it == replicasMap.end()) {
      replicaInfo = new ReplicaInfo();
      replicasMap.insert({itemId, replicaInfo});
    } else {
      replicaInfo = it->second;
    }

    return replicaInfo;
  }

  void makeRoom() {
    uint16_t itemIgnored = 0;
    while (pendingRetransmissionsHeap.size() >= PARM::maxNumberOfConcurrentManagedTransmissions) {
      const PendingRetran oldItem = pendingRetransmissionsHeap.top();
      pendingRetransmissionsHeap.pop();
      if (!oldItem.item->ackOrAbort && oldItem.item->seqNumber > lastStable) itemIgnored++;
    }
    if (itemIgnored > 0) {
      // TODO(GG): warning
    }
  }

  const uint16_t maxOutSeqNumbers;

  SeqNum lastStable;

  std::unordered_map<uint64_t, TrackedItem*> trackedItemsMap;

  std::unordered_map<uint64_t, ReplicaInfo*> replicasMap;

  struct PendingRetran {
    Time expirationTime;
    TrackedItem* item;

    struct Comparator {
      bool operator()(const PendingRetran& a, const PendingRetran& b) const {
        return (b.expirationTime < a.expirationTime);
      }
    };
  };

  std::priority_queue<PendingRetran, std::vector<PendingRetran>, PendingRetran::Comparator> pendingRetransmissionsHeap;
};

///////////////////////////////////////////////////////////////////////////////
// RetransmissionsManager
///////////////////////////////////////////////////////////////////////////////

RetransmissionsManager::RetransmissionsManager(util::SimpleThreadPool* threadPool,
                                               IncomingMsgsStorage* const incomingMsgsStorage,
                                               uint16_t maxOutNumOfSeqNumbers,
                                               SeqNum lastStableSeqNum)
    : pool{threadPool},
      incomingMsgs{incomingMsgsStorage},
      maxOutSeqNumbers{maxOutNumOfSeqNumbers},
      internalLogicInfo{new RetransmissionsLogic(maxOutNumOfSeqNumbers)} {
  ConcordAssert(threadPool != nullptr);
  ConcordAssert(incomingMsgsStorage != nullptr);
  ConcordAssert(maxOutNumOfSeqNumbers > 0);

  lastStable = lastStableSeqNum;
}

RetransmissionsManager::RetransmissionsManager()
    : pool{nullptr}, incomingMsgs{nullptr}, maxOutSeqNumbers{0}, internalLogicInfo{nullptr}, lastStable{0} {}

RetransmissionsManager::~RetransmissionsManager() {
  if (pool == nullptr) return;  // if disabled

  // TODO(GG): make sure that threadPool has been stopped

  if (setOfEvents != nullptr) delete setOfEvents;

  RetransmissionsLogic* logic = (RetransmissionsLogic*)internalLogicInfo;
  delete logic;
}

void RetransmissionsManager::onSend(uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType, bool ignorePreviousAcks) {
  if (pool == nullptr) return;  // if disabled

  if ((msgSeqNum <= lastStable) || (msgSeqNum > lastStable + maxOutSeqNumbers)) return;

  Event e;
  e.etype = (!ignorePreviousAcks ? EType::SENT : EType::SENT_AND_IGNORE_PREV);
  e.time = getMonotonicTime();
  e.replicaId = replicaId;
  e.msgSeqNum = msgSeqNum;
  e.msgType = msgType;

  add(e);
}

void RetransmissionsManager::onAck(uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType) {
  if (pool == nullptr) return;  // if disabled

  if ((msgSeqNum <= lastStable) || (msgSeqNum > lastStable + maxOutSeqNumbers)) return;

  Event e;
  e.etype = EType::ACK;
  e.time = getMonotonicTime();
  e.replicaId = replicaId;
  e.msgSeqNum = msgSeqNum;
  e.msgType = msgType;

  add(e);
}

void RetransmissionsManager::add(const Event& e) {
  if (setOfEvents == nullptr) setOfEvents = new std::vector<Event>();

  setOfEvents->push_back(e);

  if (setOfEvents->size() < PARM::sufficientNumberOfMsgsToStartBkProcess) return;

  bool succ = tryToStartProcessing();

  if (!succ && (setOfEvents->size() >= PARM::maxNumberOfMsgsInThreadLocalQueue)) {
    // TODO(GG): print warning

    setOfEvents->clear();
  }
}

void RetransmissionsManager::setLastStable(SeqNum newLastStableSeqNum) {
  if (pool == nullptr) return;  // if disabled

  bool clearEvents = ((lastStable + maxOutSeqNumbers) <= newLastStableSeqNum);

  if (lastStable < newLastStableSeqNum) lastStable = newLastStableSeqNum;

  if (clearEvents) {
    needToClearPendingRetransmissions = true;
    if (setOfEvents != nullptr) setOfEvents->clear();
  }
}

void RetransmissionsManager::setLastView(ViewNum newView) {
  if (pool == nullptr) return;  // if disabled

  if (lastView < newView) {
    needToClearPendingRetransmissions = true;
    lastView = newView;
    if (setOfEvents != nullptr) setOfEvents->clear();
  }
}

bool RetransmissionsManager::tryToStartProcessing() {
  // this class is local to this method
  class RetransmissionsProcessingJob : public util::SimpleThreadPool::Job {
   private:
    IncomingMsgsStorage* const incomingMsgs;
    std::vector<RetransmissionsManager::Event>* const setOfEvents;
    RetransmissionsLogic* const logic;
    const SeqNum lastStable;
    const ViewNum lastView;
    const bool clearPending;

   public:
    RetransmissionsProcessingJob(IncomingMsgsStorage* const incomingMsgsStorage,
                                 std::vector<RetransmissionsManager::Event>* events,
                                 RetransmissionsLogic* retransmissionsLogic,
                                 SeqNum lastStableSeqNum,
                                 ViewNum v,
                                 bool clearPendingRetransmissions)
        : incomingMsgs{incomingMsgsStorage},
          setOfEvents{events},
          logic{retransmissionsLogic},
          lastStable{lastStableSeqNum},
          lastView{v},
          clearPending{clearPendingRetransmissions} {}

    virtual ~RetransmissionsProcessingJob() {}

    virtual void release() override {
      if (setOfEvents != nullptr) delete setOfEvents;
      delete this;
    }

    virtual void execute() override {
      if (clearPending) logic->clearPendingRetransmissions();

      logic->setLastStable(lastStable);

      if (setOfEvents != nullptr) {
        for (size_t i = 0; i < setOfEvents->size(); i++) {
          RetransmissionsManager::Event& e = setOfEvents->at(i);

          if (e.etype == RetransmissionsManager::EType::SENT)
            logic->processSend(e.time, e.replicaId, e.msgSeqNum, e.msgType, false);
          else if (e.etype == RetransmissionsManager::EType::ACK)
            logic->processAck(e.time, e.replicaId, e.msgSeqNum, e.msgType);
          else if (e.etype == RetransmissionsManager::EType::SENT_AND_IGNORE_PREV)
            logic->processSend(e.time, e.replicaId, e.msgSeqNum, e.msgType, true);
          else
            ConcordAssert(false);
        }
      }

      std::forward_list<RetSuggestion> suggestedRetransmissions;

      const Time currTime = getMonotonicTime();

      logic->getSuggestedRetransmissions(currTime, suggestedRetransmissions);

      // send to main replica thread
      incomingMsgs->pushInternalMsg(
          RetranProcResultInternalMsg(lastStable, lastView, std::move(suggestedRetransmissions)));
    }
  };

  // The method's code starts here

  if (pool == nullptr) return false;  // if disabled

  if (bkProcessing) return false;

  std::vector<Event>* eventsToProcess = nullptr;

  if (setOfEvents != nullptr && setOfEvents->size() > 0) {
    eventsToProcess = setOfEvents;
    setOfEvents = nullptr;
  }

  // send work to thread pool
  RetransmissionsLogic* logic = (RetransmissionsLogic*)internalLogicInfo;

  RetransmissionsProcessingJob* j = new RetransmissionsProcessingJob(
      incomingMsgs, eventsToProcess, logic, lastStable, lastView, needToClearPendingRetransmissions);

  pool->add(j);

  bkProcessing = true;
  needToClearPendingRetransmissions = false;

  return true;
}

void RetransmissionsManager::OnProcessingComplete() {
  ConcordAssert(pool != nullptr);
  ConcordAssert(bkProcessing);

  bkProcessing = false;
}

}  // namespace impl
}  // namespace bftEngine
