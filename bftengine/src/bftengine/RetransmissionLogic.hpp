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

#pragma once

#include "messages/MessageBase.hpp"
#include "RetransmissionsManager.hpp"
#include "ReplicaRetransmissionInfo.hpp"
#include "RollingAvgAndVar.hpp"

#include <unordered_map>

namespace bftEngine::impl {

class RetransmissionLogic {
 public:
  explicit RetransmissionLogic(uint16_t maxOutNumOfSeqNumbers)
      : maxOutSeqNumbers_{maxOutNumOfSeqNumbers}, lastStable_{0} {}

  ~RetransmissionLogic();

  void setLastStable(SeqNum newLastStableSeqNum);
  void clearPendingRetransmissions();
  void processSend(Time time, uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType, bool ignorePreviousAcks);
  void processAck(Time time, uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType);
  void getSuggestedRetransmissions(Time currentTime, std::forward_list<RetSuggestion>& outSuggestedRetransmissions);

 private:
  struct TrackedItem {
    TrackedItem(uint16_t rId, uint16_t mType) : replicaId{rId}, msgType{mType} {}

    const uint16_t replicaId;
    const uint16_t msgType;
    SeqNum seqNumber = 0;
    bool ackOrAbort = false;
    uint16_t numOfTransmissions = 0;  // valid only if ackOrAbort==false
    Time timeOfTransmission;          // valid only if ackOrAbort==false
  };

  struct PendingRetransmission {
    Time expirationTime;
    TrackedItem* item = nullptr;

    struct Comparator {
      bool operator()(const PendingRetransmission& a, const PendingRetransmission& b) const {
        return (b.expirationTime < a.expirationTime);
      }
    };
  };

  const uint16_t maxOutSeqNumbers_;
  SeqNum lastStable_;
  std::unordered_map<uint64_t, TrackedItem*> trackedItemsMap_;
  std::unordered_map<uint64_t, ReplicaRetransmissionInfo*> replicasMap_;

  std::priority_queue<PendingRetransmission, std::vector<PendingRetransmission>, PendingRetransmission::Comparator>
      pendingRetransmissionsHeap_;

  TrackedItem* getTrackedItem(uint16_t replicaId, uint16_t msgType, SeqNum msgSeqNum);
  ReplicaRetransmissionInfo* getReplicaInfo(uint16_t replicaId, uint16_t msgType);
  void makeRoom();
};

}  // namespace bftEngine::impl