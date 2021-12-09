// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "PrimitiveTypes.hpp"
#include "Bitmap.hpp"
#include "ViewsManager.hpp"
#include "ReplicaConfig.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/SignedShareMsgs.hpp"
#include "messages/NewViewMsg.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "messages/CheckpointMsg.hpp"
#include "SysConsts.hpp"
#include "TimeService.hpp"

#include <vector>

namespace bftEngine {
namespace impl {

typedef std::vector<ViewsManager::PrevViewInfo> PrevViewInfoElements;

/***** DescriptorOfLastExitFromView *****/

struct DescriptorOfLastExitFromView {
  DescriptorOfLastExitFromView(ViewNum viewNum,
                               SeqNum stableNum,
                               SeqNum execNum,
                               PrevViewInfoElements elements,
                               ViewChangeMsg *viewChangeMsg,
                               SeqNum stableLowerBound)
      : isDefault(false),
        view(viewNum),
        lastStable(stableNum),
        lastExecuted(execNum),
        stableLowerBoundWhenEnteredToView(stableLowerBound),
        myViewChangeMsg(viewChangeMsg),
        elements(move(elements)) {}

  DescriptorOfLastExitFromView() : isDefault(true){};

  void clean();
  void serializeSimpleParams(char *buf, size_t bufLen, size_t &actualSize) const;
  void serializeElement(uint32_t id, char *buf, size_t bufLen, size_t &actualSize) const;

  void deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize);
  void deserializeElement(uint32_t id, char *buf, size_t bufLen, uint32_t &actualSize);

  bool equals(const DescriptorOfLastExitFromView &other) const;

  static uint32_t simpleParamsSize() {
    uint32_t elementsNum;
    uint8_t msgFilledFlag;
    return (sizeof(isDefault) + sizeof(view) + sizeof(lastStable) + sizeof(lastExecuted) +
            sizeof(stableLowerBoundWhenEnteredToView) + sizeof(msgFilledFlag) +
            maxMessageSizeInLocalBuffer<ViewChangeMsg>() + sizeof(elementsNum));
  }

  static uint32_t maxElementSize() {
    uint8_t msgFilledFlag;
    return 2 * sizeof(msgFilledFlag) + ViewsManager::PrevViewInfo::maxSize();
  }

  static uint32_t maxSize() { return simpleParamsSize() + maxElementSize() * kWorkWindowSize; }

  // Simple parameters - serialized together

  // whether this is a default descriptor instance
  bool isDefault;

  // view >= 0
  ViewNum view = 0;

  // lastStable >= 0
  SeqNum lastStable = 0;

  // lastExecuted >= lastStable
  SeqNum lastExecuted = 0;

  // Last stable when the view became active; lastStable >= stableLowerBoundWhenEnteredToView
  SeqNum stableLowerBoundWhenEnteredToView = 0;

  // myViewChangeMsg!=nullptr OR view==0; this message was relevant when the view became active
  ViewChangeMsg *myViewChangeMsg = nullptr;

  // List of messages - serialized separately

  // elements.size() <= kWorkWindowSize; the messages in elements[i] may be null
  PrevViewInfoElements elements;
};

/***** DescriptorOfLastNewView *****/

typedef std::vector<ViewChangeMsg *> ViewChangeMsgsVector;
typedef std::vector<CheckpointMsg *> CheckpointMsgsVector;

struct DescriptorOfLastNewView {
  DescriptorOfLastNewView(ViewNum viewNum,
                          NewViewMsg *newMsg,
                          ViewChangeMsgsVector msgs,
                          ViewChangeMsg *viewChangeMsg,
                          SeqNum stableLowerBound,
                          SeqNum maxSeqNum)
      : isDefault(false),
        view(viewNum),
        maxSeqNumTransferredFromPrevViews(maxSeqNum),
        stableLowerBoundWhenEnteredToView(stableLowerBound),
        newViewMsg(newMsg),
        myViewChangeMsg(viewChangeMsg),
        viewChangeMsgs(move(msgs)) {}

  DescriptorOfLastNewView();

  bool equals(const DescriptorOfLastNewView &other) const;

  void clean();
  void serializeSimpleParams(char *buf, size_t bufLen, size_t &actualSize) const;
  void serializeElement(uint32_t id, char *buf, size_t bufLen, size_t &actualSize) const;

  void deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize);
  void deserializeElement(uint32_t id, char *buf, size_t bufLen, size_t &actualSize);

  static void setViewChangeMsgsNum(uint16_t fVal, uint16_t cVal) { viewChangeMsgsNum = 2 * fVal + 2 * cVal + 1; }

  static uint32_t getViewChangeMsgsNum() { return viewChangeMsgsNum; }

  static uint32_t simpleParamsSize() {
    uint8_t msgFilledFlag;
    return (sizeof(isDefault) + sizeof(view) + sizeof(maxSeqNumTransferredFromPrevViews) + 2 * sizeof(msgFilledFlag) +
            sizeof(stableLowerBoundWhenEnteredToView) + maxMessageSizeInLocalBuffer<NewViewMsg>() +
            maxMessageSizeInLocalBuffer<ViewChangeMsg>());
  }

  static uint32_t maxElementSize() {
    uint8_t msgFilledFlag;
    return sizeof(msgFilledFlag) + maxMessageSizeInLocalBuffer<ViewChangeMsg>();
  }

  static uint32_t maxSize(uint32_t numOfReplicas) { return simpleParamsSize() + maxElementSize() * numOfReplicas; }

  static uint32_t viewChangeMsgsNum;

  // Simple parameters - serialized together

  // whether this is a default descriptor instance
  bool isDefault;

  // view >= 1
  ViewNum view = 0;

  // maxSeqNumTransferredFromPrevViews >= 0
  SeqNum maxSeqNumTransferredFromPrevViews = 0;

  // Last stable when the view became active; lastStable >= stableLowerBoundWhenEnteredToView
  SeqNum stableLowerBoundWhenEnteredToView = 0;

  // newViewMsg != nullptr
  NewViewMsg *newViewMsg = nullptr;

  // This message is relevant when the view becomes active
  // if myViewChangeMsg == nullptr, then the replica has message in viewChangeMsgs
  ViewChangeMsg *myViewChangeMsg = nullptr;

  // List of messages - serialized separately

  // viewChangeMsgs.size() == 2*F + 2*C + 1
  // The messages in viewChangeMsgs will never be null
  ViewChangeMsgsVector viewChangeMsgs;
};

/***** DescriptorOfLastExecution *****/

struct DescriptorOfLastExecution {
  DescriptorOfLastExecution(SeqNum seqNum, const Bitmap &requests, const ConsensusTickRep &ticks)
      : executedSeqNum(seqNum), validRequests(requests), timeInTicks(ticks) {}

  DescriptorOfLastExecution() = default;

  bool equals(const DescriptorOfLastExecution &other) const;

  void serialize(char *&buf, size_t bufLen, size_t &actualSize) const;
  void deserialize(char *buf, size_t bufLen, uint32_t &actualSize);

  static uint32_t maxSize() {
    return (sizeof(executedSeqNum) + Bitmap::maxSizeNeededToStoreInBuffer(maxNumOfRequestsInBatch) +
            sizeof(ConsensusTickRep));
  };

  // executedSeqNum >= 1
  SeqNum executedSeqNum = 0;

  // 1 <= validRequests.numOfBits() <= maxNumOfRequestsInBatch
  Bitmap validRequests;

  ConsensusTickRep timeInTicks = 0;
};

/***** DescriptorOfLastStableCheckpoint *****/

struct DescriptorOfLastStableCheckpoint {
  DescriptorOfLastStableCheckpoint(uint16_t numReplicasInQuorum, const CheckpointMsgsVector &msgs)
      : numOfReplicas(numReplicasInQuorum), numMsgs(0), checkpointMsgs(msgs) {}

  bool equals(const DescriptorOfLastStableCheckpoint &other) const;

  void serialize(char *&buf, size_t bufLen, size_t &actualSize) const;
  void deserialize(char *buf, size_t bufLen, size_t &actualSize);

  static uint32_t maxSize(uint16_t numReplicas) {
    return sizeof(numMsgs) + (maxMessageSizeInLocalBuffer<CheckpointMsg>() * numReplicas);
  };

  uint16_t numOfReplicas;

  mutable uint16_t numMsgs;  // used for serialization/deserialization

  CheckpointMsgsVector checkpointMsgs;
};
}  // namespace impl
}  // namespace bftEngine
