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
#include "PrePrepareMsg.hpp"
#include "SignedShareMsgs.hpp"
#include "NewViewMsg.hpp"
#include "FullCommitProofMsg.hpp"
#include "CheckpointMsg.hpp"

#include <vector>

namespace bftEngine {

typedef std::vector<ViewsManager::PrevViewInfo> PrevViewInfoElements;

/***** DescriptorOfLastExitFromView *****/

struct DescriptorOfLastExitFromView {
  DescriptorOfLastExitFromView(ViewNum viewNum, SeqNum stableNum,
                               SeqNum execNum, PrevViewInfoElements elem) :
      view(viewNum), lastStable(stableNum), lastExecuted(execNum),
      elements(move(elem)) {}

  DescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &other) {
    setTo(other);
  }

  DescriptorOfLastExitFromView &operator=(
      const DescriptorOfLastExitFromView &other) {
    setTo(other);
    return *this;
  }

  void setTo(const DescriptorOfLastExitFromView &other);

  bool isEmpty() const {
    return ((view == 0) && (lastStable == 0) && (lastExecuted == 0) &&
        elements.empty());
  }

  ~DescriptorOfLastExitFromView();
  DescriptorOfLastExitFromView() = default;

  void serializeSimpleParams(char *&buf, size_t bufLen) const;
  void serializeElement(
      uint32_t id, char *&buf, size_t bufLen, size_t &actualSize) const;

  void deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize);
  void deserializeElement(char *buf, size_t bufLen, uint32_t &actualSize);

  bool operator==(const DescriptorOfLastExitFromView &other) const;

  static uint32_t simpleParamsSize() {
    uint32_t elementsNum;
    return (sizeof(elementsNum) + sizeof(view) + sizeof(lastStable) +
        sizeof(lastExecuted));
  }

  static uint32_t maxElementSize() {
    bool msgEmptyFlag;
    return 2 * sizeof(msgEmptyFlag) + ViewsManager::PrevViewInfo::maxSize();
  }

  static uint32_t maxSize() {
    return simpleParamsSize() + maxElementSize() * kWorkWindowSize;
  }

  // view >= 0
  ViewNum view = 0;

  // lastStable >= 0
  SeqNum lastStable = 0;

  // lastExecuted >= lastStable
  SeqNum lastExecuted = 0;

  // elements.size() <= kWorkWindowSize
  // The messages in elements[i] may be null
  PrevViewInfoElements elements;
};

/***** DescriptorOfLastNewView *****/

typedef std::vector<ViewChangeMsg *> ViewChangeMsgsVector;

struct DescriptorOfLastNewView {
  DescriptorOfLastNewView(ViewNum viewNum, NewViewMsg *newMsg,
                          ViewChangeMsgsVector msgs, SeqNum maxSeqNum) :
      view(viewNum), maxSeqNumTransferredFromPrevViews(maxSeqNum),
      newViewMsg(newMsg), viewChangeMsgs(move(msgs)) {}

  DescriptorOfLastNewView(const DescriptorOfLastNewView &other) {
    setTo(other);
  }

  DescriptorOfLastNewView &operator=(const DescriptorOfLastNewView &other) {
    setTo(other);
    return *this;
  }

  void setTo(const DescriptorOfLastNewView &other);
  ~DescriptorOfLastNewView();

  bool isEmpty() const {
    return ((view == 0) && (newViewMsg == nullptr) &&
        viewChangeMsgs.empty() && (maxSeqNumTransferredFromPrevViews == 0));
  }

  DescriptorOfLastNewView() = default;

  bool operator==(const DescriptorOfLastNewView &other) const;

  void serializeSimpleParams(
      char *&buf, size_t bufLen, size_t &actualSize) const;
  void serializeElement(
      uint32_t id, char *&buf, size_t bufLen, size_t &actualSize) const;

  void deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize);
  void deserializeElement(char *buf, size_t bufLen, uint32_t &actualSize);

  static uint32_t simpleParamsSize() {
    bool msgEmptyFlag;
    return (sizeof(msgEmptyFlag) + sizeof(view) +
        sizeof(maxSeqNumTransferredFromPrevViews) +
        NewViewMsg::maxSizeOfNewViewMsgInLocalBuffer());
  }

  static uint32_t maxElementSize() {
    return ViewChangeMsg::maxSizeOfViewChangeMsgInLocalBuffer();
  }

  static uint32_t maxSize(uint32_t numOfReplicas) {
    return simpleParamsSize() + maxElementSize() * numOfReplicas;
  }

  // view >= 1
  ViewNum view = 0;

  // maxSeqNumTransferredFromPrevViews >= 0
  SeqNum maxSeqNumTransferredFromPrevViews = 0;

  // newViewMsg != nullptr
  NewViewMsg *newViewMsg = nullptr;

  // viewChangeMsgs.size() == 2*F + 2*C + 1
  // The messages in viewChangeMsgs will never be null
  ViewChangeMsgsVector viewChangeMsgs;
};

/***** DescriptorOfLastExecution *****/

struct DescriptorOfLastExecution {
  DescriptorOfLastExecution(SeqNum seqNum, const Bitmap &requests) :
      executedSeqNum(seqNum), validRequests(requests) {
  }

  bool isEmpty() const {
    return ((executedSeqNum == 0) && (validRequests.isEmpty()));
  }

  DescriptorOfLastExecution() = default;

  bool operator==(const DescriptorOfLastExecution &other) const;

  void serialize(char *&buf, size_t bufLen, size_t &actualSize) const;
  void deserialize(char *buf, size_t bufLen, uint32_t &actualSize);

  static uint32_t maxSize() {
    return (sizeof(executedSeqNum) + Bitmap::size());
  };

  // executedSeqNum >= 1
  SeqNum executedSeqNum = 0;

  // 1 <= validRequests.numOfBits() <= maxNumOfRequestsInBatch
  Bitmap validRequests;
};

}  // namespace bftEngine
