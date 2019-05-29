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
#include "Serializable.h"

#include <vector>

namespace bftEngine {

typedef std::vector<ViewsManager::PrevViewInfo> PrevViewInfoElements;

struct DescriptorOfLastExitFromView : public Serializable {
  DescriptorOfLastExitFromView(ViewNum viewNum, SeqNum stableNum,
                               SeqNum execNum, PrevViewInfoElements elem) :
      view(viewNum), lastStable(stableNum), lastExecuted(execNum),
      elements(move(elem)) {
    registerClass();
  }

  bool isEmpty() const {
    return ((view == 0) && (lastStable == 0) && (lastExecuted == 0) &&
        elements.empty());
  }

  // Serialization/deserialization
  UniquePtrToClass create(std::istream &inStream) override;

  // To be used ONLY during deserialization.
  DescriptorOfLastExitFromView() = default;

  bool operator==(const DescriptorOfLastExitFromView &other) const;
  DescriptorOfLastExitFromView& operator=(
      const DescriptorOfLastExitFromView &other);

  static uint32_t maxSize() {
    return (sizeof(view) + sizeof(lastStable) + sizeof(lastExecuted) +
        sizeof(ViewsManager::PrevViewInfo::maxSize() * kWorkWindowSize));
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

 protected:
  void serializeDataMembers(std::ostream &outStream) const override;
  std::string getName() const override { return className_; };
  uint32_t getVersion() const override { return classVersion_; };

 private:
  static void registerClass();

 private:
  const std::string className_ = "DescriptorOfLastExitFromView";
  const uint32_t classVersion_ = 1;
  static bool registered_;
};

typedef std::vector<ViewChangeMsg *> ViewChangeMsgsVector;

struct DescriptorOfLastNewView : public Serializable {
  DescriptorOfLastNewView(ViewNum viewNum, NewViewMsg *newMsg,
                          ViewChangeMsgsVector msgs, SeqNum maxSeqNum) :
      view(viewNum), newViewMsg(newMsg), viewChangeMsgs(move(msgs)),
      maxSeqNumTransferredFromPrevViews(maxSeqNum) {
    registerClass();
  }

  bool isEmpty() const {
    return ((view == 0) && (newViewMsg == nullptr) &&
        viewChangeMsgs.empty() && (maxSeqNumTransferredFromPrevViews == 0));
  }

  // Serialization/deserialization
  UniquePtrToClass create(std::istream &inStream) override;

  // To be used ONLY during deserialization.
  DescriptorOfLastNewView() = default;

  bool operator==(const DescriptorOfLastNewView &other) const;
  DescriptorOfLastNewView& operator=(const DescriptorOfLastNewView &other);

  static uint32_t maxSize(uint16_t fVal, uint16_t cVal) {
    return (sizeof(view) + NewViewMsg::maxSizeOfNewViewMsg() +
        ViewChangeMsg::maxSizeOfViewChangeMsg() *
            (2 * fVal + 2 * cVal + 1) +
        sizeof(maxSeqNumTransferredFromPrevViews));
  }

  // view >= 1
  ViewNum view = 0;

  // newViewMsg != nullptr
  NewViewMsg *newViewMsg = nullptr;

  // viewChangeMsgs.size() == 2*F + 2*C + 1
  // The messages in viewChangeMsgs will never be null
  ViewChangeMsgsVector viewChangeMsgs;

  // maxSeqNumTransferredFromPrevViews >= 0
  SeqNum maxSeqNumTransferredFromPrevViews = 0;

 protected:
  void serializeDataMembers(std::ostream &outStream) const override;
  std::string getName() const override { return className_; };
  uint32_t getVersion() const override { return classVersion_; };

 private:
  static void registerClass();

 private:
  const std::string className_ = "DescriptorOfLastNewView";
  const uint32_t classVersion_ = 1;
  static bool registered_;
};

struct DescriptorOfLastExecution : public Serializable {
  DescriptorOfLastExecution(SeqNum seqNum, const Bitmap &requests) :
      executedSeqNum(seqNum), validRequests(requests) {
    registerClass();
  }

  bool isEmpty() const {
    return ((executedSeqNum == 0) && (validRequests.isEmpty()));
  }

  // Serialization/deserialization
  UniquePtrToClass create(std::istream &inStream) override;

  // To be used ONLY during deserialization.
  DescriptorOfLastExecution() = default;

  bool operator==(const DescriptorOfLastExecution &other) const;
  DescriptorOfLastExecution& operator=(const DescriptorOfLastExecution &other);

  static uint32_t maxSize() {
    return (sizeof(executedSeqNum) + Bitmap::maxSize());
  };

  // executedSeqNum >= 1
  SeqNum executedSeqNum = 0;

  // 1 <= validRequests.numOfBits() <= maxNumOfRequestsInBatch
  Bitmap validRequests;

 protected:
  void serializeDataMembers(std::ostream &outStream) const override;
  std::string getName() const override { return className_; };
  uint32_t getVersion() const override { return classVersion_; };

 private:
  static void registerClass();

 private:
  const std::string className_ = "DescriptorOfLastExecution";
  const uint32_t classVersion_ = 1;
  static bool registered_;
};

}  // namespace bftEngine
