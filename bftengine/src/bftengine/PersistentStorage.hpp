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
namespace impl {

// The PersistentStorage interface is used to write/read the concord-bft state 
// to/from a persistent storage. In case the replicas process is killed, 
// crashed or restarted, this interface is used to restore the concord-bft 
// state from the persistent storage. 
// PersistentStorage is designed to only store data elements that are essential 
// to reconstruct the internal in-memory data structures of concord-bft.
// For simplicity and efficiency, some of the internal data elements are not 
// written to the persistent storage (for example, messages with partial 
// threshold signatures are not stored in persistent storage; if needed such 
// messages are re-created and retransmitted by the replicas).

class PersistentStorage {
 public:
  //////////////////////////////////////////////////////////////////////////
  // Types
  //////////////////////////////////////////////////////////////////////////

  struct DescriptorOfLastExitFromView {
    typedef std::vector<ViewsManager::PrevViewInfo> PrevViewInfoElements;
    DescriptorOfLastExitFromView(ViewNum viewNum, SeqNum stableNum,
                                 SeqNum execNum, PrevViewInfoElements elem) :
        view(viewNum), lastStable(stableNum), lastExecuted(execNum),
        elements(move(elem)) {}

    DescriptorOfLastExitFromView() : view(0), lastStable(0), lastExecuted(0) {}

    bool isEmpty() {
      return ((view == 0) && (lastStable == 0) && (lastExecuted == 0) &&
          elements.empty());
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

    static uint32_t maxSize() {
      return (sizeof(view) + sizeof(lastStable) + sizeof(lastExecuted) +
          sizeof(ViewsManager::PrevViewInfo::maxSize() * kWorkWindowSize));
    }
  };

  struct DescriptorOfLastNewView {
    typedef std::vector<ViewChangeMsg *> ViewChangeMsgsVector;
    DescriptorOfLastNewView(ViewNum viewNum, NewViewMsg *newMsg,
                            ViewChangeMsgsVector msgs, SeqNum maxSeqNum) :
        view(viewNum), newViewMsg(newMsg), viewChangeMsgs(move(msgs)),
        maxSeqNumTransferredFromPrevViews(maxSeqNum) {}

    DescriptorOfLastNewView() : view(0), maxSeqNumTransferredFromPrevViews(0) {}

    bool isEmpty() {
      return ((view == 0) && (newViewMsg == nullptr) &&
          viewChangeMsgs.empty() && (maxSeqNumTransferredFromPrevViews == 0));
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

    static uint32_t maxSize(uint16_t fVal, uint16_t cVal) {
      return (sizeof(view) + NewViewMsg::maxSizeOfNewViewMsg() +
          ViewChangeMsg::maxSizeOfViewChangeMsg() *
              (2 * fVal + 2 * cVal + 1) +
          sizeof(maxSeqNumTransferredFromPrevViews));
    }
  };

  struct DescriptorOfLastExecution {
    DescriptorOfLastExecution(SeqNum seqNum, const Bitmap &requests) :
        executedSeqNum(seqNum), validRequests(requests) {}

    DescriptorOfLastExecution() : executedSeqNum(0) {}

    bool isEmpty() const {
      return ((executedSeqNum == 0) && (validRequests.isEmpty()));
    }

    // executedSeqNum >= 1
    SeqNum executedSeqNum = 0;

    // 1 <= validRequests.numOfBits() <= maxNumOfRequestsInBatch
    Bitmap validRequests;

    static uint32_t maxSize() {
      return (sizeof(executedSeqNum) + Bitmap::maxSize());
    };
  };

  //////////////////////////////////////////////////////////////////////////
  // Transactions management
  //////////////////////////////////////////////////////////////////////////

  // begin reentrant write-only transaction
  // returns the number of nested transactions
  virtual uint8_t beginWriteTran() = 0;

  // end reentrant write-only transaction
  // returns the number of remaining nested transactions
  virtual uint8_t endWriteTran() = 0;

  // return true IFF write-only transactions are running now
  virtual bool isInWriteTran() const = 0;

  //////////////////////////////////////////////////////////////////////////
  // Update methods (should only be used in write-only transactions)
  //////////////////////////////////////////////////////////////////////////

  virtual void setReplicaConfig(const ReplicaConfig &config) = 0;

  virtual void setFetchingState(const bool &f) = 0;
  virtual void setLastExecutedSeqNum(const SeqNum &s) = 0;
  virtual void setPrimaryLastUsedSeqNum(const SeqNum &s) = 0;
  virtual void setStrictLowerBoundOfSeqNums(const SeqNum &s) = 0;
  virtual void setLastViewThatTransferredSeqNumbersFullyExecuted(
      const ViewNum &v) = 0;

  // DescriptorOfLastExitFromView contains pointers to messages (the content of
  // the messages should be copied, the caller is the owner of these messagses).
  virtual void setDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView &prevViewDesc) = 0;

  // DescriptorOfLastNewView contains pointers to messages (the content of the
  // messages should be copied, the caller is the owner of these messages).
  virtual void setDescriptorOfLastNewView(
      const DescriptorOfLastNewView &prevViewDesc) = 0;

  virtual void setDescriptorOfLastExecution(
      const DescriptorOfLastExecution &prevViewDesc) = 0;

  // We have two windows "SeqNumWindow" and "CheckWindow"
  // TODO(GG): explain the windows.

  virtual void setLastStableSeqNum(const SeqNum &s) = 0;

  // The window of sequence numbers is:
  // { i | LS + 1 <= i <= LS + kWorkWindowSize }
  // where LS=lastStableSeqNum
  //
  // The window of checkpoints is:
  // { LS, LS + CWS, LS + 2 * CWS }
  // where LS=lastStableSeqNum and CWS=checkpointWindowSize

  virtual void clearSeqNumWindow() = 0;

  virtual void setPrePrepareMsgInSeqNumWindow(const SeqNum &s,
                                              const PrePrepareMsg *const &m) = 0;
  virtual void setSlowStartedInSeqNumWindow(const SeqNum &s,
                                            const bool &slowStarted) = 0;
  virtual void setFullCommitProofMsgInSeqNumWindow(
      const SeqNum &s, const FullCommitProofMsg *const &m) = 0;
  virtual void setForceCompletedInSeqNumWindow(const SeqNum &s,
                                               const bool &forceCompleted) = 0;
  virtual void setPrepareFullMsgInSeqNumWindow(
      const SeqNum &s, const PrepareFullMsg *const &m) = 0;
  virtual void setCommitFullMsgInSeqNumWindow(const SeqNum &s,
                                              const CommitFullMsg *const &m) = 0;
  virtual void setCheckpointMsgInCheckWindow(const SeqNum &s,
                                             const CheckpointMsg *const &m) = 0;
  virtual void setCompletedMarkInCheckWindow(const SeqNum &s,
                                             const bool &f) = 0;

  //////////////////////////////////////////////////////////////////////////
  // Read methods (should only be used before using write-only transactions)
  //////////////////////////////////////////////////////////////////////////

  virtual bool hasReplicaConfig() = 0;
  virtual ReplicaConfig getReplicaConfig() = 0;

  virtual bool getFetchingState() = 0;
  virtual SeqNum getLastExecutedSeqNum() = 0;
  virtual SeqNum getPrimaryLastUsedSeqNum() = 0;
  virtual SeqNum getStrictLowerBoundOfSeqNums() = 0;
  virtual ViewNum getLastViewThatTransferredSeqNumbersFullyExecuted() = 0;

  virtual bool hasDescriptorOfLastExitFromView() = 0;
  virtual DescriptorOfLastExitFromView
  getAndAllocateDescriptorOfLastExitFromView() = 0;

  virtual bool hasDescriptorOfLastNewView() = 0;
  virtual DescriptorOfLastNewView getAndAllocateDescriptorOfLastNewView() = 0;

  virtual bool hasDescriptorOfLastExecution() = 0;
  virtual DescriptorOfLastExecution getDescriptorOfLastExecution() = 0;

  virtual SeqNum getLastStableSeqNum() = 0;

  virtual PrePrepareMsg *getAndAllocatePrePrepareMsgInSeqNumWindow(
      const SeqNum &s) = 0;
  virtual bool getSlowStartedInSeqNumWindow(const SeqNum &s) = 0;
  virtual FullCommitProofMsg *getAndAllocateFullCommitProofMsgInSeqNumWindow(
      const SeqNum &s) = 0;
  virtual bool getForceCompletedInSeqNumWindow(const SeqNum &s) = 0;
  virtual PrepareFullMsg *getAndAllocatePrepareFullMsgInSeqNumWindow(
      const SeqNum &s) = 0;
  virtual CommitFullMsg *getAndAllocateCommitFullMsgInSeqNumWindow(
      const SeqNum &s) = 0;
  virtual CheckpointMsg *getAndAllocateCheckpointMsgInCheckWindow(
      const SeqNum &s) = 0;
  virtual bool getCompletedMarkInCheckWindow(const SeqNum &s) = 0;
};

}  // namespace impl
}  // namespace bftEngine
