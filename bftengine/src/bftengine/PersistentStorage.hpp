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

#include <vector>

namespace bftEngine {
namespace impl {
class PrePrepareMsg;
class CheckpointMsg;
class ViewChangeMsg;
class NewViewMsg;
class FullCommitProofMsg;
class PrepareFullMsg;
class CommitFullMsg;

// The PersistentStorage interface is used to write/read the concord-bft state 
// to/from a persistent storage. In case the replica’s process is killed, 
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
    // view >= 0
    ViewNum view;

    // lastStable >= 0
    SeqNum lastStable;

    // lastExecuted >= lastStable
    SeqNum lastExecuted;

    // elements.size() <= kWorkWindowSize
    // The messages in elements[i] may be null
    std::vector<ViewsManager::PrevViewInfo> elements;
  };

  struct DescriptorOfLastNewView {
    // view >= 1
    ViewNum view;

    // newViewMsg != nullptr
    NewViewMsg* newViewMsg;

    // viewChangeMsgs.size() == 2*F + 2*C + 1
    // The messages in viewChangeMsgs will never be null
    std::vector<ViewChangeMsg*> viewChangeMsgs;

    // maxSeqNumTransferredFromPrevViews >= 0
    SeqNum maxSeqNumTransferredFromPrevViews;
  };

  struct DescriptorOfLastExecution {
    // executedSeqNum >= 1
    SeqNum executedSeqNum;

    // 1 <= validRequests.numOfBits() <= maxNumOfRequestsInBatch
    Bitmap validRequests;
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

  virtual void setReplicaConfig(ReplicaConfig config) = 0;

  virtual void setFetchingState(const bool f) = 0;
  virtual void setLastExecutedSeqNum(const SeqNum s) = 0;
  virtual void setPrimaryLastUsedSeqNum(const SeqNum s) = 0;
  virtual void setStrictLowerBoundOfSeqNums(const SeqNum s) = 0;
  virtual void setLastViewThatTransferredSeqNumbersFullyExecuted(
      const ViewNum v) = 0;

  // DescriptorOfLastExitFromView contains pointers to messages (the content of
  // the messages should be copied, the caller is the owner of these messagse).
  virtual void setDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView& prevViewDesc) = 0;

  // DescriptorOfLastNewView contains pointers to messages (the content of the
  // messages should be copied, the caller is the owner of these messagse).
  virtual void setDescriptorOfLastNewView(
      const DescriptorOfLastNewView& prevViewDesc) = 0;

  virtual void setDescriptorOfLastExecution(
      const DescriptorOfLastExecution& prevViewDesc) = 0;

	// We have two windows "SeqNumWindow" and "CheckWindow"	
	// TODO(GG): explain the windows. 

  virtual void setLastStableSeqNum(const SeqNum s) = 0;
  
	//
  // The window of sequence numbers is:
  // { i | LS + 1 <= i <= LS + kWorkWindowSize }
  // where LS=lastStableSeqNum
  //
  // The window of checkpoints is:
  // { LS, LS + CWS, LS + 2 * CWS }
  // where LS=lastStableSeqNum and CWS=checkpointWindowSize

  virtual void clearSeqNumWindow() = 0;

  virtual void setPrePrepareMsgInSeqNumWindow(const SeqNum s,
                                              const PrePrepareMsg* const m) = 0;
  virtual void setSlowStartedInSeqNumWindow(const SeqNum s,
                                            const bool slowStarted) = 0;
  virtual void setFullCommitProofMsgInSeqNumWindow(
      const SeqNum s, const FullCommitProofMsg* const m) = 0;
  virtual void setForceCompletedInSeqNumWindow(const SeqNum s,
                                               const bool forceCompleted) = 0;
  virtual void setPrepareFullMsgInSeqNumWindow(
      const SeqNum s, const PrepareFullMsg* const m) = 0;
  virtual void setCommitFullMsgInSeqNumWindow(const SeqNum s,
                                              const CommitFullMsg* const m) = 0;

  virtual void setCheckpointMsgInCheckWindow(const SeqNum s,
                                             const CheckpointMsg* const m) = 0;
  virtual void setCompletedMarkInCheckWindow(const SeqNum s, const bool f) = 0;

  //////////////////////////////////////////////////////////////////////////
  // Read methods (should only be used before using write-only transactions)
  //////////////////////////////////////////////////////////////////////////

  virtual bool hasReplicaConfig() = 0;
  virtual ReplicaConfig getReplicaConig() = 0;

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

  virtual PrePrepareMsg* getAndAllocatePrePrepareMsgInSeqNumWindow(
      const SeqNum s) = 0;
  virtual bool getSlowStartedInSeqNumWindow(const SeqNum s) = 0;
  virtual FullCommitProofMsg* getAndAllocateFullCommitProofMsgInSeqNumWindow(
      const SeqNum s) = 0;
  virtual bool getForceCompletedInSeqNumWindow(const SeqNum s) = 0;
  virtual PrepareFullMsg* getAndAllocatePrepareFullMsgInSeqNumWindow(
      const SeqNum s) = 0;
  virtual CommitFullMsg* getAndAllocateCommitFullMsgInSeqNumWindow(
      const SeqNum s) = 0;

  virtual CheckpointMsg* getAndAllocateCheckpointMsgInCheckWindow(
      const SeqNum s) = 0;
  virtual bool getCompletedMarkInCheckWindow(const SeqNum s) = 0;
};

}  // namespace impl
}  // namespace bftEngine
