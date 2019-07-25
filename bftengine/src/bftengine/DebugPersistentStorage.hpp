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

#include "PersistentStorage.hpp"
#include "PersistentStorageWindows.hpp"

namespace bftEngine {
namespace impl {

class DebugPersistentStorage : public PersistentStorage {
 public:
  DebugPersistentStorage(uint16_t fVal, uint16_t cVal);
  ~DebugPersistentStorage() override = default;

  // Inherited via PersistentStorage
  uint8_t beginWriteTran() override;
  uint8_t endWriteTran() override;
  bool isInWriteTran() const override;
  void setReplicaConfig(const ReplicaConfig &config) override;
  void setFetchingState(bool state) override;
  void setLastExecutedSeqNum(SeqNum seqNum) override;
  void setPrimaryLastUsedSeqNum(SeqNum seqNum) override;
  void setStrictLowerBoundOfSeqNums(SeqNum seqNum) override;
  void setLastViewThatTransferredSeqNumbersFullyExecuted(ViewNum view) override;
  void setDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView& prevViewDesc) override;
  void setDescriptorOfLastNewView(const DescriptorOfLastNewView& prevViewDesc) override;
  void setDescriptorOfLastExecution(const DescriptorOfLastExecution& prevViewDesc) override;
  void setLastStableSeqNum(SeqNum seqNum) override;
  void clearSeqNumWindow() override;
  void setPrePrepareMsgInSeqNumWindow(SeqNum seqNum, PrePrepareMsg *msg) override;
  void setSlowStartedInSeqNumWindow(SeqNum seqNum, bool slowStarted) override;
  void setFullCommitProofMsgInSeqNumWindow(SeqNum s, FullCommitProofMsg *msg) override;
  void setForceCompletedInSeqNumWindow(SeqNum seqNum, bool forceCompleted) override;
  void setPrepareFullMsgInSeqNumWindow(SeqNum seqNum, PrepareFullMsg *msg) override;
  void setCommitFullMsgInSeqNumWindow(SeqNum seqNum, CommitFullMsg *msg) override;
  void setCheckpointMsgInCheckWindow(SeqNum seqNum, CheckpointMsg *msg) override;
  void setCompletedMarkInCheckWindow(SeqNum seqNum, bool mark) override;
  bool hasReplicaConfig() const override;
  ReplicaConfig getReplicaConfig() override;
  bool getFetchingState() override;
  SeqNum getLastExecutedSeqNum() override;
  SeqNum getPrimaryLastUsedSeqNum() override;
  SeqNum getStrictLowerBoundOfSeqNums() override;
  ViewNum getLastViewThatTransferredSeqNumbersFullyExecuted() override;
  bool hasDescriptorOfLastExitFromView() override;
  DescriptorOfLastExitFromView getAndAllocateDescriptorOfLastExitFromView() override;
  bool hasDescriptorOfLastNewView() override;
  DescriptorOfLastNewView getAndAllocateDescriptorOfLastNewView() override;
  bool hasDescriptorOfLastExecution() override;
  DescriptorOfLastExecution getDescriptorOfLastExecution() override;
  SeqNum getLastStableSeqNum() override;
  PrePrepareMsg* getAndAllocatePrePrepareMsgInSeqNumWindow(SeqNum seqNum) override;
  bool getSlowStartedInSeqNumWindow(SeqNum seqNum) override;
  FullCommitProofMsg* getAndAllocateFullCommitProofMsgInSeqNumWindow(SeqNum seqNum) override;
  bool getForceCompletedInSeqNumWindow(SeqNum seqNum) override;
  PrepareFullMsg* getAndAllocatePrepareFullMsgInSeqNumWindow(SeqNum seqNum) override;
  CommitFullMsg* getAndAllocateCommitFullMsgInSeqNumWindow(SeqNum seqNum) override;
  CheckpointMsg* getAndAllocateCheckpointMsgInCheckWindow(SeqNum seqNum) override;
  bool getCompletedMarkInCheckWindow(SeqNum seqNum) override;

 protected:
  bool setIsAllowed() const;
  bool getIsAllowed() const;
  bool nonExecSetIsAllowed() const;

  const uint16_t fVal_;
  const uint16_t cVal_;

  uint8_t numOfNestedTransactions = 0;

  bool hasConfig_ = false;
  ReplicaConfig config_;

  bool fetchingState_ = false;
  SeqNum lastExecutedSeqNum_ = 0;
  SeqNum primaryLastUsedSeqNum_ = 0;
  SeqNum strictLowerBoundOfSeqNums_ = 0;
  ViewNum lastViewThatTransferredSeqNumbersFullyExecuted_ = 0;

  bool hasDescriptorOfLastExitFromView_ = false;
  DescriptorOfLastExitFromView descriptorOfLastExitFromView_ =
      DescriptorOfLastExitFromView{0, 0, 0, std::vector<ViewsManager::PrevViewInfo>(0), 0, 0};
  bool hasDescriptorOfLastNewView_ = false;
  DescriptorOfLastNewView descriptorOfLastNewView_ =
      DescriptorOfLastNewView{0, nullptr, std::vector<ViewChangeMsg*>(0), nullptr, 0, 0};
  bool hasDescriptorOfLastExecution_ = false;
  DescriptorOfLastExecution descriptorOfLastExecution_ = DescriptorOfLastExecution{0, Bitmap()};

  SeqNum lastStableSeqNum_ = 0;

  // range: lastStableSeqNum+1 <= i <= lastStableSeqNum + kWorkWindowSize
  SeqNumWindow seqNumWindow;

  // range: TODO(GG): !!!!!!!
  CheckWindow checkWindow;
};

}  // namespace impl
}  // namespace bftEngine
