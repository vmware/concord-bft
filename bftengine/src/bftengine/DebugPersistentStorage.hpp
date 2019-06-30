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
#include "SequenceWithActiveWindow.hpp"

namespace bftEngine {
namespace impl {

class DebugPersistentStorage : public PersistentStorage {
 public:
  DebugPersistentStorage(uint16_t fVal, uint16_t cVal);

  // Inherited via PersistentStorage
  virtual uint8_t beginWriteTran() override;
  virtual uint8_t endWriteTran() override;
  virtual bool isInWriteTran() const override;
  virtual void setReplicaConfig(ReplicaConfig config) override;
  virtual void setFetchingState(const bool f) override;
  virtual void setLastExecutedSeqNum(const SeqNum s) override;
  virtual void setPrimaryLastUsedSeqNum(const SeqNum s) override;
  virtual void setStrictLowerBoundOfSeqNums(const SeqNum s) override;
  virtual void setLastViewThatTransferredSeqNumbersFullyExecuted(
      const ViewNum v) override;
  virtual void setDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView& prevViewDesc) override;
  virtual void setDescriptorOfLastNewView(
      const DescriptorOfLastNewView& prevViewDesc) override;
  virtual void setDescriptorOfLastExecution(
      const DescriptorOfLastExecution& prevViewDesc) override;
  virtual void setLastStableSeqNum(const SeqNum s) override;
  virtual void clearSeqNumWindow() override;
  virtual void setPrePrepareMsgInSeqNumWindow(
      const SeqNum s, const PrePrepareMsg* const m) override;
  virtual void setSlowStartedInSeqNumWindow(const SeqNum s,
                                            const bool slowStarted) override;
  virtual void setFullCommitProofMsgInSeqNumWindow(
      const SeqNum s, const FullCommitProofMsg* const m) override;
  virtual void setForceCompletedInSeqNumWindow(
      const SeqNum s, const bool forceCompleted) override;
  virtual void setPrepareFullMsgInSeqNumWindow(
      const SeqNum s, const PrepareFullMsg* const m) override;
  virtual void setCommitFullMsgInSeqNumWindow(
      const SeqNum s, const CommitFullMsg* const m) override;
  virtual void setCheckpointMsgInCheckWindow(
      const SeqNum s, const CheckpointMsg* const m) override;
  virtual void setCompletedMarkInCheckWindow(const SeqNum s,
                                             const bool f) override;
  virtual bool hasReplicaConfig() override;
  virtual ReplicaConfig getReplicaConig() override;
  virtual bool getFetchingState() override;
  virtual SeqNum getLastExecutedSeqNum() override;
  virtual SeqNum getPrimaryLastUsedSeqNum() override;
  virtual SeqNum getStrictLowerBoundOfSeqNums() override;
  virtual ViewNum getLastViewThatTransferredSeqNumbersFullyExecuted() override;
  virtual bool hasDescriptorOfLastExitFromView() override;
  virtual DescriptorOfLastExitFromView
  getAndAllocateDescriptorOfLastExitFromView() override;
  virtual bool hasDescriptorOfLastNewView() override;
  virtual DescriptorOfLastNewView getAndAllocateDescriptorOfLastNewView()
      override;
  virtual bool hasDescriptorOfLastExecution() override;
  virtual DescriptorOfLastExecution getDescriptorOfLastExecution() override;
  virtual SeqNum getLastStableSeqNum() override;
  virtual PrePrepareMsg* getAndAllocatePrePrepareMsgInSeqNumWindow(
      const SeqNum s) override;
  virtual bool getSlowStartedInSeqNumWindow(const SeqNum s) override;
  virtual FullCommitProofMsg* getAndAllocateFullCommitProofMsgInSeqNumWindow(
      const SeqNum s) override;
  virtual bool getForceCompletedInSeqNumWindow(const SeqNum s) override;
  virtual PrepareFullMsg* getAndAllocatePrepareFullMsgInSeqNumWindow(
      const SeqNum s) override;
  virtual CommitFullMsg* getAndAllocateCommitFullMsgInSeqNumWindow(
      const SeqNum s) override;
  virtual CheckpointMsg* getAndAllocateCheckpointMsgInCheckWindow(
      const SeqNum s) override;
  virtual bool getCompletedMarkInCheckWindow(const SeqNum s) override;

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
      DescriptorOfLastExitFromView{
          0, 0, 0, std::vector<ViewsManager::PrevViewInfo>(0)};
  bool hasDescriptorOfLastNewView_ = false;
  DescriptorOfLastNewView descriptorOfLastNewView_ =
      DescriptorOfLastNewView{0, nullptr, std::vector<ViewChangeMsg*>(0), 0};
  bool hasDescriptorOfLastExecution_ = false;
  DescriptorOfLastExecution descriptorOfLastExecution_ =
      DescriptorOfLastExecution{0, Bitmap()};

  SeqNum lastStableSeqNum_ = 0;

  struct SeqNumData {
    PrePrepareMsg* prePrepareMsg;
    bool slowStarted;
    FullCommitProofMsg* fullCommitProofMsg;
    bool forceCompleted;
    PrepareFullMsg* prepareFullMsg;
    CommitFullMsg* commitFullMsg;
  };

  struct CheckData {
    CheckpointMsg* checkpointMsg;
    bool completedMark;
  };

  struct WindowFuncs {
    static void init(SeqNumData& i, void* d);
    static void free(SeqNumData& i);
    static void reset(SeqNumData& i);

    static void init(CheckData& i, void* d);
    static void free(CheckData& i);
    static void reset(CheckData& i);
  };

  // range: lastStableSeqNum+1 <= i <= lastStableSeqNum + kWorkWindowSize
  SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumData, WindowFuncs>
      seqNumWindow;

  // range: TODO(GG): !!!!!!!
  SequenceWithActiveWindow<kWorkWindowSize + checkpointWindowSize,
                           checkpointWindowSize,
                           SeqNum,
                           CheckData,
                           WindowFuncs>
      checkWindow;
};

}  // namespace impl
}  // namespace bftEngine
