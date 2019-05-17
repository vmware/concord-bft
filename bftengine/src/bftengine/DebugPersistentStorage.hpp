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
  uint8_t beginWriteTran() override;
  uint8_t endWriteTran() override;
  bool isInWriteTran() const override;
  void setReplicaConfig(ReplicaConfig config) override;
  void setFetchingState(const bool &f) override;
  void setLastExecutedSeqNum(const SeqNum &s) override;
  void setPrimaryLastUsedSeqNum(const SeqNum &s) override;
  void setStrictLowerBoundOfSeqNums(const SeqNum &s) override;
  void setLastViewThatTransferredSeqNumbersFullyExecuted(
      const ViewNum &v) override;
  void setDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView &prevViewDesc) override;
  void setDescriptorOfLastNewView(
      const DescriptorOfLastNewView &prevViewDesc) override;
  void setDescriptorOfLastExecution(
      const DescriptorOfLastExecution &prevViewDesc) override;
  void setLastStableSeqNum(const SeqNum &s) override;
  void clearSeqNumWindow() override;
  void setPrePrepareMsgInSeqNumWindow(
      const SeqNum &s, const PrePrepareMsg *const &m) override;
  void setSlowStartedInSeqNumWindow(const SeqNum &s,
                                    const bool &slowStarted) override;
  void setFullCommitProofMsgInSeqNumWindow(
      const SeqNum &s, const FullCommitProofMsg *const &m) override;
  void setForceCompletedInSeqNumWindow(
      const SeqNum &s, const bool &forceCompleted) override;
  void setPrepareFullMsgInSeqNumWindow(
      const SeqNum &s, const PrepareFullMsg *const &m) override;
  void setCommitFullMsgInSeqNumWindow(
      const SeqNum &s, const CommitFullMsg *const &m) override;
  void setCheckpointMsgInCheckWindow(
      const SeqNum &s, const CheckpointMsg *const &m) override;
  void setCompletedMarkInCheckWindow(const SeqNum &s, const bool &f) override;
  bool hasReplicaConfig() override;
  ReplicaConfig getReplicaConig() override;
  bool getFetchingState() override;
  SeqNum getLastExecutedSeqNum() override;
  SeqNum getPrimaryLastUsedSeqNum() override;
  SeqNum getStrictLowerBoundOfSeqNums() override;
  ViewNum getLastViewThatTransferredSeqNumbersFullyExecuted() override;
  bool hasDescriptorOfLastExitFromView() override;
  DescriptorOfLastExitFromView
  getAndAllocateDescriptorOfLastExitFromView() override;
  bool hasDescriptorOfLastNewView() override;
  DescriptorOfLastNewView getAndAllocateDescriptorOfLastNewView() override;
  bool hasDescriptorOfLastExecution() override;
  DescriptorOfLastExecution getDescriptorOfLastExecution() override;
  SeqNum getLastStableSeqNum() override;
  PrePrepareMsg *getAndAllocatePrePrepareMsgInSeqNumWindow(
      const SeqNum &s) override;
  bool getSlowStartedInSeqNumWindow(const SeqNum &s) override;
  FullCommitProofMsg *getAndAllocateFullCommitProofMsgInSeqNumWindow(
      const SeqNum &s) override;
  bool getForceCompletedInSeqNumWindow(const SeqNum &s) override;
  PrepareFullMsg *getAndAllocatePrepareFullMsgInSeqNumWindow(
      const SeqNum &s) override;
  CommitFullMsg *getAndAllocateCommitFullMsgInSeqNumWindow(
      const SeqNum &s) override;
  CheckpointMsg *getAndAllocateCheckpointMsgInCheckWindow(
      const SeqNum &s) override;
  bool getCompletedMarkInCheckWindow(const SeqNum &s) override;

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
      DescriptorOfLastNewView{0, nullptr, std::vector<ViewChangeMsg *>(0), 0};
  bool hasDescriptorOfLastExecution_ = false;
  DescriptorOfLastExecution descriptorOfLastExecution_ =
      DescriptorOfLastExecution{0, Bitmap()};

  SeqNum lastStableSeqNum_ = 0;

  struct SeqNumData {
    PrePrepareMsg *prePrepareMsg;
    bool slowStarted;
    FullCommitProofMsg *fullCommitProofMsg;
    bool forceCompleted;
    PrepareFullMsg *prepareFullMsg;
    CommitFullMsg *commitFullMsg;
  };

  struct CheckData {
    CheckpointMsg *checkpointMsg;
    bool completedMark;
  };

  struct WindowFuncs {
    static void init(SeqNumData &i, void *d);
    static void free(SeqNumData &i);
    static void reset(SeqNumData &i);

    static void init(CheckData &i, void *d);
    static void free(CheckData &i);
    static void reset(CheckData &i);
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
