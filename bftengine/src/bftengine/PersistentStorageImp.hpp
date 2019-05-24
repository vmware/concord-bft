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
#include "MetadataStorage.hpp"
#include "SequenceWithActiveWindow.hpp"

namespace bftEngine {
namespace impl {

class PersistentStorageImp : public PersistentStorage {
 public:
  PersistentStorageImp(uint16_t fVal, uint16_t cVal);
  virtual ~PersistentStorageImp() { delete config_; }

  uint8_t beginWriteTran() override;
  uint8_t endWriteTran() override;
  bool isInWriteTran() const override;
  void setReplicaConfig(const ReplicaConfig &config) override;
  void setFetchingState(const bool &state) override;
  void setLastExecutedSeqNum(const SeqNum &seqNum) override;
  void setPrimaryLastUsedSeqNum(const SeqNum &seqNum) override;
  void setStrictLowerBoundOfSeqNums(const SeqNum &seqNum) override;
  void setLastViewThatTransferredSeqNumbersFullyExecuted(
      const ViewNum &view) override;
  void setDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView &prevViewDesc) override;
  void setDescriptorOfLastNewView(
      const DescriptorOfLastNewView &prevViewDesc) override;
  void setDescriptorOfLastExecution(
      const DescriptorOfLastExecution &prevViewDesc) override;
  void setLastStableSeqNum(const SeqNum &seqNum) override;
  void clearSeqNumWindow() override;
  void setPrePrepareMsgInSeqNumWindow(
      const SeqNum &seqNum, const PrePrepareMsg *const &msg) override;
  void setSlowStartedInSeqNumWindow(const SeqNum &seqNum,
                                    const bool &slowStarted) override;
  void setFullCommitProofMsgInSeqNumWindow(
      const SeqNum &seqNum, const FullCommitProofMsg *const &msg) override;
  void setForceCompletedInSeqNumWindow(
      const SeqNum &seqNum, const bool &forceCompleted) override;
  void setPrepareFullMsgInSeqNumWindow(
      const SeqNum &seqNum, const PrepareFullMsg *const &msg) override;
  void setCommitFullMsgInSeqNumWindow(
      const SeqNum &seqNum, const CommitFullMsg *const &msg) override;
  void setCheckpointMsgInCheckWindow(
      const SeqNum &seqNum, const CheckpointMsg *const &msg) override;
  void setCompletedMarkInCheckWindow(const SeqNum &seqNum,
                                     const bool &completed) override;

  bool hasReplicaConfig() override;
  ReplicaConfig getReplicaConfig() override;
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
      const SeqNum &seqNum) override;
  bool getSlowStartedInSeqNumWindow(const SeqNum &seqNum) override;
  FullCommitProofMsg *getAndAllocateFullCommitProofMsgInSeqNumWindow(
      const SeqNum &seqNum) override;
  bool getForceCompletedInSeqNumWindow(const SeqNum &seqNum) override;
  PrepareFullMsg *getAndAllocatePrepareFullMsgInSeqNumWindow(
      const SeqNum &seqNum) override;
  CommitFullMsg *getAndAllocateCommitFullMsgInSeqNumWindow(
      const SeqNum &seqNum) override;
  CheckpointMsg *getAndAllocateCheckpointMsgInCheckWindow(
      const SeqNum &seqNum) override;
  bool getCompletedMarkInCheckWindow(const SeqNum &seqNum) override;

  void init(MetadataStorage *&metadataStorage);

 protected:
  bool setIsAllowed() const;
  bool getIsAllowed() const;
  bool nonExecSetIsAllowed() const;

 private:
  MetadataStorage *metadataStorage_ = nullptr;
  uint8_t numOfNestedTransactions_ = 0;

  const uint16_t fVal_;
  const uint16_t cVal_;

  // Parameters to be saved persistently
  SeqNum lastStableSeqNum_ = 0;
  SeqNum lastExecutedSeqNum_ = 0;
  SeqNum primaryLastUsedSeqNum_ = 0;
  SeqNum strictLowerBoundOfSeqNums_ = 0;
  ViewNum lastViewThatTransferredSeqNumbersFullyExecuted_ = 0;
  bool fetchingState_ = false;

  ReplicaConfig *config_ = nullptr;

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

  struct SeqNumData {
    PrePrepareMsg *prePrepareMsg = nullptr;
    bool slowStarted = false;
    FullCommitProofMsg *fullCommitProofMsg = nullptr;
    bool forceCompleted = false;
    PrepareFullMsg *prepareFullMsg = nullptr;
    CommitFullMsg *commitFullMsg = nullptr;

    void reset() {
      delete prePrepareMsg;
      delete fullCommitProofMsg;
      delete prepareFullMsg;
      delete commitFullMsg;
      prePrepareMsg = nullptr;
      slowStarted = false;
      fullCommitProofMsg = nullptr;
      forceCompleted = false;
      prepareFullMsg = nullptr;
      commitFullMsg = nullptr;
    }

    static uint32_t maxSize() {
      return (PrePrepareMsg::maxSizeOfPrePrepareMsg() +
          sizeof(slowStarted) +
          FullCommitProofMsg::maxSizeOfFullCommitProofMsg() +
          sizeof(forceCompleted) +
          PrepareFullMsg::maxSizeOfPrepareFull() +
          CommitFullMsg::maxSizeOfCommitFull());
    }
  };

  struct CheckData {
    CheckpointMsg *checkpointMsg = nullptr;
    bool completedMark = false;

    void reset() {
      delete checkpointMsg;
      checkpointMsg = nullptr;
      completedMark = false;
    }
    static uint32_t maxSize() {
      return (CheckpointMsg::maxSizeOfCheckpointMsg() + sizeof(completedMark));
    }
  };

  struct WindowFuncs {
    static void init(SeqNumData &seqNumData, void *data) {}
    static void free(SeqNumData &seqNumData);
    static void reset(SeqNumData &seqNumData);

    static void init(CheckData &checkData, void *data) {}
    static void free(CheckData &checkData);
    static void reset(CheckData &checkData);
  };

  typedef SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumData,
                                   WindowFuncs> SeqNumWindow;

  typedef SequenceWithActiveWindow<kWorkWindowSize + checkpointWindowSize,
                                   checkpointWindowSize, SeqNum, CheckData,
                                   WindowFuncs> CheckWindow;

  SeqNumWindow seqNumWindow_;
  CheckWindow checkWindow_;
};

}  // namespace impl
}  // namespace bftEngine
