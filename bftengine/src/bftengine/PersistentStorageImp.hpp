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
#include "ReplicaConfigSerializer.hpp"
#include "PersistentStorageWindows.hpp"

namespace bftEngine {
namespace impl {

enum ConstMetadataParameterIds {
  FETCHING_STATE = 0,
  LAST_EXEC_SEQ_NUM,
  PRIMARY_LAST_USED_SEQ_NUM,
  LOWER_BOUND_OF_SEQ_NUM,
  LAST_VIEW_TRANSFERRED_SEQ_NUM,
  LAST_STABLE_SEQ_NUM,
  REPLICA_CONFIG,
  CONST_METADATA_PARAMETERS_NUM
};

const uint16_t seqWinSize = kWorkWindowSize;

const uint16_t checkWinSize =
    (kWorkWindowSize + checkpointWindowSize) / checkpointWindowSize;

// SEQ_NUM_WINDOW contains kWorkWindowSize objects plus one - for simple window
// parameters.
const uint16_t numOfSeqWinObjs = seqWinSize + 1;

// CHECK_WINDOW contains checkWinSize objects plus one - for simple window
// parameters.
const uint16_t numOfCheckWinObjs = checkWinSize + 1;

enum WinMetadataParameterIds {
  SEQ_NUM_WINDOW = CONST_METADATA_PARAMETERS_NUM,
  CHECK_WINDOW = SEQ_NUM_WINDOW + numOfSeqWinObjs,
  WIN_PARAMETERS_NUM = CHECK_WINDOW + numOfCheckWinObjs + 1
};

// LAST_EXIT_FROM_VIEW_DESC contains up to kWorkWindowSize descriptor objects
// (one per PrevViewInfo) plus one - for simple descriptor parameters.
const uint16_t numOfLastExitFromViewDescObjs = kWorkWindowSize + 1;

// LAST_NEW_VIEW_DESC contains numOfReplicas_ (2*f + 2*c + 1) descriptor
// objects plus one - for simple descriptor parameters.

enum DescMetadataParameterIds {
  LAST_EXIT_FROM_VIEW_DESC = WIN_PARAMETERS_NUM,
  LAST_EXEC_DESC = LAST_EXIT_FROM_VIEW_DESC + numOfLastExitFromViewDescObjs,
  LAST_NEW_VIEW_DESC
};

class PersistentStorageImp : public PersistentStorage {
 public:
  PersistentStorageImp(uint16_t fVal, uint16_t cVal);
  virtual ~PersistentStorageImp() { delete configSerializer_; }

  uint8_t beginWriteTran() override;
  uint8_t endWriteTran() override;
  bool isInWriteTran() const override;

  // Setters
  void setReplicaConfig(const ReplicaConfig &config) override;
  void setFetchingState(const bool &state) override;
  void setLastExecutedSeqNum(const SeqNum &seqNum) override;
  void setPrimaryLastUsedSeqNum(const SeqNum &seqNum) override;
  void setStrictLowerBoundOfSeqNums(const SeqNum &seqNum) override;
  void setLastViewThatTransferredSeqNumbersFullyExecuted(
      const ViewNum &view) override;
  void setLastStableSeqNum(const SeqNum &seqNum) override;
  void setDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView &prevViewDesc) override;
  void setDescriptorOfLastNewView(
      const DescriptorOfLastNewView &prevViewDesc) override;
  void setDescriptorOfLastExecution(
      const DescriptorOfLastExecution &prevViewDesc) override;
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
  // Getters
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

  void clearSeqNumWindow() override;
  bool hasReplicaConfig() const override;
  void init(MetadataStorage *&metadataStorage);

 protected:
  bool setIsAllowed() const;
  bool getIsAllowed() const;
  bool nonExecSetIsAllowed() const;
  SeqNum getSeqNum(ConstMetadataParameterIds id, uint32_t size);

 private:
  void initMetadataStorage(MetadataStorage *&metadataStorage);
  void setDefaultsInMetadataStorage();
  void verifySetDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView &desc) const;
  void verifyPrevViewInfo(const DescriptorOfLastExitFromView &desc) const;
  void verifySetDescriptorOfLastNewView(
      const DescriptorOfLastNewView &desc) const;
  void verifyLastNewViewMsgs(const DescriptorOfLastNewView &desc) const;
  void verifyDescriptorOfLastExecution(
      const DescriptorOfLastExecution &desc) const;

  void saveDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView &newDesc, bool setAll);
  void setDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView &desc, bool setAll);
  void initDescriptorOfLastExitFromView(
      const DescriptorOfLastExitFromView &desc);
  void saveDescriptorOfLastNewView(
      const DescriptorOfLastNewView &newDesc, bool setAll);
  void setDescriptorOfLastNewView(
      const DescriptorOfLastNewView &desc, bool setAll);
  void initDescriptorOfLastNewView(const DescriptorOfLastNewView &desc);
  void saveDescriptorOfLastExecution(const DescriptorOfLastExecution &newDesc);
  void setDescriptorOfLastExecution(
      const DescriptorOfLastExecution &desc, bool setAll);
  void initDescriptorOfLastExecution(const DescriptorOfLastExecution &desc);

  void setSeqNumDataElement(SeqNum index, char *&buf) const;
  void setSeqNumDataElement(SeqNum seqNum) const;
  void setCheckDataElement(SeqNum index, char *&buf) const;
  void setCheckDataElement(SeqNum seqNum) const;
  void setSeqNumWindow() const;
  void setCheckWindow() const;

  void getSeqNumDataElement(SeqNum index, char *&buf);
  void getSeqNumDataElement(SeqNum seqNum);
  void getCheckDataElement(SeqNum index, char *&buf);
  void getCheckDataElement(SeqNum seqNum);
  void getSeqNumWindow();
  void getCheckWindow();

  void setFetchingStateInternal(const bool &state);
  void setLastExecutedSeqNumInternal(const SeqNum &seqNum);
  void setPrimaryLastUsedSeqNumInternal(const SeqNum &seqNum);
  void setStrictLowerBoundOfSeqNumsInternal(const SeqNum &seqNum);
  void setLastViewTransferredSeqNumbersInternal(const ViewNum &view);
  void setLastStableSeqNumInternal(const SeqNum &seqNum);

 private:
  MetadataStorage *metadataStorage_ = nullptr;
  uint8_t numOfNestedTransactions_ = 0;

  const uint32_t numOfReplicas_;
  const uint32_t metadataParamsNum_;
  const SeqNum seqNumWindowFirst_ = 1;
  SeqNum seqNumWindowLast_ = seqNumWindowFirst_;
  const SeqNum checkWindowFirst_ = 0;
  SeqNum checkWindowLast_ = checkWindowFirst_;

  // Parameters to be saved persistently
  bool fetchingState_ = false;
  SeqNum lastExecutedSeqNum_ = 0;
  SeqNum primaryLastUsedSeqNum_ = 0;
  SeqNum strictLowerBoundOfSeqNums_ = 0;
  ViewNum lastViewTransferredSeqNum_ = 0;
  SeqNum lastStableSeqNum_ = 0;

  ReplicaConfigSerializer *configSerializer_ = nullptr;

  DescriptorOfLastExitFromView descriptorOfLastExitFromView_;
  DescriptorOfLastNewView descriptorOfLastNewView_;
  DescriptorOfLastExecution descriptorOfLastExecution_;

  SeqNumWindow seqNumWindow_;
  CheckWindow checkWindow_;
};

}  // namespace impl
}  // namespace bftEngine
