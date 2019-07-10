// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of sub-components with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE file.

#pragma once

#include "PersistentStorage.hpp"
#include "MetadataStorage.hpp"
#include "ReplicaConfigSerializer.hpp"
#include "PersistentStorageWindows.hpp"

namespace bftEngine {
namespace impl {

enum ConstMetadataParameterIds {
  VERSION_PARAMETER = 0,
  FETCHING_STATE,
  LAST_EXEC_SEQ_NUM,
  PRIMARY_LAST_USED_SEQ_NUM,
  LOWER_BOUND_OF_SEQ_NUM,
  LAST_VIEW_TRANSFERRED_SEQ_NUM,
  LAST_STABLE_SEQ_NUM,
  REPLICA_CONFIG,
  CONST_METADATA_PARAMETERS_NUM
};

const uint16_t seqWinSize = kWorkWindowSize;

constexpr uint16_t numOfSeqNumWinParameters = SeqNumData::getNumOfParams();
const uint16_t numOfSeqNumWinObjs = seqWinSize * numOfSeqNumWinParameters + 1;

const uint16_t checkWinSize = (kWorkWindowSize + checkpointWindowSize) / checkpointWindowSize;

constexpr uint16_t numOfCheckWinParameters = CheckData::getNumOfParams();
const uint16_t numOfCheckWinObjs = checkWinSize * numOfCheckWinParameters + 1;

enum WinMetadataParameterIds {
  BEGINNING_OF_SEQ_NUM_WINDOW = CONST_METADATA_PARAMETERS_NUM,
  BEGINNING_OF_CHECK_WINDOW = BEGINNING_OF_SEQ_NUM_WINDOW + numOfSeqNumWinObjs,
  WIN_PARAMETERS_NUM = BEGINNING_OF_CHECK_WINDOW + numOfCheckWinObjs
};

// LAST_EXIT_FROM_VIEW_DESC contains up to kWorkWindowSize descriptor objects
// (one per PrevViewInfo) plus one - for simple descriptor parameters.
const uint16_t numOfLastExitFromViewDescObjs = kWorkWindowSize + 1;

// LAST_NEW_VIEW_DESC contains numOfReplicas_ (2 * f + 2 * c + 1) descriptor
// objects plus one - for simple descriptor parameters.
enum DescMetadataParameterIds {
  LAST_EXIT_FROM_VIEW_DESC = WIN_PARAMETERS_NUM,
  LAST_EXEC_DESC = LAST_EXIT_FROM_VIEW_DESC + numOfLastExitFromViewDescObjs,
  LAST_NEW_VIEW_DESC
};

class PersistentStorageImp : public PersistentStorage {
 public:
  PersistentStorageImp(uint16_t fVal, uint16_t cVal);
  virtual ~PersistentStorageImp();

  uint8_t beginWriteTran() override;
  uint8_t endWriteTran() override;
  bool isInWriteTran() const override;

  // Setters
  void setReplicaConfig(const ReplicaConfig &config) override;
  void setFetchingState(const bool state) override;
  void setLastExecutedSeqNum(const SeqNum seqNum) override;
  void setPrimaryLastUsedSeqNum(const SeqNum seqNum) override;
  void setStrictLowerBoundOfSeqNums(const SeqNum seqNum) override;
  void setLastViewThatTransferredSeqNumbersFullyExecuted(const ViewNum view) override;

  void setDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &prevViewDesc) override;
  void setDescriptorOfLastNewView(const DescriptorOfLastNewView &prevViewDesc) override;
  void setDescriptorOfLastExecution(const DescriptorOfLastExecution &prevViewDesc) override;

  void setLastStableSeqNum(const SeqNum seqNum) override;
  void setPrePrepareMsgInSeqNumWindow(const SeqNum seqNum, const PrePrepareMsg *const msg) override;
  void setSlowStartedInSeqNumWindow(const SeqNum seqNum, const bool slowStarted) override;
  void setFullCommitProofMsgInSeqNumWindow(const SeqNum seqNum, const FullCommitProofMsg *const msg) override;
  void setForceCompletedInSeqNumWindow(const SeqNum seqNum, const bool forceCompleted) override;
  void setPrepareFullMsgInSeqNumWindow(const SeqNum seqNum, const PrepareFullMsg *const msg) override;
  void setCommitFullMsgInSeqNumWindow(const SeqNum seqNum, const CommitFullMsg *const msg) override;
  void setCheckpointMsgInCheckWindow(const SeqNum seqNum, const CheckpointMsg *const msg) override;
  void setCompletedMarkInCheckWindow(const SeqNum seqNum, const bool completed) override;
  void clearSeqNumWindow() override;

  // Getters
  std::string getStoredVersion();
  std::string getCurrentVersion() const { return version_; }
  ReplicaConfig getReplicaConfig() override;
  bool getFetchingState() override;
  SeqNum getLastExecutedSeqNum() override;
  SeqNum getPrimaryLastUsedSeqNum() override;
  SeqNum getStrictLowerBoundOfSeqNums() override;
  ViewNum getLastViewThatTransferredSeqNumbersFullyExecuted() override;
  SeqNum getLastStableSeqNum() override;

  DescriptorOfLastExitFromView getAndAllocateDescriptorOfLastExitFromView() override;
  DescriptorOfLastNewView getAndAllocateDescriptorOfLastNewView() override;
  DescriptorOfLastExecution getDescriptorOfLastExecution() override;

  PrePrepareMsg *getAndAllocatePrePrepareMsgInSeqNumWindow(const SeqNum seqNum) override;
  bool getSlowStartedInSeqNumWindow(const SeqNum seqNum) override;
  FullCommitProofMsg *getAndAllocateFullCommitProofMsgInSeqNumWindow(const SeqNum seqNum) override;
  bool getForceCompletedInSeqNumWindow(const SeqNum seqNum) override;
  PrepareFullMsg *getAndAllocatePrepareFullMsgInSeqNumWindow(const SeqNum seqNum) override;
  CommitFullMsg *getAndAllocateCommitFullMsgInSeqNumWindow(const SeqNum seqNum) override;
  CheckpointMsg *getAndAllocateCheckpointMsgInCheckWindow(const SeqNum seqNum) override;
  bool getCompletedMarkInCheckWindow(const SeqNum seqNum) override;

  SharedPtrSeqNumWindow getSeqNumWindow();
  SharedPtrCheckWindow getCheckWindow();

  bool hasReplicaConfig() const override;

  bool hasDescriptorOfLastExitFromView() override;
  bool hasDescriptorOfLastNewView() override;
  bool hasDescriptorOfLastExecution() override;

  // Returns 'true' in case storage is empty
  bool init(MetadataStorage *&metadataStorage);

 protected:
  bool setIsAllowed() const;
  bool getIsAllowed() const;
  bool nonExecSetIsAllowed() const;
  SeqNum getSeqNum(ConstMetadataParameterIds id, uint32_t size);

 private:
  void retrieveWindowsMetadata();
  void initMetadataStorage(MetadataStorage *&metadataStorage);
  void setDefaultsInMetadataStorage();
  void verifySetDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &desc) const;
  void verifyPrevViewInfo(const DescriptorOfLastExitFromView &desc) const;
  void verifySetDescriptorOfLastNewView(const DescriptorOfLastNewView &desc) const;
  void verifyLastNewViewMsgs(const DescriptorOfLastNewView &desc) const;
  void verifyDescriptorOfLastExecution(const DescriptorOfLastExecution &desc) const;

  void saveDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &newDesc);
  void setDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &desc, bool init);
  void initDescriptorOfLastExitFromView();
  void saveDescriptorOfLastNewView(const DescriptorOfLastNewView &newDesc);
  void setDescriptorOfLastNewView(const DescriptorOfLastNewView &desc, bool init);
  void initDescriptorOfLastNewView();
  void saveDescriptorOfLastExecution(const DescriptorOfLastExecution &newDesc);
  void setDescriptorOfLastExecution(const DescriptorOfLastExecution &desc, bool init);
  void initDescriptorOfLastExecution();

  void setVersion() const;

  void setMsgInSeqNumWindow(const SeqNum &seqNum, const SeqNum &parameterId,
                            MessageBase *msg, const size_t &msgSize) const;
  void setBooleanInSeqNumWindow(const SeqNum &seqNum, const SeqNum &parameterId, const bool &boolean) const;
  void setSeqNumDataElement(const SeqNum &index, const SharedPtrSeqNumWindow &seqNumWindow) const;
  void serializeAndSaveSeqNumWindow(const SharedPtrSeqNumWindow &seqNumWindow);
  void setSeqNumDataElement(const SeqNum &index, const SeqNumData &elem) const;

  void serializeAndSaveCheckWindow(const SharedPtrCheckWindow &checkWindow);
  void setCheckDataElement(const SeqNum &index, const CheckData &elem) const;
  void setCheckDataElement(const SeqNum &index, const SharedPtrCheckWindow &checkWindow) const;

  SeqNum readBeginningOfActiveWindow(const uint32_t &index) const;
  MessageBase *readMsgFromDisk(const SeqNum &index, const SeqNum &parameterId, const size_t &msgSize) const;
  PrePrepareMsg *readPrePrepareMsgFromDisk(const SeqNum &seqNum) const;
  FullCommitProofMsg *readFullCommitProofMsgFromDisk(const SeqNum &seqNum) const;
  PrepareFullMsg *readPrepareFullMsgFromDisk(const SeqNum &seqNum) const;
  CommitFullMsg *readCommitFullMsgFromDisk(const SeqNum &seqNum) const;
  bool readBooleanFromDisk(const SeqNum &seqNum, const SeqNum &parameterId) const;
  void readSeqNumDataElementFromDisk(const SeqNum &index, const SharedPtrSeqNumWindow &seqNumWindow);
  const SeqNum convertSeqNumWindowIndex(const SeqNum &index) const;

  void readCheckDataElementFromDisk(const SeqNum &index, const SharedPtrCheckWindow &checkWindow);
  const SeqNum convertCheckWindowIndex(const SeqNum &index) const;
  CheckpointMsg *readCheckpointMsgFromDisk(const SeqNum &seqNum) const;
  bool readCompletedMarkFromDisk(const SeqNum &index) const;

  void writeBeginningOfActiveWindow(const uint32_t &index, const SeqNum &beginning) const;
  void setFetchingStateInternal(const bool &state);
  void setLastExecutedSeqNumInternal(const SeqNum &seqNum);
  void setPrimaryLastUsedSeqNumInternal(const SeqNum &seqNum);
  void setStrictLowerBoundOfSeqNumsInternal(const SeqNum &seqNum);
  void setLastViewTransferredSeqNumbersInternal(const ViewNum &view);
  void setDefaultWindowsValues();

 private:
  MetadataStorage *metadataStorage_ = nullptr;
  ReplicaConfigSerializer *configSerializer_ = nullptr;

  const uint32_t maxVersionSize_ = 80;

  const uint16_t fVal_;
  const uint16_t cVal_;

  uint8_t numOfNestedTransactions_ = 0;
  const uint32_t numOfReplicas_;
  const SeqNum seqNumWindowFirst_ = 1;
  const SeqNum seqNumWindowLast_ = seqWinSize;
  const SeqNum checkWindowFirst_ = 0;
  const SeqNum checkWindowLast_ = checkWinSize;
  SeqNum checkWindowBeginning_ = 0;
  SeqNum seqNumWindowBeginning_ = 0;

  bool hasDescriptorOfLastExitFromView_ = false;
  bool hasDescriptorOfLastNewView_ = false;
  bool hasDescriptorOfLastExecution_ = false;

  // Parameters to be saved persistently
  std::string version_;
  bool fetchingState_ = false;
  SeqNum lastExecutedSeqNum_ = 0;
  SeqNum primaryLastUsedSeqNum_ = 0;
  SeqNum strictLowerBoundOfSeqNums_ = 0;
  ViewNum lastViewTransferredSeqNum_ = 0;
  SeqNum lastStableSeqNum_ = 0;
};

}  // namespace impl
}  // namespace bftEngine
