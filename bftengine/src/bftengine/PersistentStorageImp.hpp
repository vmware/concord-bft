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

// Make a reservation for future params
const uint16_t reservedParamsNum = 50;

enum ConstMetadataParameterIds : uint32_t {
  INITIALIZED_FLAG = 1, // A flag saying whether DB is initialized or not; handled by storage class itself.
  FIRST_METADATA_PARAMETER,
  VERSION_PARAMETER = FIRST_METADATA_PARAMETER,
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
  BEGINNING_OF_SEQ_NUM_WINDOW = CONST_METADATA_PARAMETERS_NUM + reservedParamsNum,
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

typedef unique_ptr<MetadataStorage::ObjectDesc> ObjectDescUniquePtr;

class PersistentStorageImp : public PersistentStorage {
 public:
  PersistentStorageImp(uint16_t fVal, uint16_t cVal);
  ~PersistentStorageImp() override = default;

  uint8_t beginWriteTran() override;
  uint8_t endWriteTran() override;
  bool isInWriteTran() const override;

  // Setters
  void setReplicaConfig(const ReplicaConfig &config) override;
  void setFetchingState(bool state) override;
  void setLastExecutedSeqNum(SeqNum seqNum) override;
  void setPrimaryLastUsedSeqNum(SeqNum seqNum) override;
  void setStrictLowerBoundOfSeqNums(SeqNum seqNum) override;
  void setLastViewThatTransferredSeqNumbersFullyExecuted(ViewNum view) override;

  void setDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &prevViewDesc) override;
  void setDescriptorOfLastNewView(const DescriptorOfLastNewView &prevViewDesc) override;
  void setDescriptorOfLastExecution(const DescriptorOfLastExecution &prevViewDesc) override;

  void setLastStableSeqNum(SeqNum seqNum) override;
  void setPrePrepareMsgInSeqNumWindow(SeqNum seqNum, PrePrepareMsg *msg) override;
  void setSlowStartedInSeqNumWindow(SeqNum seqNum, bool slowStarted) override;
  void setFullCommitProofMsgInSeqNumWindow(SeqNum seqNum, FullCommitProofMsg *msg) override;
  void setForceCompletedInSeqNumWindow(SeqNum seqNum, bool forceCompleted) override;
  void setPrepareFullMsgInSeqNumWindow(SeqNum seqNum, PrepareFullMsg *msg) override;
  void setCommitFullMsgInSeqNumWindow(SeqNum seqNum, CommitFullMsg *msg) override;
  void setCheckpointMsgInCheckWindow(SeqNum seqNum, CheckpointMsg *msg) override;
  void setCompletedMarkInCheckWindow(SeqNum seqNum, bool completed) override;
  void clearSeqNumWindow() override;
  ObjectDescUniquePtr getDefaultMetadataObjectDescriptors(uint16_t &numOfObjects) const;

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

  PrePrepareMsg *getAndAllocatePrePrepareMsgInSeqNumWindow(SeqNum seqNum) override;
  bool getSlowStartedInSeqNumWindow(SeqNum seqNum) override;
  FullCommitProofMsg *getAndAllocateFullCommitProofMsgInSeqNumWindow(SeqNum seqNum) override;
  bool getForceCompletedInSeqNumWindow(SeqNum seqNum) override;
  PrepareFullMsg *getAndAllocatePrepareFullMsgInSeqNumWindow(SeqNum seqNum) override;
  CommitFullMsg *getAndAllocateCommitFullMsgInSeqNumWindow(SeqNum seqNum) override;
  CheckpointMsg *getAndAllocateCheckpointMsgInCheckWindow(SeqNum seqNum) override;
  bool getCompletedMarkInCheckWindow(SeqNum seqNum) override;

  SharedPtrSeqNumWindow getSeqNumWindow();
  SharedPtrCheckWindow getCheckWindow();

  bool hasReplicaConfig() const override;

  bool hasDescriptorOfLastExitFromView() override;
  bool hasDescriptorOfLastNewView() override;
  bool hasDescriptorOfLastExecution() override;

  // Returns 'true' in case storage is empty
  bool init(std::unique_ptr<MetadataStorage> metadataStorage);

 protected:
  bool setIsAllowed() const;
  bool getIsAllowed() const;
  bool nonExecSetIsAllowed();
  SeqNum getSeqNum(ConstMetadataParameterIds id, uint32_t size);

 private:
  void retrieveWindowsMetadata();
  void setDefaultsInMetadataStorage();
  void verifySetDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &desc);
  void verifyPrevViewInfo(const DescriptorOfLastExitFromView &desc) const;
  void verifySetDescriptorOfLastNewView(const DescriptorOfLastNewView &desc);
  void verifyLastNewViewMsgs(const DescriptorOfLastNewView &desc) const;
  void verifyDescriptorOfLastExecution(const DescriptorOfLastExecution &desc);

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

  void setMsgInSeqNumWindow(SeqNum seqNum, SeqNum parameterId, MessageBase *msg, size_t msgSize) const;
  void setOneByteInSeqNumWindow(SeqNum seqNum, SeqNum parameterId, uint8_t oneByte) const;
  void saveDefaultsInSeqNumWindow();
  void setSeqNumDataElement(SeqNum index, const SeqNumData &elem) const;

  void saveDefaultsInCheckWindow();
  void setCheckDataElement(SeqNum index, const CheckData &elem) const;

  SeqNum readBeginningOfActiveWindow(uint32_t index) const;
  MessageBase *readMsgFromDisk(SeqNum seqNum, SeqNum parameterId, size_t msgSize) const;
  PrePrepareMsg *readPrePrepareMsgFromDisk(SeqNum seqNum) const;
  FullCommitProofMsg *readFullCommitProofMsgFromDisk(SeqNum seqNum) const;
  PrepareFullMsg *readPrepareFullMsgFromDisk(SeqNum seqNum) const;
  CommitFullMsg *readCommitFullMsgFromDisk(SeqNum seqNum) const;
  uint8_t readOneByteFromDisk(SeqNum index, SeqNum parameterId) const;
  void readSeqNumDataElementFromDisk(SeqNum index, const SharedPtrSeqNumWindow &seqNumWindow);
  const SeqNum convertSeqNumWindowIndex(SeqNum seqNum) const;

  void readCheckDataElementFromDisk(SeqNum index, const SharedPtrCheckWindow &checkWindow);
  const SeqNum convertCheckWindowIndex(SeqNum index) const;
  CheckpointMsg *readCheckpointMsgFromDisk(SeqNum seqNum) const;
  uint8_t readCompletedMarkFromDisk(SeqNum index) const;

  void writeBeginningOfActiveWindow(uint32_t index, SeqNum beginning) const;
  void setFetchingStateInternal(uint8_t state);
  void setLastExecutedSeqNumInternal(SeqNum seqNum);
  void setPrimaryLastUsedSeqNumInternal(SeqNum seqNum);
  void setStrictLowerBoundOfSeqNumsInternal(SeqNum seqNum);
  void setLastViewTransferredSeqNumbersInternal(ViewNum view);
  void setDefaultWindowsValues();

 private:
  std::unique_ptr<MetadataStorage> metadataStorage_;
  std::shared_ptr<ReplicaConfigSerializer> configSerializer_;
  const ReplicaConfigSerializer defaultReplicaConfig_;

  const uint32_t maxVersionSize_ = 80;

  const uint16_t fVal_;
  const uint16_t cVal_;

  uint8_t numOfNestedTransactions_ = 0;
  const uint32_t numOfReplicas_;
  const SeqNum seqNumWindowFirst_ = 1;
  const SeqNum checkWindowFirst_ = 0;
  SeqNum checkWindowBeginning_ = 0;
  SeqNum seqNumWindowBeginning_ = 0;

  bool hasDescriptorOfLastExitFromView_ = false;
  bool hasDescriptorOfLastNewView_ = false;
  bool hasDescriptorOfLastExecution_ = false;

  const DescriptorOfLastExecution emptyDescriptorOfLastExecution_ = DescriptorOfLastExecution{0, Bitmap()};
  DescriptorOfLastExecution descriptorOfLastExecution_ = emptyDescriptorOfLastExecution_;

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
