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

#include "PersistentStorageImp.hpp"
#include <sstream>

namespace bftEngine {
namespace impl {

PersistentStorageImp::PersistentStorageImp(uint16_t fVal, uint16_t cVal)
    : fVal_(fVal), cVal_(cVal), seqNumWindow_{1, nullptr},
      checkWindow_{0, nullptr} {}

void PersistentStorageImp::init(MetadataStorage *&metadataStorage) {
  metadataStorage_ = metadataStorage;

  // Init MetadataStorage
  MetadataStorage::ObjectDesc metadataObjectsArray[METADATA_PARAMETERS_NUM];
  for (int i = 0; i < METADATA_PARAMETERS_NUM; ++i)
    metadataObjectsArray[i].id = i;

  metadataObjectsArray[LAST_STABLE_SEQ_NUM].maxSize = sizeof(lastStableSeqNum_);
  metadataObjectsArray[LAST_EXEC_SEQ_NUM].maxSize = sizeof(lastExecutedSeqNum_);
  metadataObjectsArray[PRIMARY_LAST_USED_SEQ_NUM].maxSize =
      sizeof(primaryLastUsedSeqNum_);
  metadataObjectsArray[LOWER_BOUND_OF_SEQ_NUM].maxSize =
      sizeof(strictLowerBoundOfSeqNums_);
  metadataObjectsArray[LAST_VIEW_TRANSFERRED_SEQ_NUM].maxSize =
      sizeof(lastViewTransferredSeqNumbersFullyExecuted_);
  metadataObjectsArray[FETCHING_STATE].maxSize = sizeof(fetchingState_);

  metadataObjectsArray[REPLICA_CONFIG].maxSize =
      ReplicaConfigSerializer::maxSize();

  metadataObjectsArray[LAST_EXIT_FROM_VIEW_DESC].maxSize =
      DescriptorOfLastExitFromView::maxSize();
  metadataObjectsArray[LAST_NEW_VIEW_DESC].maxSize =
      DescriptorOfLastNewView::maxSize(fVal_, cVal_);
  metadataObjectsArray[LAST_EXEC_DESC].maxSize =
      DescriptorOfLastExecution::maxSize();

  metadataObjectsArray[SEQ_NUM_WINDOW].maxSize = SeqNumWindow::maxSize();
  metadataObjectsArray[CHECK_WINDOW].maxSize = CheckWindow::maxSize();

  metadataStorage_->initMaxSizeOfObjects(metadataObjectsArray,
                                         METADATA_PARAMETERS_NUM);
}

uint8_t PersistentStorageImp::beginWriteTran() {
  if (numOfNestedTransactions_ == 0) {
    metadataStorage_->beginAtomicWriteOnlyTransaction();
  }
  return ++numOfNestedTransactions_;
}

uint8_t PersistentStorageImp::endWriteTran() {
  Assert(numOfNestedTransactions_ != 0);
  if (--numOfNestedTransactions_ == 0) {
    metadataStorage_->commitAtomicWriteOnlyTransaction();
  }
  return numOfNestedTransactions_;
}

bool PersistentStorageImp::isInWriteTran() const {
  return (numOfNestedTransactions_ != 0);
}

/***** Setters *****/

void PersistentStorageImp::setReplicaConfig(const ReplicaConfig &config) {
  Assert(isInWriteTran());
  Assert(configSerializer_ == nullptr);
  configSerializer_ = new ReplicaConfigSerializer(config);
  UniquePtrToChar outBuf;
  int64_t outBufSize = 0;
  configSerializer_->serialize(outBuf, outBufSize);
  metadataStorage_->writeInTransaction(REPLICA_CONFIG, (char *) outBuf.get(),
                                       outBufSize);
}

void PersistentStorageImp::setFetchingState(const bool &state) {
  Assert(nonExecSetIsAllowed());
  Assert(!state);
  fetchingState_ = state;
  metadataStorage_->writeInTransaction(FETCHING_STATE, (char *) &state,
                                       sizeof(state));
}

void PersistentStorageImp::setLastExecutedSeqNum(const SeqNum &seqNum) {
  Assert(setIsAllowed());
  Assert(lastExecutedSeqNum_ <= seqNum);
  lastExecutedSeqNum_ = seqNum;
  metadataStorage_->writeInTransaction(LAST_EXEC_SEQ_NUM, (char *) &seqNum,
                                       sizeof(seqNum));
}

void PersistentStorageImp::setPrimaryLastUsedSeqNum(const SeqNum &seqNum) {
  Assert(nonExecSetIsAllowed());
  primaryLastUsedSeqNum_ = seqNum;
  metadataStorage_->writeInTransaction(PRIMARY_LAST_USED_SEQ_NUM,
                                       (char *) &seqNum, sizeof(seqNum));
}

void PersistentStorageImp::setStrictLowerBoundOfSeqNums(const SeqNum &seqNum) {
  Assert(nonExecSetIsAllowed());
  strictLowerBoundOfSeqNums_ = seqNum;
  metadataStorage_->writeInTransaction(LOWER_BOUND_OF_SEQ_NUM,
                                       (char *) &seqNum, sizeof(seqNum));
}

void PersistentStorageImp::setLastViewThatTransferredSeqNumbersFullyExecuted(
    const ViewNum &view) {
  Assert(nonExecSetIsAllowed());
  Assert(lastViewTransferredSeqNumbersFullyExecuted_ <= view);
  lastViewTransferredSeqNumbersFullyExecuted_ = view;
  metadataStorage_->writeInTransaction(LAST_VIEW_TRANSFERRED_SEQ_NUM,
                                       (char *) &view, sizeof(view));
}

/***** Descriptors handling *****/

void PersistentStorageImp::setDescriptorOfLastExitFromView(
    const DescriptorOfLastExitFromView &desc) {
  verifySetDescriptorOfLastExitFromView(desc);
  PrevViewInfoElements clonedElements(desc.elements.size());
  int i = 0;
  for (auto elem : desc.elements) {
    verifyPrevViewInfo(desc, elem);
    auto *clonedPrePrepareMsg =
        (PrePrepareMsg *) elem.prePrepare->cloneObjAndMsg();
    Assert(clonedPrePrepareMsg->type() == MsgCode::PrePrepare);
    PrepareFullMsg *clonedPrepareFull = nullptr;
    if (elem.prepareFull != nullptr) {
      clonedPrepareFull = (PrepareFullMsg *) elem.prepareFull->cloneObjAndMsg();
      Assert(clonedPrepareFull->type() == MsgCode::PrepareFull);
    }
    clonedElements[i].prePrepare = clonedPrePrepareMsg;
    clonedElements[i].hasAllRequests = elem.hasAllRequests;
    clonedElements[i].prepareFull = clonedPrepareFull;
    ++i;
  }
  // Delete messages from previous descriptor
  for (auto elem : descriptorOfLastExitFromView_.elements) {
    delete elem.prePrepare;
    delete elem.prepareFull;
  }
  descriptorOfLastExitFromView_ =
      DescriptorOfLastExitFromView(desc.view, desc.lastStable,
                                   desc.lastExecuted, clonedElements);
}

void PersistentStorageImp::setDescriptorOfLastNewView(
    const DescriptorOfLastNewView &desc) {
  verifySetDescriptorOfLastNewView(desc);
  const size_t numOfVCMsgs = 2 * configSerializer_->getConfig()->fVal +
      2 * configSerializer_->getConfig()->cVal + 1;

  Assert(desc.viewChangeMsgs.size() == numOfVCMsgs);
  ViewChangeMsgsVector clonedViewChangeMsgs(numOfVCMsgs);
  for (size_t i = 0; i < numOfVCMsgs; i++) {
    const ViewChangeMsg *vc = desc.viewChangeMsgs[i];
    Assert(vc->newView() == desc.view);

    Digest digestOfVCMsg;
    vc->getMsgDigest(digestOfVCMsg);
    Assert(desc.newViewMsg->includesViewChangeFromReplica(
        vc->idOfGeneratedReplica(), digestOfVCMsg));

    auto *clonedVC = (ViewChangeMsg *) vc->cloneObjAndMsg();
    Assert(clonedVC->type() == MsgCode::ViewChange);
    clonedViewChangeMsgs[i] = clonedVC;
  }

  auto *clonedNewViewMsg = (NewViewMsg *) desc.newViewMsg->cloneObjAndMsg();
  Assert(clonedNewViewMsg->type() == MsgCode::NewView);

  if (!descriptorOfLastNewView_.isEmpty()) {
    // Delete messages from previous descriptor
    delete descriptorOfLastNewView_.newViewMsg;
    Assert(descriptorOfLastNewView_.viewChangeMsgs.size() == numOfVCMsgs);
    for (size_t i = 0; i < numOfVCMsgs; i++) {
      delete descriptorOfLastNewView_.viewChangeMsgs[i];
    }
  }
  descriptorOfLastNewView_ =
      DescriptorOfLastNewView{desc.view,
                              clonedNewViewMsg,
                              clonedViewChangeMsgs,
                              desc.maxSeqNumTransferredFromPrevViews};
}

void PersistentStorageImp::setDescriptorOfLastExecution(
    const DescriptorOfLastExecution &desc) {
  Assert(setIsAllowed());
  Assert(descriptorOfLastExecution_.isEmpty() ||
      descriptorOfLastExecution_.executedSeqNum < desc.executedSeqNum);
  Assert(lastExecutedSeqNum_ + 1 == desc.executedSeqNum);
  Assert(desc.validRequests.numOfBits() >= 1);
  Assert(desc.validRequests.numOfBits() <= maxNumOfRequestsInBatch);

  descriptorOfLastExecution_ =
      DescriptorOfLastExecution{desc.executedSeqNum, desc.validRequests};
}

/***** Windows handling *****/

void PersistentStorageImp::setLastStableSeqNum(const SeqNum &seqNum) {
  Assert(seqNum >= lastStableSeqNum_);
  lastStableSeqNum_ = seqNum;
  metadataStorage_->writeInTransaction(LAST_STABLE_SEQ_NUM,
                                       (char *) &seqNum, sizeof(seqNum));
  seqNumWindow_.advanceActiveWindow(lastStableSeqNum_ + 1);
  checkWindow_.advanceActiveWindow(lastStableSeqNum_);
}

void PersistentStorageImp::clearSeqNumWindow() {
  SeqNum s = seqNumWindow_.currentActiveWindow().first;
  Assert(s == lastStableSeqNum_ + 1);
  seqNumWindow_.resetAll(s);
}

void PersistentStorageImp::setPrePrepareMsgInSeqNumWindow(
    const SeqNum &seqNum, const PrePrepareMsg *const &msg) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  Assert(seqNumData.prePrepareMsg == nullptr);
  seqNumData.prePrepareMsg = (PrePrepareMsg *) msg->cloneObjAndMsg();
}

void PersistentStorageImp::setSlowStartedInSeqNumWindow(
    const SeqNum &seqNum, const bool &slowStarted) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  seqNumData.slowStarted = slowStarted;
}

void PersistentStorageImp::setFullCommitProofMsgInSeqNumWindow(
    const SeqNum &seqNum, const FullCommitProofMsg *const &msg) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  Assert(seqNumData.fullCommitProofMsg == nullptr);
  seqNumData.fullCommitProofMsg = (FullCommitProofMsg *) msg->cloneObjAndMsg();
}

void PersistentStorageImp::setForceCompletedInSeqNumWindow(
    const SeqNum &seqNum, const bool &forceCompleted) {
  Assert(forceCompleted);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  seqNumData.forceCompleted = forceCompleted;
}

void PersistentStorageImp::setPrepareFullMsgInSeqNumWindow(
    const SeqNum &seqNum, const PrepareFullMsg *const &msg) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  Assert(seqNumData.prepareFullMsg == nullptr);
  seqNumData.prepareFullMsg = (PrepareFullMsg *) msg->cloneObjAndMsg();
}

void PersistentStorageImp::setCommitFullMsgInSeqNumWindow(
    const SeqNum &seqNum, const CommitFullMsg *const &msg) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  Assert(seqNumData.commitFullMsg == nullptr);
  seqNumData.commitFullMsg = (CommitFullMsg *) msg->cloneObjAndMsg();
}

void PersistentStorageImp::setCheckpointMsgInCheckWindow(
    const SeqNum &seqNum, const CheckpointMsg *const &msg) {
  Assert(checkWindow_.insideActiveWindow(seqNum));
  CheckData &checkData = checkWindow_.get(seqNum);
  delete checkData.checkpointMsg;
  checkData.checkpointMsg = (CheckpointMsg *) msg->cloneObjAndMsg();
}

void PersistentStorageImp::setCompletedMarkInCheckWindow(
    const SeqNum &seqNum, const bool &completed) {
  Assert(completed);
  Assert(checkWindow_.insideActiveWindow(seqNum));
  CheckData &checkData = checkWindow_.get(seqNum);
  checkData.completedMark = completed;
}

/***** Getters *****/

ReplicaConfig PersistentStorageImp::getReplicaConfig() {
  Assert(getIsAllowed());
  uint32_t outActualObjectSize = 0;
  UniquePtrToChar outBuf(new char[ReplicaConfigSerializer::maxSize()]);
  metadataStorage_->read(REPLICA_CONFIG, ReplicaConfigSerializer::maxSize(),
                         outBuf.get(), outActualObjectSize);
  UniquePtrToClass replicaConfigPtr =
      Serializable::deserialize(outBuf, outActualObjectSize);
  return *dynamic_cast<ReplicaConfig *>(replicaConfigPtr.get());
}

bool PersistentStorageImp::getFetchingState() {
  Assert(getIsAllowed());
  uint32_t outActualObjectSize = 0;
  metadataStorage_->read(FETCHING_STATE, sizeof(fetchingState_),
                         (char *) &fetchingState_, outActualObjectSize);
  Assert(outActualObjectSize == sizeof(fetchingState_));
  return fetchingState_;
}

SeqNum PersistentStorageImp::getSeqNum(MetadataParameterIds id, uint32_t size) {
  uint32_t outActualObjectSize = 0;
  SeqNum seqNum = 0;
  metadataStorage_->read(id, size, (char *) &seqNum, outActualObjectSize);
  Assert(outActualObjectSize == size);
  return seqNum;
}

SeqNum PersistentStorageImp::getLastExecutedSeqNum() {
  Assert(getIsAllowed());
  return getSeqNum(LAST_EXEC_SEQ_NUM, sizeof(lastExecutedSeqNum_));
}

SeqNum PersistentStorageImp::getPrimaryLastUsedSeqNum() {
  Assert(getIsAllowed());
  return getSeqNum(PRIMARY_LAST_USED_SEQ_NUM, sizeof(primaryLastUsedSeqNum_));
}

SeqNum PersistentStorageImp::getStrictLowerBoundOfSeqNums() {
  Assert(getIsAllowed());
  return getSeqNum(LOWER_BOUND_OF_SEQ_NUM, sizeof(strictLowerBoundOfSeqNums_));
}

SeqNum PersistentStorageImp::getLastStableSeqNum() {
  Assert(getIsAllowed());
  return getSeqNum(LAST_STABLE_SEQ_NUM, sizeof(lastStableSeqNum_));
}

ViewNum
PersistentStorageImp::getLastViewThatTransferredSeqNumbersFullyExecuted() {
  Assert(getIsAllowed());
  return getSeqNum(LAST_VIEW_TRANSFERRED_SEQ_NUM,
                   sizeof(lastViewTransferredSeqNumbersFullyExecuted_));
}

DescriptorOfLastExitFromView
PersistentStorageImp::getAndAllocateDescriptorOfLastExitFromView() {
  Assert(getIsAllowed());
  Assert(!descriptorOfLastExitFromView_.isEmpty());

  DescriptorOfLastExitFromView &desc = descriptorOfLastExitFromView_;
  PrevViewInfoElements elements(desc.elements.size());
  for (size_t i = 0; i < elements.size(); i++) {
    const ViewsManager::PrevViewInfo &e = desc.elements[i];
    elements[i].prePrepare = (PrePrepareMsg *) e.prePrepare->cloneObjAndMsg();
    elements[i].hasAllRequests = e.hasAllRequests;
    if (e.prepareFull != nullptr)
      elements[i].prepareFull =
          (PrepareFullMsg *) e.prepareFull->cloneObjAndMsg();
    else
      elements[i].prepareFull = nullptr;
  }
  DescriptorOfLastExitFromView retVal{
      desc.view, desc.lastStable, desc.lastExecuted, elements};
  return retVal;
}

DescriptorOfLastNewView
PersistentStorageImp::getAndAllocateDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  Assert(!descriptorOfLastNewView_.isEmpty());

  DescriptorOfLastNewView &desc = descriptorOfLastNewView_;
  auto *newViewMsg = (NewViewMsg *) desc.newViewMsg->cloneObjAndMsg();
  ViewChangeMsgsVector viewChangeMsgs(desc.viewChangeMsgs.size());
  for (size_t i = 0; i < viewChangeMsgs.size(); i++) {
    viewChangeMsgs[i] =
        (ViewChangeMsg *) desc.viewChangeMsgs[i]->cloneObjAndMsg();
  }
  DescriptorOfLastNewView retVal{
      desc.view, newViewMsg, viewChangeMsgs,
      desc.maxSeqNumTransferredFromPrevViews};
  return retVal;
}

bool PersistentStorageImp::hasDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  return (!descriptorOfLastNewView_.isEmpty());
}

bool PersistentStorageImp::hasDescriptorOfLastExitFromView() {
  Assert(getIsAllowed());
  return (!descriptorOfLastExitFromView_.isEmpty());
}

bool PersistentStorageImp::hasDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  return (!descriptorOfLastExecution_.isEmpty());
}

DescriptorOfLastExecution PersistentStorageImp::getDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  Assert(!descriptorOfLastExecution_.isEmpty());

  DescriptorOfLastExecution &desc = descriptorOfLastExecution_;
  return DescriptorOfLastExecution{desc.executedSeqNum, desc.validRequests};
}

PrePrepareMsg *PersistentStorageImp::getAndAllocatePrePrepareMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  auto *msg = (PrePrepareMsg *) seqNumWindow_.get(seqNum)
      .prePrepareMsg->cloneObjAndMsg();
  Assert(msg->type() == MsgCode::PrePrepare);
  return msg;
}

bool PersistentStorageImp::getSlowStartedInSeqNumWindow(const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  return seqNumWindow_.get(seqNum).slowStarted;
}

FullCommitProofMsg *
PersistentStorageImp::getAndAllocateFullCommitProofMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  auto *msg = (FullCommitProofMsg *) seqNumWindow_.get(seqNum)
      .fullCommitProofMsg->cloneObjAndMsg();
  Assert(msg->type() == MsgCode::FullCommitProof);
  return msg;
}

bool PersistentStorageImp::getForceCompletedInSeqNumWindow(const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  return seqNumWindow_.get(seqNum).forceCompleted;
}

PrepareFullMsg *
PersistentStorageImp::getAndAllocatePrepareFullMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  auto *msg = (PrepareFullMsg *) seqNumWindow_.get(seqNum)
      .prepareFullMsg->cloneObjAndMsg();
  Assert(msg->type() == MsgCode::PrepareFull);
  return msg;
}

CommitFullMsg *PersistentStorageImp::getAndAllocateCommitFullMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  auto *msg = (CommitFullMsg *) seqNumWindow_.get(seqNum)
      .commitFullMsg->cloneObjAndMsg();
  Assert(msg->type() == MsgCode::CommitFull);
  return msg;
}

CheckpointMsg *PersistentStorageImp::getAndAllocateCheckpointMsgInCheckWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow_.currentActiveWindow().first);
  Assert(checkWindow_.insideActiveWindow(seqNum));
  auto *msg = (CheckpointMsg *) checkWindow_.get(seqNum)
      .checkpointMsg->cloneObjAndMsg();
  Assert(msg->type() == MsgCode::Checkpoint);
  return msg;
}

bool PersistentStorageImp::getCompletedMarkInCheckWindow(const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow_.currentActiveWindow().first);
  Assert(checkWindow_.insideActiveWindow(seqNum));
  return checkWindow_.get(seqNum).completedMark;
}

void PersistentStorageImp::verifySetDescriptorOfLastExitFromView(
    const DescriptorOfLastExitFromView &desc) const {
  Assert(nonExecSetIsAllowed());
  Assert(desc.view >= 0);
  // Here we assume that the first view is always 0
  // (even if we load the initial state from the disk).
  Assert(!descriptorOfLastExitFromView_.isEmpty() || desc.view == 0);
  Assert(descriptorOfLastExitFromView_.isEmpty() ||
      desc.view > descriptorOfLastExitFromView_.view);
  Assert(descriptorOfLastExitFromView_.isEmpty() ||
      desc.view == descriptorOfLastNewView_.view);
  Assert(desc.lastStable >= lastStableSeqNum_);
  Assert(desc.lastExecuted >= lastExecutedSeqNum_);
  Assert(desc.elements.size() <= kWorkWindowSize);
  Assert(!descriptorOfLastExitFromView_.isEmpty() ||
      descriptorOfLastExitFromView_.elements.empty());
}

void PersistentStorageImp::verifyPrevViewInfo(
    const DescriptorOfLastExitFromView &desc,
    const ViewsManager::PrevViewInfo &elem) const {
  Assert(elem.prePrepare->seqNumber() >= lastStableSeqNum_ + 1);
  Assert(elem.prePrepare->seqNumber() <= lastStableSeqNum_ + kWorkWindowSize);
  Assert(elem.prePrepare->viewNumber() == desc.view);
  Assert(elem.prepareFull == nullptr ||
      elem.prepareFull->viewNumber() == desc.view);
  Assert(elem.prepareFull == nullptr ||
      elem.prepareFull->seqNumber() == elem.prePrepare->seqNumber());
}

void PersistentStorageImp::verifySetDescriptorOfLastNewView(
    const DescriptorOfLastNewView &desc) const {
  Assert(nonExecSetIsAllowed());
  Assert(desc.view >= 1);
  Assert(!descriptorOfLastExitFromView_.isEmpty());
  Assert(desc.view > descriptorOfLastExitFromView_.view);
  Assert(desc.newViewMsg->newView() == desc.view);
}

bool PersistentStorageImp::hasReplicaConfig() {
  return (configSerializer_->getConfig() != nullptr);
}

bool PersistentStorageImp::setIsAllowed() const {
  return (isInWriteTran() && (configSerializer_->getConfig() != nullptr));
}

bool PersistentStorageImp::getIsAllowed() const {
  return (!isInWriteTran() && (configSerializer_->getConfig() != nullptr));
}

bool PersistentStorageImp::nonExecSetIsAllowed() const {
  return setIsAllowed() && (descriptorOfLastExecution_.isEmpty() ||
      descriptorOfLastExecution_.executedSeqNum <= lastExecutedSeqNum_);
}

void PersistentStorageImp::WindowFuncs::free(SeqNumData &seqNumData) {
  reset(seqNumData);
}

void PersistentStorageImp::WindowFuncs::reset(SeqNumData &seqNumData) {
  seqNumData.reset();
}

void PersistentStorageImp::WindowFuncs::free(CheckData &checkData) {
  reset(checkData);
}

void PersistentStorageImp::WindowFuncs::reset(CheckData &checkData) {
  checkData.reset();
}

}  // namespace impl
}  // namespace bftEngine
