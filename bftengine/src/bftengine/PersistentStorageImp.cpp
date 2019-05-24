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

namespace bftEngine {
namespace impl {

enum MetadataParameters {
  LAST_STABLE_SEQ_NUM,
  LAST_EXEC_SEQ_NUM,
  PRIMARY_LAST_USED_SEQ_NUM,
  LOWER_BOUND_OF_SEQ_NUM,
  LAST_VIEW_TRANSFERRED_SEQ_NUM,
  FETCHING_STATE,
  REPLICA_CONFIG,
  LAST_EXIT_FROM_VIEW_DESC,
  LAST_NEW_VIEW_DESC,
  LAST_EXEC_DESC,
  SEQ_NUM_WINDOW,
  CHECK_WINDOW,
  METADATA_PARAMETERS_NUM
};

PersistentStorageImp::PersistentStorageImp(uint16_t fVal, uint16_t cVal)
    : fVal_(fVal),
      cVal_(cVal),
      seqNumWindow_{1, nullptr},
      checkWindow_{0, nullptr} {}

void PersistentStorageImp::init(MetadataStorage *&metadataStorage) {
  metadataStorage_ = metadataStorage;
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
      sizeof(lastViewThatTransferredSeqNumbersFullyExecuted_);
  metadataObjectsArray[FETCHING_STATE].maxSize = sizeof(fetchingState_);

  metadataObjectsArray[REPLICA_CONFIG].maxSize = ReplicaConfig::maxSize();

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

void PersistentStorageImp::setReplicaConfig(const ReplicaConfig &config) {
  Assert(config_ == nullptr);
  Assert(isInWriteTran());
  config_ = new ReplicaConfig;
  *config_ = config;
}

void PersistentStorageImp::setFetchingState(const bool &state) {
  Assert(nonExecSetIsAllowed());
  Assert(!state || !fetchingState_);  // f ==> !fetchingState_
  fetchingState_ = state;
}

void PersistentStorageImp::setLastExecutedSeqNum(const SeqNum &seqNum) {
  Assert(setIsAllowed());
  Assert(lastExecutedSeqNum_ <= seqNum);
  lastExecutedSeqNum_ = seqNum;
}

void PersistentStorageImp::setPrimaryLastUsedSeqNum(const SeqNum &seqNum) {
  Assert(nonExecSetIsAllowed());
  primaryLastUsedSeqNum_ = seqNum;
}

void PersistentStorageImp::setStrictLowerBoundOfSeqNums(const SeqNum &seqNum) {
  Assert(nonExecSetIsAllowed());
  strictLowerBoundOfSeqNums_ = seqNum;
}

void PersistentStorageImp::setLastViewThatTransferredSeqNumbersFullyExecuted(
    const ViewNum &view) {
  Assert(nonExecSetIsAllowed());
  Assert(lastViewThatTransferredSeqNumbersFullyExecuted_ <= view);
  lastViewThatTransferredSeqNumbersFullyExecuted_ = view;
}

void PersistentStorageImp::setDescriptorOfLastExitFromView(
    const DescriptorOfLastExitFromView &d) {
  Assert(nonExecSetIsAllowed());
  Assert(d.view >= 0);

  // Here we assume that the first view is always 0
  // (even if we load the initial state from disk)
  // TODO(GG): check this
  Assert(hasDescriptorOfLastNewView_ || d.view == 0);

  Assert(!hasDescriptorOfLastExitFromView_ ||
      d.view > descriptorOfLastExitFromView_.view);
  Assert(!hasDescriptorOfLastNewView_ ||
      d.view == descriptorOfLastNewView_.view);
  Assert(d.lastStable >= lastStableSeqNum_);
  Assert(d.lastExecuted >= lastExecutedSeqNum_);
  Assert(d.elements.size() <= kWorkWindowSize);
  Assert(hasDescriptorOfLastExitFromView_ ||
      descriptorOfLastExitFromView_.elements.size() == 0);

  std::vector<ViewsManager::PrevViewInfo> clonedElements(d.elements.size());
  int i = 0;
  for (auto elem : d.elements) {
    Assert(elem.prePrepare != nullptr);
    Assert(elem.prePrepare->seqNumber() >= lastStableSeqNum_ + 1);
    Assert(elem.prePrepare->seqNumber() <= lastStableSeqNum_ + kWorkWindowSize);
    Assert(elem.prePrepare->viewNumber() == d.view);
    Assert(elem.prepareFull == nullptr
               || elem.prepareFull->viewNumber() == d.view);
    Assert(elem.prepareFull == nullptr ||
        elem.prepareFull->seqNumber() == elem.prePrepare->seqNumber());

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

  // delete messages from previous descriptor
  for (auto elem : descriptorOfLastExitFromView_.elements) {
    Assert(elem.prePrepare != nullptr);
    delete elem.prePrepare;
    delete elem.prepareFull;
  }

  hasDescriptorOfLastExitFromView_ = true;
  descriptorOfLastExitFromView_ = DescriptorOfLastExitFromView{
      d.view, d.lastStable, d.lastExecuted, clonedElements};
}

void PersistentStorageImp::setDescriptorOfLastNewView(
    const DescriptorOfLastNewView &d) {
  Assert(nonExecSetIsAllowed());
  Assert(d.view >= 1);
  Assert(hasDescriptorOfLastExitFromView_);
  Assert(d.view > descriptorOfLastExitFromView_.view);

  Assert(d.newViewMsg != nullptr);
  Assert(d.newViewMsg->newView() == d.view);

  const size_t numOfVCMsgs = 2 * config_->fVal + 2 * config_->cVal + 1;

  Assert(d.viewChangeMsgs.size() == numOfVCMsgs);

  std::vector<ViewChangeMsg *> clonedViewChangeMsgs(numOfVCMsgs);

  for (size_t i = 0; i < numOfVCMsgs; i++) {
    const ViewChangeMsg *vc = d.viewChangeMsgs[i];
    Assert(vc != nullptr);
    Assert(vc->newView() == d.view);

    Digest digestOfVCMsg;
    vc->getMsgDigest(digestOfVCMsg);
    Assert(d.newViewMsg->includesViewChangeFromReplica(
        vc->idOfGeneratedReplica(), digestOfVCMsg));

    ViewChangeMsg *clonedVC = (ViewChangeMsg *) vc->cloneObjAndMsg();
    Assert(clonedVC->type() == MsgCode::ViewChange);
    clonedViewChangeMsgs[i] = clonedVC;
  }

  NewViewMsg *clonedNewViewMsg = (NewViewMsg *) d.newViewMsg->cloneObjAndMsg();
  Assert(clonedNewViewMsg->type() == MsgCode::NewView);

  if (hasDescriptorOfLastNewView_) {
    // delete messages from previous descriptor
    delete descriptorOfLastNewView_.newViewMsg;

    Assert(descriptorOfLastNewView_.viewChangeMsgs.size() == numOfVCMsgs);

    for (size_t i = 0; i < numOfVCMsgs; i++) {
      delete descriptorOfLastNewView_.viewChangeMsgs[i];
    }
  }

  hasDescriptorOfLastNewView_ = true;
  descriptorOfLastNewView_ =
      DescriptorOfLastNewView{d.view,
                              clonedNewViewMsg,
                              clonedViewChangeMsgs,
                              d.maxSeqNumTransferredFromPrevViews};
}

void PersistentStorageImp::setDescriptorOfLastExecution(
    const DescriptorOfLastExecution &d) {
  Assert(setIsAllowed());
  Assert(!hasDescriptorOfLastExecution_ ||
      descriptorOfLastExecution_.executedSeqNum < d.executedSeqNum);
  Assert(lastExecutedSeqNum_ + 1 == d.executedSeqNum);
  Assert(d.validRequests.numOfBits() >= 1);
  Assert(d.validRequests.numOfBits() <= maxNumOfRequestsInBatch);

  hasDescriptorOfLastExecution_ = true;
  descriptorOfLastExecution_ =
      DescriptorOfLastExecution{d.executedSeqNum, d.validRequests};
}

void PersistentStorageImp::setLastStableSeqNum(const SeqNum &seqNum) {
  Assert(seqNum >= lastStableSeqNum_);
  lastStableSeqNum_ = seqNum;
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
  Assert(forceCompleted == true);
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
  Assert(completed == true);
  Assert(checkWindow_.insideActiveWindow(seqNum));
  CheckData &checkData = checkWindow_.get(seqNum);
  checkData.completedMark = completed;
}

bool PersistentStorageImp::hasReplicaConfig() {
  return (config_ != nullptr);
}

ReplicaConfig PersistentStorageImp::getReplicaConfig() {
  Assert(getIsAllowed());
  return *config_;
}

bool PersistentStorageImp::getFetchingState() {
  Assert(getIsAllowed());
  return fetchingState_;
}

SeqNum PersistentStorageImp::getLastExecutedSeqNum() {
  Assert(getIsAllowed());
  return lastStableSeqNum_;
}

SeqNum PersistentStorageImp::getPrimaryLastUsedSeqNum() {
  Assert(getIsAllowed());
  return primaryLastUsedSeqNum_;
}

SeqNum PersistentStorageImp::getStrictLowerBoundOfSeqNums() {
  Assert(getIsAllowed());
  return strictLowerBoundOfSeqNums_;
}

ViewNum
PersistentStorageImp::getLastViewThatTransferredSeqNumbersFullyExecuted() {
  Assert(getIsAllowed());
  return lastViewThatTransferredSeqNumbersFullyExecuted_;
}

bool PersistentStorageImp::hasDescriptorOfLastExitFromView() {
  Assert(getIsAllowed());
  return hasDescriptorOfLastExitFromView_;
}

PersistentStorage::DescriptorOfLastExitFromView
PersistentStorageImp::getAndAllocateDescriptorOfLastExitFromView() {
  Assert(getIsAllowed());
  Assert(hasDescriptorOfLastExitFromView_);

  DescriptorOfLastExitFromView &d = descriptorOfLastExitFromView_;

  std::vector<ViewsManager::PrevViewInfo> elements(d.elements.size());

  for (size_t i = 0; i < elements.size(); i++) {
    const ViewsManager::PrevViewInfo &e = d.elements[i];
    elements[i].prePrepare = (PrePrepareMsg *) e.prePrepare->cloneObjAndMsg();
    elements[i].hasAllRequests = e.hasAllRequests;
    if (e.prepareFull != nullptr)
      elements[i].prepareFull =
          (PrepareFullMsg *) e.prepareFull->cloneObjAndMsg();
    else
      elements[i].prepareFull = nullptr;
  }

  DescriptorOfLastExitFromView retVal{
      d.view, d.lastStable, d.lastExecuted, elements};

  return retVal;
}

bool PersistentStorageImp::hasDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  return hasDescriptorOfLastNewView_;
}

PersistentStorage::DescriptorOfLastNewView
PersistentStorageImp::getAndAllocateDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  Assert(hasDescriptorOfLastNewView_);

  DescriptorOfLastNewView &d = descriptorOfLastNewView_;

  NewViewMsg *newViewMsg = (NewViewMsg *) d.newViewMsg->cloneObjAndMsg();

  std::vector<ViewChangeMsg *> viewChangeMsgs(d.viewChangeMsgs.size());

  for (size_t i = 0; i < viewChangeMsgs.size(); i++) {
    viewChangeMsgs[i] = (ViewChangeMsg *) d.viewChangeMsgs[i]->cloneObjAndMsg();
  }

  DescriptorOfLastNewView retVal{
      d.view, newViewMsg, viewChangeMsgs, d.maxSeqNumTransferredFromPrevViews};

  return retVal;
}

bool PersistentStorageImp::hasDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  return hasDescriptorOfLastExecution_;
}

PersistentStorage::DescriptorOfLastExecution PersistentStorageImp::getDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  Assert(hasDescriptorOfLastExecution_);

  DescriptorOfLastExecution &d = descriptorOfLastExecution_;
  return DescriptorOfLastExecution{d.executedSeqNum, d.validRequests};
}

SeqNum PersistentStorageImp::getLastStableSeqNum() {
  Assert(getIsAllowed());
  return lastStableSeqNum_;
}

PrePrepareMsg *PersistentStorageImp::getAndAllocatePrePrepareMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  PrePrepareMsg *m =
      (PrePrepareMsg *) seqNumWindow_.get(seqNum).prePrepareMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::PrePrepare);
  return m;
}

bool PersistentStorageImp::getSlowStartedInSeqNumWindow(const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  bool b = seqNumWindow_.get(seqNum).slowStarted;
  return b;
}

FullCommitProofMsg *
PersistentStorageImp::getAndAllocateFullCommitProofMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  FullCommitProofMsg *m = (FullCommitProofMsg *) seqNumWindow_.get(seqNum)
      .fullCommitProofMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::FullCommitProof);
  return m;
}

bool PersistentStorageImp::getForceCompletedInSeqNumWindow(const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  bool b = seqNumWindow_.get(seqNum).forceCompleted;
  return b;
}

PrepareFullMsg *
PersistentStorageImp::getAndAllocatePrepareFullMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  PrepareFullMsg *m =
      (PrepareFullMsg *) seqNumWindow_.get(seqNum).prepareFullMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::PrepareFull);
  return m;
}

CommitFullMsg *PersistentStorageImp::getAndAllocateCommitFullMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  CommitFullMsg *m =
      (CommitFullMsg *) seqNumWindow_.get(seqNum).commitFullMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::CommitFull);
  return m;
}

CheckpointMsg *PersistentStorageImp::getAndAllocateCheckpointMsgInCheckWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow_.currentActiveWindow().first);
  Assert(checkWindow_.insideActiveWindow(seqNum));
  CheckpointMsg *m =
      (CheckpointMsg *) checkWindow_.get(seqNum).checkpointMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::Checkpoint);
  return m;
}

bool PersistentStorageImp::getCompletedMarkInCheckWindow(const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow_.currentActiveWindow().first);
  Assert(checkWindow_.insideActiveWindow(seqNum));
  bool b = checkWindow_.get(seqNum).completedMark;
  return b;
}

bool PersistentStorageImp::setIsAllowed() const {
  return (isInWriteTran() && (config_ != nullptr));
}

bool PersistentStorageImp::getIsAllowed() const {
  return (!isInWriteTran() && (config_ != nullptr));
}

bool PersistentStorageImp::nonExecSetIsAllowed() const {
  return setIsAllowed() &&
      (!hasDescriptorOfLastExecution_ ||
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
