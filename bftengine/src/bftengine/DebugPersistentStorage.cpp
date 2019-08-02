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

#include "DebugPersistentStorage.hpp"

#include "PrePrepareMsg.hpp"
#include "SignedShareMsgs.hpp"
#include "NewViewMsg.hpp"
#include "FullCommitProofMsg.hpp"
#include "CheckpointMsg.hpp"

namespace bftEngine {
namespace impl {

DebugPersistentStorage::DebugPersistentStorage(uint16_t fVal, uint16_t cVal)
    : fVal_{fVal},
      cVal_{cVal},
      seqNumWindow(1),
      checkWindow(0) {}

uint8_t DebugPersistentStorage::beginWriteTran() {
  return ++numOfNestedTransactions;
}

uint8_t DebugPersistentStorage::endWriteTran() {
  Assert(numOfNestedTransactions != 0);
  return --numOfNestedTransactions;
}

bool DebugPersistentStorage::isInWriteTran() const {
  return (numOfNestedTransactions != 0);
}

void DebugPersistentStorage::setReplicaConfig(const ReplicaConfig &config) {
  Assert(!hasConfig_);
  Assert(isInWriteTran());
  hasConfig_ = true;
  config_ = config;
}

void DebugPersistentStorage::setFetchingState(bool state) {
  Assert(nonExecSetIsAllowed());
  Assert(!state || !fetchingState_);  // f ==> !fetchingState_
  fetchingState_ = state;
}

void DebugPersistentStorage::setLastExecutedSeqNum(SeqNum seqNum) {
  Assert(setIsAllowed());
  Assert(lastExecutedSeqNum_ <= seqNum);
  lastExecutedSeqNum_ = seqNum;
}

void DebugPersistentStorage::setPrimaryLastUsedSeqNum(const SeqNum seqNum) {
  Assert(nonExecSetIsAllowed());
  primaryLastUsedSeqNum_ = seqNum;
}

void DebugPersistentStorage::setStrictLowerBoundOfSeqNums(const SeqNum seqNum) {
  Assert(nonExecSetIsAllowed());
  strictLowerBoundOfSeqNums_ = seqNum;
}

void DebugPersistentStorage::setLastViewThatTransferredSeqNumbersFullyExecuted(const ViewNum view) {
  Assert(nonExecSetIsAllowed());
  Assert(lastViewThatTransferredSeqNumbersFullyExecuted_ <= view);
  lastViewThatTransferredSeqNumbersFullyExecuted_ = view;
}

void DebugPersistentStorage::setDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &d) {
  Assert(nonExecSetIsAllowed());
  Assert(d.view >= 0);

  // Here we assume that the first view is always 0
  // (even if we load the initial state from disk)
  Assert(hasDescriptorOfLastNewView_ || d.view == 0);

  Assert(!hasDescriptorOfLastExitFromView_ ||
      d.view > descriptorOfLastExitFromView_.view);
  Assert(!hasDescriptorOfLastNewView_ ||
      d.view == descriptorOfLastNewView_.view);
  Assert(d.lastStable >= lastStableSeqNum_);
  Assert(d.lastExecuted >= lastExecutedSeqNum_);
  Assert(d.lastExecuted >= d.lastStable);
  Assert(d.stableLowerBoundWhenEnteredToView >= 0 &&
      d.lastStable >= d.stableLowerBoundWhenEnteredToView);
  Assert(d.elements.size() <= kWorkWindowSize);
  Assert(hasDescriptorOfLastExitFromView_ ||
      descriptorOfLastExitFromView_.elements.size() == 0);
  if (d.view > 0) {
    Assert(d.myViewChangeMsg != nullptr);
    Assert(d.myViewChangeMsg->newView() == d.view);
    // Assert(d.myViewChangeMsg->idOfGeneratedReplica() == myId); TODO(GG): add
    Assert(d.myViewChangeMsg->lastStable() <= d.lastStable);
  } else {
    Assert(d.myViewChangeMsg == nullptr);
  }

  std::vector<ViewsManager::PrevViewInfo> clonedElements(d.elements.size());

  for (size_t i = 0; i < d.elements.size(); i++) {
    const ViewsManager::PrevViewInfo &e = d.elements[i];
    Assert(e.prePrepare != nullptr);
    Assert(e.prePrepare->seqNumber() >= lastStableSeqNum_ + 1);
    Assert(e.prePrepare->seqNumber() <= lastStableSeqNum_ + kWorkWindowSize);
    Assert(e.prePrepare->viewNumber() == d.view);
    Assert(e.prepareFull == nullptr || e.prepareFull->viewNumber() == d.view);
    Assert(e.prepareFull == nullptr ||
        e.prepareFull->seqNumber() == e.prePrepare->seqNumber());

    PrePrepareMsg *clonedPrePrepareMsg = nullptr;
    if (e.prePrepare != nullptr) {
      clonedPrePrepareMsg = (PrePrepareMsg *) e.prePrepare->cloneObjAndMsg();
      Assert(clonedPrePrepareMsg->type() == MsgCode::PrePrepare);
    }

    PrepareFullMsg *clonedPrepareFull = nullptr;
    if (e.prepareFull != nullptr) {
      clonedPrepareFull = (PrepareFullMsg *) e.prepareFull->cloneObjAndMsg();
      Assert(clonedPrepareFull->type() == MsgCode::PrepareFull);
    }

    clonedElements[i].prePrepare = clonedPrePrepareMsg;
    clonedElements[i].hasAllRequests = e.hasAllRequests;
    clonedElements[i].prepareFull = clonedPrepareFull;
  }

  ViewChangeMsg *clonedViewChangeMsg = nullptr;
  if (d.myViewChangeMsg != nullptr)
    clonedViewChangeMsg = (ViewChangeMsg *) d.myViewChangeMsg->cloneObjAndMsg();

  // delete messages from previous descriptor
  for (size_t i = 0; i < descriptorOfLastExitFromView_.elements.size(); i++) {
    const ViewsManager::PrevViewInfo &e =
        descriptorOfLastExitFromView_.elements[i];
    Assert(e.prePrepare != nullptr);
    delete e.prePrepare;
    if (e.prepareFull != nullptr) delete e.prepareFull;
  }
  if (descriptorOfLastExitFromView_.myViewChangeMsg != nullptr)
    delete descriptorOfLastExitFromView_.myViewChangeMsg;

  hasDescriptorOfLastExitFromView_ = true;
  descriptorOfLastExitFromView_ = DescriptorOfLastExitFromView{
      d.view, d.lastStable, d.lastExecuted, clonedElements, clonedViewChangeMsg, d.stableLowerBoundWhenEnteredToView};
}

void DebugPersistentStorage::setDescriptorOfLastNewView(const DescriptorOfLastNewView &d) {
  Assert(nonExecSetIsAllowed());
  Assert(d.view >= 1);
  Assert(hasDescriptorOfLastExitFromView_);
  Assert(d.view > descriptorOfLastExitFromView_.view);

  Assert(d.newViewMsg != nullptr);
  Assert(d.newViewMsg->newView() == d.view);

  Assert(d.myViewChangeMsg == nullptr || d.myViewChangeMsg->newView() == d.view);

  const size_t numOfVCMsgs = 2 * fVal_ + 2 * cVal_ + 1;

  Assert(d.viewChangeMsgs.size() == numOfVCMsgs);

  std::vector<ViewChangeMsg *> clonedViewChangeMsgs(numOfVCMsgs);

  for (size_t i = 0; i < numOfVCMsgs; i++) {
    const ViewChangeMsg *vc = d.viewChangeMsgs[i];
    Assert(vc != nullptr);
    Assert(vc->newView() == d.view);
    Assert(d.myViewChangeMsg == nullptr ||
        d.myViewChangeMsg->idOfGeneratedReplica() != vc->idOfGeneratedReplica());

    Digest digestOfVCMsg;
    vc->getMsgDigest(digestOfVCMsg);
    Assert(d.newViewMsg->includesViewChangeFromReplica(
        vc->idOfGeneratedReplica(), digestOfVCMsg));

    ViewChangeMsg *clonedVC = (ViewChangeMsg *) vc->cloneObjAndMsg();
    Assert(clonedVC->type() == MsgCode::ViewChange);
    clonedViewChangeMsgs[i] = clonedVC;
  }

  // TODO(GG): check thay we a message with the id of the current replica

  NewViewMsg *clonedNewViewMsg = (NewViewMsg *) d.newViewMsg->cloneObjAndMsg();
  Assert(clonedNewViewMsg->type() == MsgCode::NewView);

  ViewChangeMsg *clonedMyViewChangeMsg = nullptr;
  if (d.myViewChangeMsg != nullptr)
    clonedMyViewChangeMsg = (ViewChangeMsg *) d.myViewChangeMsg->cloneObjAndMsg();

  if (hasDescriptorOfLastNewView_) {
    // delete messages from previous descriptor
    delete descriptorOfLastNewView_.newViewMsg;
    delete descriptorOfLastNewView_.myViewChangeMsg;

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
                              clonedMyViewChangeMsg,
                              d.stableLowerBoundWhenEnteredToView,
                              d.maxSeqNumTransferredFromPrevViews};
}

void DebugPersistentStorage::setDescriptorOfLastExecution(const DescriptorOfLastExecution &d) {
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

void DebugPersistentStorage::setLastStableSeqNum(SeqNum seqNum) {
  Assert(seqNum >= lastStableSeqNum_);
  lastStableSeqNum_ = seqNum;
  seqNumWindow.advanceActiveWindow(lastStableSeqNum_ + 1);
  checkWindow.advanceActiveWindow(lastStableSeqNum_);
}

void DebugPersistentStorage::DebugPersistentStorage::clearSeqNumWindow() {
  Assert(seqNumWindow.getBeginningOfActiveWindow() == lastStableSeqNum_ + 1);
  seqNumWindow.resetAll(seqNumWindow.getBeginningOfActiveWindow());
}

void DebugPersistentStorage::setPrePrepareMsgInSeqNumWindow(SeqNum seqNum, PrePrepareMsg *msg) {
  Assert(seqNumWindow.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow.get(seqNum);
  Assert(!seqNumData.isPrePrepareMsgSet());
  seqNumData.setPrePrepareMsg(msg->cloneObjAndMsg());
}

void DebugPersistentStorage::setSlowStartedInSeqNumWindow(SeqNum seqNum, bool slowStarted) {
  Assert(seqNumWindow.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow.get(seqNum);
  seqNumData.setSlowStarted(slowStarted);
}

void DebugPersistentStorage::setFullCommitProofMsgInSeqNumWindow(SeqNum seqNum, FullCommitProofMsg *msg) {
  Assert(seqNumWindow.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow.get(seqNum);
  Assert(!seqNumData.isFullCommitProofMsgSet());
  seqNumData.setFullCommitProofMsg(msg->cloneObjAndMsg());
}

void DebugPersistentStorage::setForceCompletedInSeqNumWindow(SeqNum seqNum, bool forceCompleted) {
  Assert(forceCompleted);
  Assert(seqNumWindow.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow.get(seqNum);
  seqNumData.setForceCompleted(forceCompleted);
}

void DebugPersistentStorage::setPrepareFullMsgInSeqNumWindow(SeqNum seqNum, PrepareFullMsg *msg) {
  Assert(seqNumWindow.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow.get(seqNum);
  Assert(!seqNumData.isPrepareFullMsgSet());
  seqNumData.setPrepareFullMsg(msg->cloneObjAndMsg());
}

void DebugPersistentStorage::setCommitFullMsgInSeqNumWindow(SeqNum seqNum, CommitFullMsg *msg) {
  Assert(seqNumWindow.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow.get(seqNum);
  Assert(!seqNumData.isCommitFullMsgSet());
  seqNumData.setCommitFullMsg(msg->cloneObjAndMsg());
}

void DebugPersistentStorage::setCheckpointMsgInCheckWindow(SeqNum s, CheckpointMsg *msg) {
  Assert(checkWindow.insideActiveWindow(s));
  CheckData &checkData = checkWindow.get(s);
  checkData.deleteCheckpointMsg();
  checkData.setCheckpointMsg(msg->cloneObjAndMsg());
}

void DebugPersistentStorage::setCompletedMarkInCheckWindow(SeqNum seqNum, bool mark) {
  Assert(mark == true);
  Assert(checkWindow.insideActiveWindow(seqNum));
  CheckData &checkData = checkWindow.get(seqNum);
  checkData.setCompletedMark(mark);
}

bool DebugPersistentStorage::hasReplicaConfig() const { return hasConfig_; }

ReplicaConfig DebugPersistentStorage::getReplicaConfig() {
  Assert(getIsAllowed());
  Assert(hasConfig_);
  return config_;
}

bool DebugPersistentStorage::getFetchingState() {
  Assert(getIsAllowed());
  return fetchingState_;
}

SeqNum DebugPersistentStorage::getLastExecutedSeqNum() {
  Assert(getIsAllowed());
  return lastExecutedSeqNum_;
}

SeqNum DebugPersistentStorage::getPrimaryLastUsedSeqNum() {
  Assert(getIsAllowed());
  return primaryLastUsedSeqNum_;
}

SeqNum DebugPersistentStorage::getStrictLowerBoundOfSeqNums() {
  Assert(getIsAllowed());
  return strictLowerBoundOfSeqNums_;
}

ViewNum
DebugPersistentStorage::getLastViewThatTransferredSeqNumbersFullyExecuted() {
  Assert(getIsAllowed());
  return lastViewThatTransferredSeqNumbersFullyExecuted_;
}

bool DebugPersistentStorage::hasDescriptorOfLastExitFromView() {
  Assert(getIsAllowed());
  return hasDescriptorOfLastExitFromView_;
}

DescriptorOfLastExitFromView
DebugPersistentStorage::getAndAllocateDescriptorOfLastExitFromView() {
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

  Assert(d.myViewChangeMsg != nullptr || d.view == 0);
  ViewChangeMsg *myVCMsg = nullptr;
  if (d.myViewChangeMsg != nullptr) myVCMsg = (ViewChangeMsg *) d.myViewChangeMsg->cloneObjAndMsg();

  DescriptorOfLastExitFromView retVal{
      d.view, d.lastStable, d.lastExecuted, elements, myVCMsg, d.stableLowerBoundWhenEnteredToView};

  return retVal;
}

bool DebugPersistentStorage::hasDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  return hasDescriptorOfLastNewView_;
}

DescriptorOfLastNewView
DebugPersistentStorage::getAndAllocateDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  Assert(hasDescriptorOfLastNewView_);

  DescriptorOfLastNewView &d = descriptorOfLastNewView_;

  NewViewMsg *newViewMsg = (NewViewMsg *) d.newViewMsg->cloneObjAndMsg();

  std::vector<ViewChangeMsg *> viewChangeMsgs(d.viewChangeMsgs.size());

  for (size_t i = 0; i < viewChangeMsgs.size(); i++) {
    viewChangeMsgs[i] = (ViewChangeMsg *) d.viewChangeMsgs[i]->cloneObjAndMsg();
  }

  ViewChangeMsg *myViewChangeMsg = nullptr;
  if (d.myViewChangeMsg != nullptr)
    myViewChangeMsg = (ViewChangeMsg *) d.myViewChangeMsg->cloneObjAndMsg();

  DescriptorOfLastNewView retVal{
      d.view, newViewMsg, viewChangeMsgs, myViewChangeMsg, d.stableLowerBoundWhenEnteredToView,
      d.maxSeqNumTransferredFromPrevViews};

  return retVal;
}

bool DebugPersistentStorage::hasDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  return hasDescriptorOfLastExecution_;
}

DescriptorOfLastExecution
DebugPersistentStorage::getDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  Assert(hasDescriptorOfLastExecution_);

  DescriptorOfLastExecution &d = descriptorOfLastExecution_;

  return DescriptorOfLastExecution{d.executedSeqNum, d.validRequests};
}

SeqNum DebugPersistentStorage::getLastStableSeqNum() {
  Assert(getIsAllowed());
  return lastStableSeqNum_;
}

PrePrepareMsg *
DebugPersistentStorage::getAndAllocatePrePrepareMsgInSeqNumWindow(SeqNum seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.getBeginningOfActiveWindow());
  Assert(seqNumWindow.insideActiveWindow(seqNum));

  PrePrepareMsg *orgMsg = seqNumWindow.get(seqNum).getPrePrepareMsg();
  if (orgMsg == nullptr) return nullptr;

  PrePrepareMsg *m = (PrePrepareMsg *) orgMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::PrePrepare);
  return m;
}

bool DebugPersistentStorage::getSlowStartedInSeqNumWindow(SeqNum seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.getBeginningOfActiveWindow());
  Assert(seqNumWindow.insideActiveWindow(seqNum));
  bool b = seqNumWindow.get(seqNum).getSlowStarted();
  return b;
}

FullCommitProofMsg *
DebugPersistentStorage::getAndAllocateFullCommitProofMsgInSeqNumWindow(SeqNum seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.getBeginningOfActiveWindow());
  Assert(seqNumWindow.insideActiveWindow(seqNum));

  FullCommitProofMsg *orgMsg = seqNumWindow.get(seqNum).getFullCommitProofMsg();
  if (orgMsg == nullptr) return nullptr;

  FullCommitProofMsg *m = (FullCommitProofMsg *) orgMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::FullCommitProof);
  return m;
}

bool DebugPersistentStorage::getForceCompletedInSeqNumWindow(SeqNum seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.getBeginningOfActiveWindow());
  Assert(seqNumWindow.insideActiveWindow(seqNum));
  bool b = seqNumWindow.get(seqNum).getForceCompleted();
  return b;
}

PrepareFullMsg *
DebugPersistentStorage::getAndAllocatePrepareFullMsgInSeqNumWindow(SeqNum seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.getBeginningOfActiveWindow());
  Assert(seqNumWindow.insideActiveWindow(seqNum));

  PrepareFullMsg *orgMsg = seqNumWindow.get(seqNum).getPrepareFullMsg();
  if (orgMsg == nullptr) return nullptr;

  PrepareFullMsg *m = (PrepareFullMsg *) orgMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::PrepareFull);
  return m;
}

CommitFullMsg *
DebugPersistentStorage::getAndAllocateCommitFullMsgInSeqNumWindow(SeqNum seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.getBeginningOfActiveWindow());
  Assert(seqNumWindow.insideActiveWindow(seqNum));

  CommitFullMsg *orgMsg = seqNumWindow.get(seqNum).getCommitFullMsg();
  if (orgMsg == nullptr) return nullptr;

  CommitFullMsg *m = (CommitFullMsg *) orgMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::CommitFull);
  return m;
}

CheckpointMsg *DebugPersistentStorage::getAndAllocateCheckpointMsgInCheckWindow(SeqNum seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow.getBeginningOfActiveWindow());
  Assert(checkWindow.insideActiveWindow(seqNum));

  CheckpointMsg *orgMsg = checkWindow.get(seqNum).getCheckpointMsg();
  if (orgMsg == nullptr) return nullptr;

  CheckpointMsg *m = (CheckpointMsg *) orgMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::Checkpoint);
  return m;
}

bool DebugPersistentStorage::getCompletedMarkInCheckWindow(SeqNum seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow.getBeginningOfActiveWindow());
  Assert(checkWindow.insideActiveWindow(seqNum));
  bool b = checkWindow.get(seqNum).getCompletedMark();
  return b;
}

bool DebugPersistentStorage::setIsAllowed() const {
  return isInWriteTran() && hasConfig_;
}

bool DebugPersistentStorage::getIsAllowed() const {
  return !isInWriteTran() && hasConfig_;
}

bool DebugPersistentStorage::nonExecSetIsAllowed() const {
  return setIsAllowed() &&
      (!hasDescriptorOfLastExecution_ ||
          descriptorOfLastExecution_.executedSeqNum <= lastExecutedSeqNum_);
}

}  // namespace impl
}  // namespace bftEngine
