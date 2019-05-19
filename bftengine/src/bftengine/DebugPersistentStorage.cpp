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
      seqNumWindow{1, nullptr},
      checkWindow{0, nullptr}

{}

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

void DebugPersistentStorage::setReplicaConfig(ReplicaConfig config) {
  Assert(!hasConfig_);
  Assert(isInWriteTran());
  hasConfig_ = true;
  config_ = config;
}

void DebugPersistentStorage::setFetchingState(const bool f) {
  Assert(nonExecSetIsAllowed());
  Assert(!f || !fetchingState_);  // f ==> !fetchingState_
  fetchingState_ = f;
}

void DebugPersistentStorage::setLastExecutedSeqNum(const SeqNum s) {
  Assert(setIsAllowed());
  Assert(lastExecutedSeqNum_ <= s);
  lastExecutedSeqNum_ = s;
}

void DebugPersistentStorage::setPrimaryLastUsedSeqNum(const SeqNum s) {
  Assert(nonExecSetIsAllowed());
  primaryLastUsedSeqNum_ = s;
}

void DebugPersistentStorage::setStrictLowerBoundOfSeqNums(const SeqNum s) {
  Assert(nonExecSetIsAllowed());
  strictLowerBoundOfSeqNums_ = s;
}

void DebugPersistentStorage::setLastViewThatTransferredSeqNumbersFullyExecuted(
    const ViewNum v) {
  Assert(nonExecSetIsAllowed());
  Assert(lastViewThatTransferredSeqNumbersFullyExecuted_ <= v);
  lastViewThatTransferredSeqNumbersFullyExecuted_ = v;
}

void DebugPersistentStorage::setDescriptorOfLastExitFromView(
    const DescriptorOfLastExitFromView& d) {
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

  for (size_t i = 0; i < d.elements.size(); i++) {
    const ViewsManager::PrevViewInfo& e = d.elements[i];
    Assert(e.prePrepare != nullptr);
    Assert(e.prePrepare->seqNumber() >= lastStableSeqNum_ + 1);
    Assert(e.prePrepare->seqNumber() <= lastStableSeqNum_ + kWorkWindowSize);
    Assert(e.prePrepare->viewNumber() == d.view);
    Assert(e.prepareFull == nullptr || e.prepareFull->viewNumber() == d.view);
    Assert(e.prepareFull == nullptr ||
           e.prepareFull->seqNumber() == e.prePrepare->seqNumber());

    PrePrepareMsg* clonedPrePrepareMsg =
        (PrePrepareMsg*)e.prePrepare->cloneObjAndMsg();
    Assert(clonedPrePrepareMsg->type() == MsgCode::PrePrepare);
    PrepareFullMsg* clonedPrepareFull = nullptr;
    if (e.prepareFull != nullptr) {
      clonedPrepareFull = (PrepareFullMsg*)e.prepareFull->cloneObjAndMsg();
      Assert(clonedPrepareFull->type() == MsgCode::PrepareFull);
    }

    clonedElements[i].prePrepare = clonedPrePrepareMsg;
    clonedElements[i].hasAllRequests = e.hasAllRequests;
    clonedElements[i].prepareFull = clonedPrepareFull;
  }

  // delete messages from previous descriptor
  for (size_t i = 0; i < descriptorOfLastExitFromView_.elements.size(); i++) {
    const ViewsManager::PrevViewInfo& e =
        descriptorOfLastExitFromView_.elements[i];
    Assert(e.prePrepare != nullptr);
    delete e.prePrepare;
    if (e.prepareFull != nullptr) delete e.prepareFull;
  }

  hasDescriptorOfLastExitFromView_ = true;
  descriptorOfLastExitFromView_ = DescriptorOfLastExitFromView{
      d.view, d.lastStable, d.lastExecuted, clonedElements};
}

void DebugPersistentStorage::setDescriptorOfLastNewView(
    const DescriptorOfLastNewView& d) {
  Assert(nonExecSetIsAllowed());
  Assert(d.view >= 1);
  Assert(hasDescriptorOfLastExitFromView_);
  Assert(d.view > descriptorOfLastExitFromView_.view);

  Assert(d.newViewMsg != nullptr);
  Assert(d.newViewMsg->newView() == d.view);

  const size_t numOfVCMsgs = 2 * fVal_ + 2 * cVal_ + 1;

  Assert(d.viewChangeMsgs.size() == numOfVCMsgs);

  std::vector<ViewChangeMsg*> clonedViewChangeMsgs(numOfVCMsgs);

  for (size_t i = 0; i < numOfVCMsgs; i++) {
    const ViewChangeMsg* vc = d.viewChangeMsgs[i];
    Assert(vc != nullptr);
    Assert(vc->newView() == d.view);

    Digest digestOfVCMsg;
    vc->getMsgDigest(digestOfVCMsg);
    Assert(d.newViewMsg->includesViewChangeFromReplica(
        vc->idOfGeneratedReplica(), digestOfVCMsg));

    ViewChangeMsg* clonedVC = (ViewChangeMsg*)vc->cloneObjAndMsg();
    Assert(clonedVC->type() == MsgCode::ViewChange);
    clonedViewChangeMsgs[i] = clonedVC;
  }

  NewViewMsg* clonedNewViewMsg = (NewViewMsg*)d.newViewMsg->cloneObjAndMsg();
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

void DebugPersistentStorage::setDescriptorOfLastExecution(
    const DescriptorOfLastExecution& d) {
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

void DebugPersistentStorage::setLastStableSeqNum(const SeqNum s) {
  Assert(s >= lastStableSeqNum_);
  lastStableSeqNum_ = s;
  seqNumWindow.advanceActiveWindow(lastStableSeqNum_ + 1);
  checkWindow.advanceActiveWindow(lastStableSeqNum_);
}

void DebugPersistentStorage::DebugPersistentStorage::clearSeqNumWindow() {
  SeqNum s = seqNumWindow.currentActiveWindow().first;
  Assert(s == lastStableSeqNum_ + 1);
  seqNumWindow.resetAll(s);
}

void DebugPersistentStorage::setPrePrepareMsgInSeqNumWindow(
    const SeqNum s, const PrePrepareMsg* const m) {
  Assert(seqNumWindow.insideActiveWindow(s));
  SeqNumData& seqNumData = seqNumWindow.get(s);
  Assert(seqNumData.prePrepareMsg == nullptr);
  seqNumData.prePrepareMsg = (PrePrepareMsg*)m->cloneObjAndMsg();
}

void DebugPersistentStorage::setSlowStartedInSeqNumWindow(
    const SeqNum s, const bool slowStarted) {
  Assert(seqNumWindow.insideActiveWindow(s));
  SeqNumData& seqNumData = seqNumWindow.get(s);
  seqNumData.slowStarted = slowStarted;
}

void DebugPersistentStorage::setFullCommitProofMsgInSeqNumWindow(
    const SeqNum s, const FullCommitProofMsg* const m) {
  Assert(seqNumWindow.insideActiveWindow(s));
  SeqNumData& seqNumData = seqNumWindow.get(s);
  Assert(seqNumData.fullCommitProofMsg == nullptr);
  seqNumData.fullCommitProofMsg = (FullCommitProofMsg*)m->cloneObjAndMsg();
}

void DebugPersistentStorage::setForceCompletedInSeqNumWindow(
    const SeqNum s, const bool forceCompleted) {
  Assert(forceCompleted == true);
  Assert(seqNumWindow.insideActiveWindow(s));
  SeqNumData& seqNumData = seqNumWindow.get(s);
  seqNumData.forceCompleted = forceCompleted;
}

void DebugPersistentStorage::setPrepareFullMsgInSeqNumWindow(
    const SeqNum s, const PrepareFullMsg* const m) {
  Assert(seqNumWindow.insideActiveWindow(s));
  SeqNumData& seqNumData = seqNumWindow.get(s);
  Assert(seqNumData.prepareFullMsg == nullptr);
  seqNumData.prepareFullMsg = (PrepareFullMsg*)m->cloneObjAndMsg();
}

void DebugPersistentStorage::setCommitFullMsgInSeqNumWindow(
    const SeqNum s, const CommitFullMsg* const m) {
  Assert(seqNumWindow.insideActiveWindow(s));
  SeqNumData& seqNumData = seqNumWindow.get(s);
  Assert(seqNumData.commitFullMsg == nullptr);
  seqNumData.commitFullMsg = (CommitFullMsg*)m->cloneObjAndMsg();
}

void DebugPersistentStorage::setCheckpointMsgInCheckWindow(
    const SeqNum s, const CheckpointMsg* const m) {
  Assert(checkWindow.insideActiveWindow(s));
  CheckData& checkData = checkWindow.get(s);
  if (checkData.checkpointMsg != nullptr) delete checkData.checkpointMsg;
  checkData.checkpointMsg = (CheckpointMsg*)m->cloneObjAndMsg();
}

void DebugPersistentStorage::setCompletedMarkInCheckWindow(const SeqNum s,
                                                           const bool f) {
  Assert(f == true);
  Assert(checkWindow.insideActiveWindow(s));
  CheckData& checkData = checkWindow.get(s);
  checkData.completedMark = f;
}

bool DebugPersistentStorage::hasReplicaConfig() { return hasConfig_; }

ReplicaConfig DebugPersistentStorage::getReplicaConig() {
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
  return lastStableSeqNum_;
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

PersistentStorage::DescriptorOfLastExitFromView
DebugPersistentStorage::getAndAllocateDescriptorOfLastExitFromView() {
  Assert(getIsAllowed());
  Assert(hasDescriptorOfLastExitFromView_);

  DescriptorOfLastExitFromView& d = descriptorOfLastExitFromView_;

  std::vector<ViewsManager::PrevViewInfo> elements(d.elements.size());

  for (size_t i = 0; i < elements.size(); i++) {
    const ViewsManager::PrevViewInfo& e = d.elements[i];
    elements[i].prePrepare = (PrePrepareMsg*)e.prePrepare->cloneObjAndMsg();
    elements[i].hasAllRequests = e.hasAllRequests;
    if (e.prepareFull != nullptr)
      elements[i].prepareFull =
          (PrepareFullMsg*)e.prepareFull->cloneObjAndMsg();
    else
      elements[i].prepareFull = nullptr;
  }

  DescriptorOfLastExitFromView retVal{
      d.view, d.lastStable, d.lastExecuted, elements};

  return retVal;
}

bool DebugPersistentStorage::hasDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  return hasDescriptorOfLastNewView_;
}

PersistentStorage::DescriptorOfLastNewView
DebugPersistentStorage::getAndAllocateDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  Assert(hasDescriptorOfLastNewView_);

  DescriptorOfLastNewView& d = descriptorOfLastNewView_;

  NewViewMsg* newViewMsg = (NewViewMsg*)d.newViewMsg->cloneObjAndMsg();

  std::vector<ViewChangeMsg*> viewChangeMsgs(d.viewChangeMsgs.size());

  for (size_t i = 0; i < viewChangeMsgs.size(); i++) {
    viewChangeMsgs[i] = (ViewChangeMsg*)d.viewChangeMsgs[i]->cloneObjAndMsg();
  }

  DescriptorOfLastNewView retVal{
      d.view, newViewMsg, viewChangeMsgs, d.maxSeqNumTransferredFromPrevViews};

  return retVal;
}

bool DebugPersistentStorage::hasDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  return hasDescriptorOfLastExecution_;
}

PersistentStorage::DescriptorOfLastExecution
DebugPersistentStorage::getDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  Assert(hasDescriptorOfLastExecution_);

  DescriptorOfLastExecution& d = descriptorOfLastExecution_;

  return PersistentStorage::DescriptorOfLastExecution{d.executedSeqNum,
                                                      d.validRequests};
}

SeqNum DebugPersistentStorage::getLastStableSeqNum() {
  Assert(getIsAllowed());
  return lastStableSeqNum_;
}

PrePrepareMsg*
DebugPersistentStorage::getAndAllocatePrePrepareMsgInSeqNumWindow(
    const SeqNum s) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.currentActiveWindow().first);
  Assert(seqNumWindow.insideActiveWindow(s));
  PrePrepareMsg* m =
      (PrePrepareMsg*)seqNumWindow.get(s).prePrepareMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::PrePrepare);
  return m;
}

bool DebugPersistentStorage::getSlowStartedInSeqNumWindow(const SeqNum s) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.currentActiveWindow().first);
  Assert(seqNumWindow.insideActiveWindow(s));
  bool b = seqNumWindow.get(s).slowStarted;
  return b;
}

FullCommitProofMsg*
DebugPersistentStorage::getAndAllocateFullCommitProofMsgInSeqNumWindow(
    const SeqNum s) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.currentActiveWindow().first);
  Assert(seqNumWindow.insideActiveWindow(s));
  FullCommitProofMsg* m = (FullCommitProofMsg*)seqNumWindow.get(s)
                              .fullCommitProofMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::FullCommitProof);
  return m;
}

bool DebugPersistentStorage::getForceCompletedInSeqNumWindow(const SeqNum s) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.currentActiveWindow().first);
  Assert(seqNumWindow.insideActiveWindow(s));
  bool b = seqNumWindow.get(s).forceCompleted;
  return b;
}

PrepareFullMsg*
DebugPersistentStorage::getAndAllocatePrepareFullMsgInSeqNumWindow(
    const SeqNum s) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.currentActiveWindow().first);
  Assert(seqNumWindow.insideActiveWindow(s));
  PrepareFullMsg* m =
      (PrepareFullMsg*)seqNumWindow.get(s).prepareFullMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::PrepareFull);
  return m;
}

CommitFullMsg*
DebugPersistentStorage::getAndAllocateCommitFullMsgInSeqNumWindow(
    const SeqNum s) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow.currentActiveWindow().first);
  Assert(seqNumWindow.insideActiveWindow(s));
  CommitFullMsg* m =
      (CommitFullMsg*)seqNumWindow.get(s).commitFullMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::CommitFull);
  return m;
}

CheckpointMsg* DebugPersistentStorage::getAndAllocateCheckpointMsgInCheckWindow(
    const SeqNum s) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow.currentActiveWindow().first);
  Assert(checkWindow.insideActiveWindow(s));
  CheckpointMsg* m =
      (CheckpointMsg*)checkWindow.get(s).checkpointMsg->cloneObjAndMsg();
  Assert(m->type() == MsgCode::Checkpoint);
  return m;
}

bool DebugPersistentStorage::getCompletedMarkInCheckWindow(const SeqNum s) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow.currentActiveWindow().first);
  Assert(checkWindow.insideActiveWindow(s));
  bool b = checkWindow.get(s).completedMark;
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

void DebugPersistentStorage::WindowFuncs::init(SeqNumData& i, void* d) {
  i.prePrepareMsg = nullptr;
  i.slowStarted = false;
  i.fullCommitProofMsg = nullptr;
  i.forceCompleted = false;
  i.prepareFullMsg = nullptr;
  i.commitFullMsg = nullptr;
}

void DebugPersistentStorage::WindowFuncs::free(SeqNumData& i) { reset(i); }

void DebugPersistentStorage::WindowFuncs::reset(SeqNumData& i) {
  if (i.prePrepareMsg != nullptr) delete i.prePrepareMsg;
  if (i.fullCommitProofMsg != nullptr) delete i.fullCommitProofMsg;
  if (i.prepareFullMsg != nullptr) delete i.prepareFullMsg;
  if (i.commitFullMsg != nullptr) delete i.commitFullMsg;
  i.prePrepareMsg = nullptr;
  i.slowStarted = false;
  i.fullCommitProofMsg = nullptr;
  i.forceCompleted = false;
  i.prepareFullMsg = nullptr;
  i.commitFullMsg = nullptr;
}

void DebugPersistentStorage::WindowFuncs::init(CheckData& i, void* d) {
  i.checkpointMsg = nullptr;
  i.completedMark = false;
}
void DebugPersistentStorage::WindowFuncs::free(CheckData& i) { reset(i); }

void DebugPersistentStorage::WindowFuncs::reset(CheckData& i) {
  if (i.checkpointMsg != nullptr) delete i.checkpointMsg;
  i.checkpointMsg = nullptr;
  i.completedMark = false;
}

}  // namespace impl
}  // namespace bftEngine
