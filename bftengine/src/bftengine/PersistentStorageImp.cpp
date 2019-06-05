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

using namespace std;

namespace bftEngine {
namespace impl {

PersistentStorageImp::PersistentStorageImp(uint16_t fVal, uint16_t cVal)
    : numOfReplicas_(3 * fVal + 2 * cVal + 1),
      metadataParamsNum_(LAST_NEW_VIEW_DESC + numOfReplicas_ + 1),
      seqNumWindow_{seqNumWindowFirst_, nullptr},
      checkWindow_{checkWindowFirst_, nullptr} {
  seqNumWindowLast_ = seqNumWindowFirst_ + seqNumWindow_.getNumItems() - 1;
  checkWindowLast_ = checkWindowFirst_ + checkWindow_.getNumItems() - 1;
}

void PersistentStorageImp::init(MetadataStorage *&metadataStorage) {
  initMetadataStorage(metadataStorage);
  setDefaultsInMetadataStorage();
}

void PersistentStorageImp::setDefaultsInMetadataStorage() {
  beginWriteTran();

  setFetchingStateInternal(fetchingState_);
  setLastExecutedSeqNumInternal(lastExecutedSeqNum_);
  setPrimaryLastUsedSeqNumInternal(primaryLastUsedSeqNum_);
  setStrictLowerBoundOfSeqNumsInternal(strictLowerBoundOfSeqNums_);
  setLastViewTransferredSeqNumbersInternal(lastViewTransferredSeqNum_);
  setLastStableSeqNumInternal(lastStableSeqNum_);

  initDescriptorOfLastExitFromView(descriptorOfLastExitFromView_);
  initDescriptorOfLastNewView(descriptorOfLastNewView_);
  initDescriptorOfLastExecution(descriptorOfLastExecution_);

  endWriteTran();
}

void PersistentStorageImp::initMetadataStorage(
    MetadataStorage *&metadataStorage) {
  metadataStorage_ = metadataStorage;

  unique_ptr<MetadataStorage::ObjectDesc> metadataObjectsArray(
      new MetadataStorage::ObjectDesc[metadataParamsNum_]);
  for (uint32_t i = 0; i < metadataParamsNum_; ++i)
    metadataObjectsArray.get()[i].id = i;

  metadataObjectsArray.get()[LAST_STABLE_SEQ_NUM].maxSize =
      sizeof(lastStableSeqNum_);
  metadataObjectsArray.get()[LAST_EXEC_SEQ_NUM].maxSize =
      sizeof(lastExecutedSeqNum_);
  metadataObjectsArray.get()[PRIMARY_LAST_USED_SEQ_NUM].maxSize =
      sizeof(primaryLastUsedSeqNum_);
  metadataObjectsArray.get()[LOWER_BOUND_OF_SEQ_NUM].maxSize =
      sizeof(strictLowerBoundOfSeqNums_);
  metadataObjectsArray.get()[LAST_VIEW_TRANSFERRED_SEQ_NUM].maxSize =
      sizeof(lastViewTransferredSeqNum_);
  metadataObjectsArray.get()[FETCHING_STATE].maxSize = sizeof(fetchingState_);

  metadataObjectsArray.get()[REPLICA_CONFIG].maxSize =
      ReplicaConfigSerializer::maxSize(numOfReplicas_);

  metadataObjectsArray.get()[SEQ_NUM_WINDOW].maxSize =
      SeqNumWindow::simpleParamsSize();

  metadataObjectsArray.get()[CHECK_WINDOW].maxSize =
      CheckWindow::simpleParamsSize();

  for (uint32_t i = 0; i < kWorkWindowSize; ++i) {
    metadataObjectsArray.get()[LAST_EXIT_FROM_VIEW_DESC + 1 + i].maxSize =
        DescriptorOfLastExitFromView::maxElementSize();
    metadataObjectsArray.get()[SEQ_NUM_WINDOW + 1 + i].maxSize =
        SeqNumWindow::maxElementSize();
  }

  for (uint32_t i = 0; i < numOfReplicas_; ++i) {
    metadataObjectsArray.get()[LAST_NEW_VIEW_DESC + 1 + i].maxSize =
        DescriptorOfLastNewView::maxElementSize();
  }

  for (uint32_t i = 0; i < checkWinSize; ++i)
    metadataObjectsArray.get()[CHECK_WINDOW + 1 + i].maxSize =
        CheckWindow::maxElementSize();

  metadataObjectsArray.get()[LAST_EXIT_FROM_VIEW_DESC].maxSize =
      DescriptorOfLastExitFromView::simpleParamsSize();

  metadataObjectsArray.get()[LAST_EXEC_DESC].maxSize =
      DescriptorOfLastExecution::maxSize();

  metadataObjectsArray.get()[LAST_NEW_VIEW_DESC].maxSize =
      DescriptorOfLastNewView::simpleParamsSize();

  metadataStorage_->initMaxSizeOfObjects(metadataObjectsArray.get(),
                                         metadataParamsNum_);
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
  int64_t outBufSize = ReplicaConfigSerializer::maxSize(numOfReplicas_);
  configSerializer_->serialize(outBuf, outBufSize);
  metadataStorage_->writeInTransaction(REPLICA_CONFIG, (char *) outBuf.get(),
                                       outBufSize);
}

void PersistentStorageImp::setFetchingStateInternal(const bool &state) {
  fetchingState_ = state;
  metadataStorage_->writeInTransaction(FETCHING_STATE, (char *) &state,
                                       sizeof(state));
}

void PersistentStorageImp::setFetchingState(const bool &state) {
  Assert(nonExecSetIsAllowed());
  Assert(!state);
  setFetchingStateInternal(state);
}

void PersistentStorageImp::setLastExecutedSeqNumInternal(const SeqNum &seqNum) {
  lastExecutedSeqNum_ = seqNum;
  metadataStorage_->writeInTransaction(LAST_EXEC_SEQ_NUM, (char *) &seqNum,
                                       sizeof(seqNum));
}

void PersistentStorageImp::setLastExecutedSeqNum(const SeqNum &seqNum) {
  Assert(setIsAllowed());
  Assert(lastExecutedSeqNum_ <= seqNum);
  setLastExecutedSeqNumInternal(seqNum);
}

void PersistentStorageImp::setPrimaryLastUsedSeqNumInternal(
    const SeqNum &seqNum) {
  primaryLastUsedSeqNum_ = seqNum;
  metadataStorage_->writeInTransaction(PRIMARY_LAST_USED_SEQ_NUM,
                                       (char *) &seqNum, sizeof(seqNum));
}

void PersistentStorageImp::setPrimaryLastUsedSeqNum(const SeqNum &seqNum) {
  Assert(nonExecSetIsAllowed());
  setPrimaryLastUsedSeqNumInternal(seqNum);
}

void PersistentStorageImp::setStrictLowerBoundOfSeqNumsInternal(
    const SeqNum &seqNum) {
  strictLowerBoundOfSeqNums_ = seqNum;
  metadataStorage_->writeInTransaction(LOWER_BOUND_OF_SEQ_NUM,
                                       (char *) &seqNum, sizeof(seqNum));
}

void PersistentStorageImp::setStrictLowerBoundOfSeqNums(const SeqNum &seqNum) {
  Assert(nonExecSetIsAllowed());
  setStrictLowerBoundOfSeqNumsInternal(seqNum);
}

void PersistentStorageImp::setLastViewTransferredSeqNumbersInternal(
    const ViewNum &view) {
  lastViewTransferredSeqNum_ = view;
  metadataStorage_->writeInTransaction(LAST_VIEW_TRANSFERRED_SEQ_NUM,
                                       (char *) &view, sizeof(view));
}

void PersistentStorageImp::setLastViewThatTransferredSeqNumbersFullyExecuted(
    const ViewNum &view) {
  Assert(nonExecSetIsAllowed());
  Assert(lastViewTransferredSeqNum_ <= view);
  setLastViewTransferredSeqNumbersInternal(view);
}

/***** Descriptors handling *****/

void PersistentStorageImp::saveDescriptorOfLastExitFromView(
    const DescriptorOfLastExitFromView &newDesc, bool setAll) {
  const size_t bufLen = DescriptorOfLastExitFromView::maxSize();
  char *descBuf = new char[bufLen];
  char *descBufPtr = descBuf;
  const size_t simpleParamsSize =
      DescriptorOfLastExitFromView::simpleParamsSize();
  newDesc.serializeSimpleParams(descBufPtr, simpleParamsSize);
  metadataStorage_->writeInTransaction(LAST_EXIT_FROM_VIEW_DESC, descBufPtr,
                                       simpleParamsSize);
  size_t actualElementSize = 0;
  uint32_t elementsNum = newDesc.elements.size();
  for (uint32_t i = 0; i < elementsNum; ++i) {
    if (!setAll) {
      // Don't re-write unchanged elements
      if (descriptorOfLastExitFromView_.elements[i] == newDesc.elements[i])
        continue;
    }
    newDesc.serializeElement(
        i, descBufPtr, DescriptorOfLastExitFromView::maxElementSize(),
        actualElementSize);
    Assert(actualElementSize);
    metadataStorage_->writeInTransaction(
        LAST_EXIT_FROM_VIEW_DESC + 1 + i, descBufPtr, actualElementSize);
  }
  delete[] descBuf;
}

void PersistentStorageImp::setDescriptorOfLastExitFromView(
    const DescriptorOfLastExitFromView &desc, bool setAll) {
  if (!setAll) {
    verifySetDescriptorOfLastExitFromView(desc);
    verifyPrevViewInfo(desc);
  }
  saveDescriptorOfLastExitFromView(desc, setAll);
  descriptorOfLastExitFromView_ = desc;
}

void PersistentStorageImp::setDescriptorOfLastExitFromView(
    const DescriptorOfLastExitFromView &desc) {
  setDescriptorOfLastExitFromView(desc, false);
}

void PersistentStorageImp::initDescriptorOfLastExitFromView(
    const DescriptorOfLastExitFromView &desc) {
  setDescriptorOfLastExitFromView(desc, true);
}

void PersistentStorageImp::saveDescriptorOfLastNewView(
    const DescriptorOfLastNewView &newDesc, bool setAll) {
  const size_t bufLen = DescriptorOfLastNewView::maxSize(numOfReplicas_);
  char *descBuf = new char[bufLen];
  char *descBufPtr = descBuf;
  const size_t simpleParamsSize = DescriptorOfLastNewView::simpleParamsSize();
  size_t actualSize = 0;
  newDesc.serializeSimpleParams(descBufPtr, simpleParamsSize, actualSize);
  metadataStorage_->writeInTransaction(
      LAST_NEW_VIEW_DESC, descBufPtr, actualSize);

  size_t actualElementSize = 0;
  uint32_t numOfMessages = descriptorOfLastNewView_.viewChangeMsgs.size();
  for (uint32_t i = 0; i < numOfMessages; ++i) {
    if (!setAll) {
      // Don't re-write unchanged elements
      if (descriptorOfLastNewView_.viewChangeMsgs[i]
          == newDesc.viewChangeMsgs[i])
        continue;
    }
    newDesc.serializeElement(
        i, descBufPtr, DescriptorOfLastNewView::maxElementSize(),
        actualElementSize);
    Assert(actualElementSize);
    metadataStorage_->writeInTransaction(
        LAST_NEW_VIEW_DESC + 1 + i, descBufPtr, actualElementSize);
  }
  delete[] descBuf;
}

void PersistentStorageImp::setDescriptorOfLastNewView(
    const DescriptorOfLastNewView &desc, bool setAll) {
  if (!setAll) {
    verifySetDescriptorOfLastNewView(desc);
    verifyLastNewViewMsgs(desc);
  }
  saveDescriptorOfLastNewView(desc, setAll);
  descriptorOfLastNewView_ = desc;
}

void PersistentStorageImp::setDescriptorOfLastNewView(
    const DescriptorOfLastNewView &desc) {
  setDescriptorOfLastNewView(desc, false);
}

void PersistentStorageImp::initDescriptorOfLastNewView(
    const DescriptorOfLastNewView &desc) {
  setDescriptorOfLastNewView(desc, true);
}

void PersistentStorageImp::saveDescriptorOfLastExecution(
    const DescriptorOfLastExecution &newDesc) {
  const size_t bufLen = DescriptorOfLastExecution::maxSize();
  char *descBuf = new char[bufLen];
  char *descBufPtr = descBuf;
  size_t actualSize = 0;
  newDesc.serialize(descBufPtr, bufLen, actualSize);
  Assert(actualSize);
  metadataStorage_->writeInTransaction(LAST_EXEC_DESC, descBufPtr, actualSize);
  delete[] descBuf;
}

void PersistentStorageImp::setDescriptorOfLastExecution(
    const DescriptorOfLastExecution &desc, bool setAll) {
  if (!setAll) {
    verifyDescriptorOfLastExecution(desc);
    saveDescriptorOfLastExecution(desc);
  }
  descriptorOfLastExecution_ = desc;
}

void PersistentStorageImp::initDescriptorOfLastExecution(
    const DescriptorOfLastExecution &desc) {
  setDescriptorOfLastExecution(desc, true);
}

void PersistentStorageImp::setDescriptorOfLastExecution(
    const DescriptorOfLastExecution &desc) {
  setDescriptorOfLastExecution(desc, false);
}

/***** Windows handling *****/

void PersistentStorageImp::setSeqNumDataElement(
    SeqNum index, char *&buf) const {
  size_t actualSize = 0;
  seqNumWindow_.serializeElement(index, buf, SeqNumWindow::maxElementSize(),
                                 actualSize);
  Assert(actualSize != 0);
  metadataStorage_->writeInTransaction(
      SEQ_NUM_WINDOW + 1 + index, buf, actualSize);
}

void PersistentStorageImp::setSeqNumWindow() const {
  const size_t winBufLen = SeqNumWindow::maxSize();
  char *winBuf = new char[winBufLen];
  char *winBufPtr = winBuf;
  size_t simpleParamsSize = SeqNumWindow::simpleParamsSize();
  seqNumWindow_.serializeSimpleParams(winBufPtr, simpleParamsSize);
  metadataStorage_->writeInTransaction(
      SEQ_NUM_WINDOW, winBufPtr, simpleParamsSize);

  for (uint32_t i = seqNumWindowFirst_; i < seqNumWindowLast_; ++i)
    setSeqNumDataElement(i, winBufPtr);
  delete[] winBuf;
}

void PersistentStorageImp::setCheckDataElement(SeqNum index, char *&buf) const {
  size_t actualSize = 0;
  checkWindow_.serializeElement(
      index, buf, CheckWindow::maxElementSize(), actualSize);
  Assert(actualSize != 0);
  metadataStorage_->writeInTransaction(
      CHECK_WINDOW + 1 + index, buf, actualSize);
}

void PersistentStorageImp::setCheckDataElement(SeqNum seqNum) const {
  size_t bufLen = CheckWindow::maxElementSize();
  char *buf = new char[bufLen];
  setCheckDataElement(seqNum, buf);
  delete[] buf;
}

void PersistentStorageImp::setSeqNumDataElement(
    SeqNum seqNum) const {
  size_t bufLen = SeqNumWindow::maxElementSize();
  char *buf = new char[bufLen];
  setSeqNumDataElement(seqNum, buf);
  delete[] buf;
}

void PersistentStorageImp::setCheckWindow() const {
  const size_t winBufLen = CheckWindow::maxSize();
  char *winBuf = new char[winBufLen];
  char *winBufPtr = winBuf;
  size_t simpleParamsSize = CheckWindow::simpleParamsSize();
  checkWindow_.serializeSimpleParams(winBufPtr, simpleParamsSize);
  metadataStorage_->writeInTransaction(
      CHECK_WINDOW, winBufPtr, simpleParamsSize);

  for (uint32_t i = checkWindowFirst_; i < checkWindowLast_; ++i)
    setCheckDataElement(i, winBufPtr);
  delete[] winBuf;
}

void PersistentStorageImp::setLastStableSeqNumInternal(const SeqNum &seqNum) {
  lastStableSeqNum_ = seqNum;
  metadataStorage_->writeInTransaction(LAST_STABLE_SEQ_NUM,
                                       (char *) &seqNum, sizeof(seqNum));
  seqNumWindow_.advanceActiveWindow(lastStableSeqNum_ + 1);
  checkWindow_.advanceActiveWindow(lastStableSeqNum_);
  setSeqNumWindow();
  setCheckWindow();
}

void PersistentStorageImp::setLastStableSeqNum(const SeqNum &seqNum) {
  Assert(seqNum >= lastStableSeqNum_);
  setLastStableSeqNumInternal(seqNum);
}

void PersistentStorageImp::clearSeqNumWindow() {
  SeqNum s = seqNumWindow_.currentActiveWindow().first;
  Assert(s == lastStableSeqNum_ + 1);
  seqNumWindow_.resetAll(s);
  setSeqNumWindow();
}

void PersistentStorageImp::setPrePrepareMsgInSeqNumWindow(
    const SeqNum &seqNum, const PrePrepareMsg *const &msg) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  Assert(seqNumData.prePrepareMsg == nullptr);
  seqNumData.prePrepareMsg = (PrePrepareMsg *) msg->cloneObjAndMsg();
  setSeqNumDataElement(seqNum);
}

void PersistentStorageImp::setSlowStartedInSeqNumWindow(
    const SeqNum &seqNum, const bool &slowStarted) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  seqNumData.slowStarted = slowStarted;
  setSeqNumDataElement(seqNum);
}

void PersistentStorageImp::setFullCommitProofMsgInSeqNumWindow(
    const SeqNum &seqNum, const FullCommitProofMsg *const &msg) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  Assert(seqNumData.fullCommitProofMsg == nullptr);
  seqNumData.fullCommitProofMsg = (FullCommitProofMsg *) msg->cloneObjAndMsg();
  setSeqNumDataElement(seqNum);
}

void PersistentStorageImp::setForceCompletedInSeqNumWindow(
    const SeqNum &seqNum, const bool &forceCompleted) {
  Assert(forceCompleted);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  seqNumData.forceCompleted = forceCompleted;
  setSeqNumDataElement(seqNum);
}

void PersistentStorageImp::setPrepareFullMsgInSeqNumWindow(
    const SeqNum &seqNum, const PrepareFullMsg *const &msg) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  Assert(seqNumData.prepareFullMsg == nullptr);
  seqNumData.prepareFullMsg = (PrepareFullMsg *) msg->cloneObjAndMsg();
  setSeqNumDataElement(seqNum);
}

void PersistentStorageImp::setCommitFullMsgInSeqNumWindow(
    const SeqNum &seqNum, const CommitFullMsg *const &msg) {
  Assert(seqNumWindow_.insideActiveWindow(seqNum));
  SeqNumData &seqNumData = seqNumWindow_.get(seqNum);
  Assert(seqNumData.commitFullMsg == nullptr);
  seqNumData.commitFullMsg = (CommitFullMsg *) msg->cloneObjAndMsg();
  setSeqNumDataElement(seqNum);
}

void PersistentStorageImp::setCheckpointMsgInCheckWindow(
    const SeqNum &seqNum, const CheckpointMsg *const &msg) {
  Assert(checkWindow_.insideActiveWindow(seqNum));
  CheckData &checkData = checkWindow_.get(seqNum);
  delete checkData.checkpointMsg;
  checkData.checkpointMsg = (CheckpointMsg *) msg->cloneObjAndMsg();
  setCheckDataElement(seqNum);
}

void PersistentStorageImp::setCompletedMarkInCheckWindow(
    const SeqNum &seqNum, const bool &completed) {
  Assert(completed);
  Assert(checkWindow_.insideActiveWindow(seqNum));
  CheckData &checkData = checkWindow_.get(seqNum);
  checkData.completedMark = completed;
  setCheckDataElement(seqNum);
}

/***** Getters *****/

ReplicaConfig PersistentStorageImp::getReplicaConfig() {
  Assert(getIsAllowed());
  uint32_t outActualObjectSize = 0;
  UniquePtrToChar
      outBuf(new char[ReplicaConfigSerializer::maxSize(numOfReplicas_)]);
  metadataStorage_->read(
      REPLICA_CONFIG, ReplicaConfigSerializer::maxSize(numOfReplicas_),
      outBuf.get(), outActualObjectSize);
  UniquePtrToClass replicaConfigPtr =
      Serializable::deserialize(outBuf, outActualObjectSize);
  auto *configPtr = dynamic_cast<ReplicaConfig *>(replicaConfigPtr.get());
  if (!configSerializer_)
    configSerializer_ = new ReplicaConfigSerializer(*configPtr);
  else
    configSerializer_->setConfig(*configPtr);
  return *configPtr;
}

bool PersistentStorageImp::getFetchingState() {
  Assert(getIsAllowed());
  uint32_t outActualObjectSize = 0;
  metadataStorage_->read(FETCHING_STATE, sizeof(fetchingState_),
                         (char *) &fetchingState_, outActualObjectSize);
  Assert(outActualObjectSize == sizeof(fetchingState_));
  return fetchingState_;
}

SeqNum PersistentStorageImp::getLastExecutedSeqNum() {
  Assert(getIsAllowed());
  lastExecutedSeqNum_ = getSeqNum(LAST_EXEC_SEQ_NUM,
                                  sizeof(lastExecutedSeqNum_));
  return lastExecutedSeqNum_;
}

SeqNum PersistentStorageImp::getPrimaryLastUsedSeqNum() {
  Assert(getIsAllowed());
  primaryLastUsedSeqNum_ = getSeqNum(PRIMARY_LAST_USED_SEQ_NUM,
                                     sizeof(primaryLastUsedSeqNum_));
  return primaryLastUsedSeqNum_;
}

SeqNum PersistentStorageImp::getStrictLowerBoundOfSeqNums() {
  Assert(getIsAllowed());
  strictLowerBoundOfSeqNums_ = getSeqNum(LOWER_BOUND_OF_SEQ_NUM,
                                         sizeof(strictLowerBoundOfSeqNums_));
  return strictLowerBoundOfSeqNums_;
}

SeqNum PersistentStorageImp::getLastStableSeqNum() {
  Assert(getIsAllowed());
  lastStableSeqNum_ = getSeqNum(LAST_STABLE_SEQ_NUM, sizeof(lastStableSeqNum_));
  return lastStableSeqNum_;
}

ViewNum
PersistentStorageImp::getLastViewThatTransferredSeqNumbersFullyExecuted() {
  Assert(getIsAllowed());
  lastViewTransferredSeqNum_ = getSeqNum(LAST_VIEW_TRANSFERRED_SEQ_NUM,
                                         sizeof(lastViewTransferredSeqNum_));
  return lastViewTransferredSeqNum_;
}

/***** Descriptors handling *****/

DescriptorOfLastExitFromView
PersistentStorageImp::getAndAllocateDescriptorOfLastExitFromView() {
  Assert(getIsAllowed());
  DescriptorOfLastExitFromView dbDesc;
  size_t simpleParamsSize = DescriptorOfLastExitFromView::simpleParamsSize();
  uint32_t sizeInDb = 0;

  // Read first simple params.
  UniquePtrToChar simpleParamsBuf(new char[simpleParamsSize]);
  metadataStorage_->read(LAST_EXIT_FROM_VIEW_DESC, simpleParamsSize,
                         simpleParamsBuf.get(), sizeInDb);
  Assert(sizeInDb == simpleParamsSize);
  uint32_t actualSize = 0;
  dbDesc.deserializeSimpleParams(simpleParamsBuf.get(), simpleParamsSize,
                                 actualSize);

  size_t maxElementSize = DescriptorOfLastExitFromView::maxElementSize();
  UniquePtrToChar elementBuf(new char[maxElementSize]);
  uint32_t actualElementSize = 0;
  uint32_t elementsNum = dbDesc.elements.capacity();

  for (uint32_t i = 0; i < elementsNum; ++i) {
    metadataStorage_->read(LAST_EXIT_FROM_VIEW_DESC + 1 + i, maxElementSize,
                           elementBuf.get(), actualSize);
    dbDesc.deserializeElement(elementBuf.get(), actualSize, actualElementSize);
    Assert(actualElementSize != 0);
  }

  descriptorOfLastExitFromView_ = dbDesc;
  return descriptorOfLastExitFromView_;
}

DescriptorOfLastNewView
PersistentStorageImp::getAndAllocateDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  DescriptorOfLastNewView dbDesc;
  size_t simpleParamsSize = DescriptorOfLastNewView::simpleParamsSize();
  uint32_t actualSize = 0;

  // Read first simple params.
  UniquePtrToChar simpleParamsBuf(new char[simpleParamsSize]);
  metadataStorage_->read(LAST_NEW_VIEW_DESC, simpleParamsSize,
                         simpleParamsBuf.get(), actualSize);
  dbDesc.deserializeSimpleParams(simpleParamsBuf.get(), simpleParamsSize,
                                 actualSize);

  size_t maxElementSize = DescriptorOfLastNewView::maxElementSize();
  UniquePtrToChar elementBuf(new char[maxElementSize]);
  size_t actualElementSize = 0;
  for (uint32_t i = 0; i < numOfReplicas_; ++i) {
    metadataStorage_->read(LAST_NEW_VIEW_DESC + 1 + i, maxElementSize,
                           elementBuf.get(), actualSize);
    dbDesc.deserializeElement(elementBuf.get(), actualSize, actualElementSize);
    Assert(actualElementSize != 0);
  }

  descriptorOfLastNewView_ = dbDesc;
  return descriptorOfLastNewView_;
}

DescriptorOfLastExecution PersistentStorageImp::getDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  DescriptorOfLastExecution dbDesc;
  size_t maxSize = DescriptorOfLastExecution::maxSize();
  uint32_t actualSize = 0;

  UniquePtrToChar buf(new char[maxSize]);
  metadataStorage_->read(LAST_EXEC_DESC, maxSize, buf.get(), actualSize);
  dbDesc.deserialize(buf.get(), maxSize, actualSize);
  Assert(actualSize != 0);

  descriptorOfLastExecution_ = dbDesc;
  return descriptorOfLastExecution_;
}

bool PersistentStorageImp::hasDescriptorOfLastExitFromView() {
  Assert(getIsAllowed());
  getAndAllocateDescriptorOfLastExitFromView();
  return (!descriptorOfLastExitFromView_.isEmpty());
}

bool PersistentStorageImp::hasDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  getAndAllocateDescriptorOfLastNewView();
  return (!descriptorOfLastNewView_.isEmpty());
}

bool PersistentStorageImp::hasDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  getDescriptorOfLastExecution();
  return (!descriptorOfLastExecution_.isEmpty());
}

/***** Windows handling *****/

void PersistentStorageImp::getSeqNumDataElement(SeqNum index, char *&buf) {
  uint32_t actualElementSize = 0;
  metadataStorage_->read(SEQ_NUM_WINDOW + 1 + index,
                         SeqNumWindow::maxElementSize(), buf,
                         actualElementSize);
  uint32_t actualSize = 0;
  seqNumWindow_.deserializeElement(index, buf, actualElementSize, actualSize);
  Assert(actualSize == actualElementSize);
}

void PersistentStorageImp::getSeqNumWindow() {
  const size_t winBufLen = SeqNumWindow::maxSize();
  char *winBuf = new char[winBufLen];
  size_t simpleParamsSize = SeqNumWindow::simpleParamsSize();

  uint32_t actualSize = 0;
  metadataStorage_->read(SEQ_NUM_WINDOW, simpleParamsSize, winBuf, actualSize);
  seqNumWindow_.deserializeSimpleParams(winBuf, simpleParamsSize, actualSize);
  Assert(actualSize == simpleParamsSize);

  for (uint32_t i = seqNumWindowFirst_; i < seqNumWindowLast_; ++i)
    getSeqNumDataElement(i, winBuf);
  delete[] winBuf;
}

void PersistentStorageImp::getCheckDataElement(SeqNum index, char *&buf) {
  uint32_t actualElementSize = 0;
  metadataStorage_->read(CHECK_WINDOW + 1 + index,
                         CheckWindow::maxElementSize(), buf,
                         actualElementSize);
  uint32_t actualSize = 0;
  checkWindow_.deserializeElement(index, buf, actualElementSize, actualSize);
  Assert(actualSize == actualElementSize);
}

void PersistentStorageImp::getCheckWindow() {
  const size_t winBufLen = CheckWindow::maxSize();
  char *winBuf = new char[winBufLen];
  size_t simpleParamsSize = CheckWindow::simpleParamsSize();

  uint32_t actualSize = 0;
  metadataStorage_->read(CHECK_WINDOW, simpleParamsSize, winBuf, actualSize);
  checkWindow_.deserializeSimpleParams(winBuf, simpleParamsSize, actualSize);
  Assert(actualSize == simpleParamsSize);

  uint32_t windowSize = checkWindow_.getNumItems();
  for (uint32_t i = 0; i < windowSize; ++i)
    getCheckDataElement(i, winBuf);
  delete[] winBuf;
}

void PersistentStorageImp::getCheckDataElement(SeqNum seqNum) {
  size_t bufLen = CheckWindow::maxElementSize();
  char *buf = new char[bufLen];
  getCheckDataElement(seqNum, buf);
  delete[] buf;
}

void PersistentStorageImp::getSeqNumDataElement(SeqNum seqNum) {
  size_t bufLen = SeqNumWindow::maxElementSize();
  char *buf = new char[bufLen];
  getSeqNumDataElement(seqNum, buf);
  delete[] buf;
}

PrePrepareMsg *PersistentStorageImp::getAndAllocatePrePrepareMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));

  getSeqNumDataElement(seqNum);
  return seqNumWindow_.get(seqNum).prePrepareMsg;
}

bool PersistentStorageImp::getSlowStartedInSeqNumWindow(const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));

  getSeqNumDataElement(seqNum);
  return seqNumWindow_.get(seqNum).slowStarted;
}

FullCommitProofMsg *
PersistentStorageImp::getAndAllocateFullCommitProofMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));

  getSeqNumDataElement(seqNum);
  return seqNumWindow_.get(seqNum).fullCommitProofMsg;
}

bool PersistentStorageImp::getForceCompletedInSeqNumWindow(const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));

  getSeqNumDataElement(seqNum);
  return seqNumWindow_.get(seqNum).forceCompleted;
}

PrepareFullMsg *
PersistentStorageImp::getAndAllocatePrepareFullMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));

  getSeqNumDataElement(seqNum);
  return seqNumWindow_.get(seqNum).prepareFullMsg;
}

CommitFullMsg *PersistentStorageImp::getAndAllocateCommitFullMsgInSeqNumWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ + 1 == seqNumWindow_.currentActiveWindow().first);
  Assert(seqNumWindow_.insideActiveWindow(seqNum));

  getSeqNumDataElement(seqNum);
  return seqNumWindow_.get(seqNum).commitFullMsg;
}

CheckpointMsg *PersistentStorageImp::getAndAllocateCheckpointMsgInCheckWindow(
    const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow_.currentActiveWindow().first);
  Assert(checkWindow_.insideActiveWindow(seqNum));

  getCheckDataElement(seqNum);
  return checkWindow_.get(seqNum).checkpointMsg;
}

bool PersistentStorageImp::getCompletedMarkInCheckWindow(const SeqNum &seqNum) {
  Assert(getIsAllowed());
  Assert(lastStableSeqNum_ == checkWindow_.currentActiveWindow().first);
  Assert(checkWindow_.insideActiveWindow(seqNum));

  getCheckDataElement(seqNum);
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
    const DescriptorOfLastExitFromView &desc) const {
  for (auto elem : desc.elements) {
    Assert(elem.prePrepare->seqNumber() >= lastStableSeqNum_ + 1);
    Assert(elem.prePrepare->seqNumber() <= lastStableSeqNum_ + kWorkWindowSize);
    Assert(elem.prePrepare->viewNumber() == desc.view);
    Assert(elem.prepareFull == nullptr ||
        elem.prepareFull->viewNumber() == desc.view);
    Assert(elem.prepareFull == nullptr ||
        elem.prepareFull->seqNumber() == elem.prePrepare->seqNumber());
  }
}

void PersistentStorageImp::verifyLastNewViewMsgs(
    const DescriptorOfLastNewView &desc) const {
  for (size_t i = 0; i < desc.viewChangeMsgs.size(); i++) {
    const ViewChangeMsg *vc = desc.viewChangeMsgs[i];
    Assert(vc->newView() == desc.view);

    Digest digestOfVCMsg;
    vc->getMsgDigest(digestOfVCMsg);
    Assert(desc.newViewMsg->includesViewChangeFromReplica(
        vc->idOfGeneratedReplica(), digestOfVCMsg));

    auto *clonedVC = (ViewChangeMsg *) vc->cloneObjAndMsg();
    Assert(clonedVC->type() == MsgCode::ViewChange);
  }
}

void PersistentStorageImp::verifySetDescriptorOfLastNewView(
    const DescriptorOfLastNewView &desc) const {
  Assert(nonExecSetIsAllowed());
  Assert(desc.view >= 1);
  Assert(!descriptorOfLastExitFromView_.isEmpty());
  Assert(desc.view > descriptorOfLastExitFromView_.view);
  Assert(desc.newViewMsg->newView() == desc.view);
  Assert(hasReplicaConfig());
  const size_t numOfVCMsgs = 2 * configSerializer_->getConfig()->fVal +
      2 * configSerializer_->getConfig()->cVal + 1;
  Assert(desc.viewChangeMsgs.size() == numOfVCMsgs);
}

void PersistentStorageImp::verifyDescriptorOfLastExecution(
    const DescriptorOfLastExecution &desc) const {
  Assert(setIsAllowed());
  Assert(descriptorOfLastExecution_.isEmpty() ||
      descriptorOfLastExecution_.executedSeqNum < desc.executedSeqNum);
  Assert(lastExecutedSeqNum_ + 1 == desc.executedSeqNum);
  Assert(desc.validRequests.numOfBits() >= 1);
  Assert(desc.validRequests.numOfBits() <= maxNumOfRequestsInBatch);
}

// Helper function for getting different kinds of sequence numbers.
SeqNum PersistentStorageImp::getSeqNum(
    ConstMetadataParameterIds id, uint32_t size) {
  uint32_t actualObjectSize = 0;
  SeqNum seqNum = 0;
  metadataStorage_->read(id, size, (char *) &seqNum, actualObjectSize);
  Assert(actualObjectSize == size);
  return seqNum;
}

bool PersistentStorageImp::hasReplicaConfig() const {
  return (configSerializer_ && configSerializer_->getConfig());
}

bool PersistentStorageImp::setIsAllowed() const {
  return (isInWriteTran() && configSerializer_ &&
      configSerializer_->getConfig());
}

bool PersistentStorageImp::getIsAllowed() const {
  return (!isInWriteTran() && configSerializer_ &&
      configSerializer_->getConfig());
}

bool PersistentStorageImp::nonExecSetIsAllowed() const {
  return setIsAllowed() && (descriptorOfLastExecution_.isEmpty() ||
      descriptorOfLastExecution_.executedSeqNum <= lastExecutedSeqNum_);
}

}  // namespace impl
}  // namespace bftEngine
