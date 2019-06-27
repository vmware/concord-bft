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

#include "PersistentStorageImp.hpp"
#include <sstream>

using namespace std;
using namespace concordSerializable;

namespace bftEngine {
namespace impl {

const string METADATA_PARAMS_VERSION = "1.1";
const uint16_t MAX_METADATA_PARAMS_NUM = 10000;

PersistentStorageImp::PersistentStorageImp(uint16_t fVal, uint16_t cVal)
    : fVal_(fVal), cVal_(cVal), numOfReplicas_(3 * fVal + 2 * cVal + 1),
      version_(METADATA_PARAMS_VERSION) {
  DescriptorOfLastNewView::setViewChangeMsgsNum(fVal, cVal);
  configSerializer_ = new ReplicaConfigSerializer(nullptr); // ReplicaConfig placeholder
}

PersistentStorageImp::~PersistentStorageImp() {
  delete configSerializer_;
}

void PersistentStorageImp::retrieveWindowsMetadata() {
  SeqNum seqNum = 0;
  const size_t sizeofSeqNum = sizeof(seqNum);
  uint32_t actualSize = 0;
  char buf[sizeofSeqNum];
  metadataStorage_->read(SEQ_NUM_WINDOW, sizeofSeqNum, buf, actualSize);
  seqNumWindowBeginning_ = SeqNumWindow::deserializeActiveWindowBeginning(buf);
  metadataStorage_->read(CHECK_WINDOW, sizeofSeqNum, buf, actualSize);
  checkWindowBeginning_ = CheckWindow::deserializeActiveWindowBeginning(buf);
}

void PersistentStorageImp::init(MetadataStorage *&metadataStorage) {
  metadataStorage_ = metadataStorage;
  try {
    if (!getStoredVersion().empty()) {
      retrieveWindowsMetadata();
      return;
    }
  } catch (const runtime_error &exc) {}
  initMetadataStorage(metadataStorage);
  // DB is not populated yet with default metadata parameter values.
  setDefaultsInMetadataStorage();
}

void PersistentStorageImp::setDefaultsInMetadataStorage() {
  beginWriteTran();

  setVersion();
  setFetchingStateInternal(fetchingState_);
  setLastExecutedSeqNumInternal(lastExecutedSeqNum_);
  setPrimaryLastUsedSeqNumInternal(primaryLastUsedSeqNum_);
  setStrictLowerBoundOfSeqNumsInternal(strictLowerBoundOfSeqNums_);
  setLastViewTransferredSeqNumbersInternal(lastViewTransferredSeqNum_);

  SharedPtrSeqNumWindow seqNumWindow(new SeqNumWindow(seqNumWindowFirst_));
  SharedPtrCheckWindow checkWindow(new CheckWindow(checkWindowFirst_));
  setLastStableSeqNumInternal(lastStableSeqNum_, seqNumWindow, checkWindow);

  initDescriptorOfLastExitFromView();
  initDescriptorOfLastNewView();
  initDescriptorOfLastExecution();

  endWriteTran();
}

void PersistentStorageImp::initMetadataStorage(MetadataStorage *&metadataStorage) {
  unique_ptr<MetadataStorage::ObjectDesc> metadataObjectsArray(
      new MetadataStorage::ObjectDesc[MAX_METADATA_PARAMS_NUM]);

  for (auto i = 0; i < MAX_METADATA_PARAMS_NUM; ++i) {
    metadataObjectsArray.get()[i].id = i;
    metadataObjectsArray.get()[i].maxSize = 0;
  }

  metadataObjectsArray.get()[VERSION_PARAMETER].maxSize = maxVersionSize_;
  metadataObjectsArray.get()[FETCHING_STATE].maxSize = sizeof(fetchingState_);
  metadataObjectsArray.get()[LAST_EXEC_SEQ_NUM].maxSize = sizeof(lastExecutedSeqNum_);
  metadataObjectsArray.get()[PRIMARY_LAST_USED_SEQ_NUM].maxSize = sizeof(primaryLastUsedSeqNum_);
  metadataObjectsArray.get()[LOWER_BOUND_OF_SEQ_NUM].maxSize = sizeof(strictLowerBoundOfSeqNums_);
  metadataObjectsArray.get()[LAST_VIEW_TRANSFERRED_SEQ_NUM].maxSize = sizeof(lastViewTransferredSeqNum_);
  metadataObjectsArray.get()[LAST_STABLE_SEQ_NUM].maxSize = sizeof(lastStableSeqNum_);

  metadataObjectsArray.get()[REPLICA_CONFIG].maxSize = ReplicaConfigSerializer::maxSize(numOfReplicas_);

  metadataObjectsArray.get()[SEQ_NUM_WINDOW].maxSize = SeqNumWindow::simpleParamsSize();
  metadataObjectsArray.get()[CHECK_WINDOW].maxSize = CheckWindow::simpleParamsSize();

  for (auto i = 0; i < kWorkWindowSize; ++i) {
    metadataObjectsArray.get()[LAST_EXIT_FROM_VIEW_DESC + 1 + i].maxSize =
        DescriptorOfLastExitFromView::maxElementSize();
  }

  for (auto i = 0; i < kWorkWindowSize * numOfSeqNumWinParameters; ++i) {
    metadataObjectsArray.get()[SEQ_NUM_WINDOW + PRE_PREPARE_MSG + i].maxSize = SeqNumWindow::maxElementSize();
  }

  uint32_t viewChangeMsgsNum = DescriptorOfLastNewView::getViewChangeMsgsNum();
  for (uint32_t i = 0; i < viewChangeMsgsNum; ++i) {
    metadataObjectsArray.get()[LAST_NEW_VIEW_DESC + 1 + i].maxSize = DescriptorOfLastNewView::maxElementSize();
  }

  for (auto i = 0; i < checkWinSize * numOfCheckWinParameters; ++i)
    metadataObjectsArray.get()[CHECK_WINDOW + COMPLETED_MARK + i].maxSize = CheckWindow::maxElementSize();

  metadataObjectsArray.get()[LAST_EXIT_FROM_VIEW_DESC].maxSize = DescriptorOfLastExitFromView::simpleParamsSize();
  metadataObjectsArray.get()[LAST_EXEC_DESC].maxSize = DescriptorOfLastExecution::maxSize();
  metadataObjectsArray.get()[LAST_NEW_VIEW_DESC].maxSize = DescriptorOfLastNewView::simpleParamsSize();

  metadataStorage_->initMaxSizeOfObjects(metadataObjectsArray.get(), MAX_METADATA_PARAMS_NUM);
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
  UniquePtrToChar outBuf;
  int64_t outBufSize = ReplicaConfigSerializer::maxSize(numOfReplicas_);
  configSerializer_->setConfig(config);
  configSerializer_->serialize(outBuf, outBufSize);
  metadataStorage_->writeInTransaction(REPLICA_CONFIG, (char *) outBuf.get(), outBufSize);
}

void PersistentStorageImp::setVersion() const {
  Assert(isInWriteTran());
  const uint32_t sizeOfVersion = version_.size();
  const uint32_t sizeOfSizeOfVersion = sizeof(sizeOfVersion);
  const int64_t outBufSize = sizeOfVersion + sizeOfSizeOfVersion;
  char *outBuf = new char[outBufSize];
  char *outBufPtr = outBuf;

  memcpy(outBufPtr, &sizeOfVersion, sizeOfSizeOfVersion);
  outBufPtr += sizeOfSizeOfVersion;
  memcpy(outBufPtr, version_.c_str(), sizeOfVersion);

  metadataStorage_->writeInTransaction(VERSION_PARAMETER, outBuf, outBufSize);
  delete[] outBuf;
}

void PersistentStorageImp::setFetchingStateInternal(const bool &state) {
  metadataStorage_->writeInTransaction(FETCHING_STATE, (char *) &state, sizeof(state));
}

void PersistentStorageImp::setFetchingState(const bool state) {
  Assert(!nonExecSetIsAllowed());
  Assert(state);
  setFetchingStateInternal(state);
}

void PersistentStorageImp::setLastExecutedSeqNumInternal(const SeqNum &seqNum) {
  metadataStorage_->writeInTransaction(LAST_EXEC_SEQ_NUM, (char *) &seqNum, sizeof(seqNum));
}

void PersistentStorageImp::setLastExecutedSeqNum(const SeqNum seqNum) {
  Assert(setIsAllowed());
  setLastExecutedSeqNumInternal(seqNum);
}

void PersistentStorageImp::setPrimaryLastUsedSeqNumInternal(const SeqNum &seqNum) {
  metadataStorage_->writeInTransaction(PRIMARY_LAST_USED_SEQ_NUM, (char *) &seqNum, sizeof(seqNum));
}

void PersistentStorageImp::setPrimaryLastUsedSeqNum(const SeqNum seqNum) {
  Assert(!nonExecSetIsAllowed());
  setPrimaryLastUsedSeqNumInternal(seqNum);
}

void PersistentStorageImp::setStrictLowerBoundOfSeqNumsInternal(const SeqNum &seqNum) {
  metadataStorage_->writeInTransaction(LOWER_BOUND_OF_SEQ_NUM, (char *) &seqNum, sizeof(seqNum));
}

void PersistentStorageImp::setStrictLowerBoundOfSeqNums(const SeqNum seqNum) {
  Assert(!nonExecSetIsAllowed());
  setStrictLowerBoundOfSeqNumsInternal(seqNum);
}

void PersistentStorageImp::setLastViewTransferredSeqNumbersInternal(const ViewNum &view) {
  metadataStorage_->writeInTransaction(LAST_VIEW_TRANSFERRED_SEQ_NUM, (char *) &view, sizeof(view));
}

void PersistentStorageImp::setLastViewThatTransferredSeqNumbersFullyExecuted(const ViewNum view) {
  Assert(!nonExecSetIsAllowed());
  setLastViewTransferredSeqNumbersInternal(view);
}

/***** Descriptors handling *****/

void PersistentStorageImp::saveDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &newDesc) {
  const size_t simpleParamsSize = DescriptorOfLastExitFromView::simpleParamsSize();
  char *simpleParamsBuf = new char[simpleParamsSize];
  newDesc.serializeSimpleParams(simpleParamsBuf, simpleParamsSize);
  metadataStorage_->writeInTransaction(LAST_EXIT_FROM_VIEW_DESC, simpleParamsBuf, simpleParamsSize);
  delete[] simpleParamsBuf;

  size_t actualElementSize = 0;
  uint32_t elementsNum = newDesc.elements.size();
  uint32_t maxElementSize = DescriptorOfLastExitFromView::maxElementSize();
  char *elementBuf = new char[maxElementSize];
  for (size_t i = 0; i < elementsNum; ++i) {
    newDesc.serializeElement(i, elementBuf, maxElementSize, actualElementSize);
    Assert(actualElementSize != 0);
    metadataStorage_->writeInTransaction(LAST_EXIT_FROM_VIEW_DESC + 1 + i, elementBuf, actualElementSize);
  }
  delete[] elementBuf;
}

void PersistentStorageImp::setDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &desc, bool init) {
  if (!init) {
    verifySetDescriptorOfLastExitFromView(desc);
    verifyPrevViewInfo(desc);
  }
  saveDescriptorOfLastExitFromView(desc);
}

void PersistentStorageImp::setDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &desc) {
  setDescriptorOfLastExitFromView(desc, false);
  hasDescriptorOfLastExitFromView_ = true;
}

void PersistentStorageImp::initDescriptorOfLastExitFromView() {
  DescriptorOfLastExitFromView desc;
  setDescriptorOfLastExitFromView(desc, true);
}

void PersistentStorageImp::saveDescriptorOfLastNewView(const DescriptorOfLastNewView &newDesc) {
  const size_t simpleParamsSize = DescriptorOfLastNewView::simpleParamsSize();
  char *simpleParamsBuf = new char[simpleParamsSize];
  size_t actualSize = 0;
  newDesc.serializeSimpleParams(simpleParamsBuf, simpleParamsSize, actualSize);
  metadataStorage_->writeInTransaction(LAST_NEW_VIEW_DESC, simpleParamsBuf, actualSize);
  delete[] simpleParamsBuf;

  size_t actualElementSize = 0;
  uint32_t numOfMessages = DescriptorOfLastNewView::getViewChangeMsgsNum();
  uint32_t maxElementSize = DescriptorOfLastNewView::maxElementSize();
  char *elementBuf = new char[maxElementSize];
  for (uint32_t i = 0; i < numOfMessages; ++i) {
    newDesc.serializeElement(i, elementBuf, maxElementSize, actualElementSize);
    Assert(actualElementSize != 0);
    metadataStorage_->writeInTransaction(LAST_NEW_VIEW_DESC + 1 + i, elementBuf, actualElementSize);
  }
  delete[] elementBuf;
}

void PersistentStorageImp::setDescriptorOfLastNewView(const DescriptorOfLastNewView &desc, bool init) {
  if (!init) {
    verifySetDescriptorOfLastNewView(desc);
    verifyLastNewViewMsgs(desc);
  }
  saveDescriptorOfLastNewView(desc);
}

void PersistentStorageImp::setDescriptorOfLastNewView(const DescriptorOfLastNewView &desc) {
  setDescriptorOfLastNewView(desc, false);
  hasDescriptorOfLastNewView_ = true;
}

void PersistentStorageImp::initDescriptorOfLastNewView() {
  DescriptorOfLastNewView desc;
  setDescriptorOfLastNewView(desc, true);
}

void PersistentStorageImp::saveDescriptorOfLastExecution(const DescriptorOfLastExecution &newDesc) {
  const size_t bufLen = DescriptorOfLastExecution::maxSize();
  char *descBuf = new char[bufLen];
  char *descBufPtr = descBuf;
  size_t actualSize = 0;
  newDesc.serialize(descBufPtr, bufLen, actualSize);
  Assert(actualSize != 0);
  metadataStorage_->writeInTransaction(LAST_EXEC_DESC, descBuf, actualSize);
  delete[] descBuf;
}

void PersistentStorageImp::setDescriptorOfLastExecution(const DescriptorOfLastExecution &desc, bool init) {
  if (!init)
    verifyDescriptorOfLastExecution(desc);
  saveDescriptorOfLastExecution(desc);
}

void PersistentStorageImp::initDescriptorOfLastExecution() {
  DescriptorOfLastExecution desc;
  setDescriptorOfLastExecution(desc, true);
}

void PersistentStorageImp::setDescriptorOfLastExecution(const DescriptorOfLastExecution &desc) {
  setDescriptorOfLastExecution(desc, false);
  hasDescriptorOfLastExecution_ = true;
}

/***** Windows handling *****/

/***** Private functions *****/

void PersistentStorageImp::serializeAndSaveSeqNumWindow(SharedPtrSeqNumWindow seqNumWindow) {
  const size_t simpleParamsSize = sizeof(SeqNum);
  char simpleParamsBuf[simpleParamsSize];
  seqNumWindow.get()->serializeActiveWindowBeginning(simpleParamsBuf);
  metadataStorage_->writeInTransaction(SEQ_NUM_WINDOW, simpleParamsBuf, simpleParamsSize);

  char *winBuf = new char[SeqNumData::maxSize()];
  for (uint32_t i = seqNumWindowFirst_; i < seqNumWindowLast_; ++i)
    setSeqNumDataElement(i, winBuf, seqNumWindow);
  delete[] winBuf;
}

void PersistentStorageImp::setSeqNumDataElement(SeqNum index, char *buf, SharedPtrSeqNumWindow seqNumWindow) const {
  SeqNumData &seqNumData = seqNumWindow.get()->getByRealIndex(index);
  SeqNum shift = (index - 1) * numOfSeqNumWinParameters;
  char *movablePtr = buf;
  size_t actualSize = seqNumData.serializePrePrepareMsg(movablePtr);
  metadataStorage_->writeInTransaction(SEQ_NUM_WINDOW + PRE_PREPARE_MSG + shift, buf, actualSize);
  Assert(actualSize != 0);

  movablePtr = buf;
  actualSize = seqNumData.serializeFullCommitProofMsg(movablePtr);
  metadataStorage_->writeInTransaction(SEQ_NUM_WINDOW + FULL_COMMIT_PROOF_MSG + shift, buf, actualSize);
  Assert(actualSize != 0);

  movablePtr = buf;
  actualSize = seqNumData.serializePrepareFullMsg(movablePtr);
  metadataStorage_->writeInTransaction(SEQ_NUM_WINDOW + PRE_PREPARE_FULL_MSG + shift, buf, actualSize);
  Assert(actualSize != 0);

  movablePtr = buf;
  actualSize = seqNumData.serializeCommitFullMsg(movablePtr);
  metadataStorage_->writeInTransaction(SEQ_NUM_WINDOW + COMMIT_FULL_MSG + shift, buf, actualSize);
  Assert(actualSize != 0);

  movablePtr = buf;
  actualSize = seqNumData.serializeForceCompleted(movablePtr);
  metadataStorage_->writeInTransaction(SEQ_NUM_WINDOW + FORCE_COMPLETED + shift, buf, actualSize);

  movablePtr = buf;
  actualSize = seqNumData.serializeSlowStarted(movablePtr);
  metadataStorage_->writeInTransaction(SEQ_NUM_WINDOW + SLOW_STARTED + shift, buf, actualSize);
}

void PersistentStorageImp::serializeAndSaveCheckWindow(SharedPtrCheckWindow checkWindow) {
  const size_t simpleParamsSize = sizeof(SeqNum);
  char simpleParamsBuf[simpleParamsSize];
  checkWindow.get()->serializeActiveWindowBeginning(simpleParamsBuf);
  metadataStorage_->writeInTransaction(CHECK_WINDOW, simpleParamsBuf, simpleParamsSize);

  char *winBuf = new char[CheckData::maxSize()];
  for (uint32_t i = checkWindowFirst_; i < checkWindowLast_; ++i)
    setCheckDataElement(i, winBuf, checkWindow);
  delete[] winBuf;
}

void PersistentStorageImp::setCheckDataElement(SeqNum index, char *buf, SharedPtrCheckWindow checkWindow) const {
  CheckData &checkData = checkWindow.get()->getByRealIndex(index);
  char *movablePtr = buf;
  size_t actualSize = checkData.serializeCompletedMark(movablePtr);
  SeqNum shift = index * numOfCheckWinParameters;
  metadataStorage_->writeInTransaction(CHECK_WINDOW + COMPLETED_MARK + shift, buf, actualSize);
  movablePtr = buf;
  actualSize = checkData.serializeCheckpointMsg(movablePtr);
  Assert(actualSize != 0);
  metadataStorage_->writeInTransaction(CHECK_WINDOW + CHECKPOINT_MSG + shift, buf, actualSize);
}

void PersistentStorageImp::setLastStableSeqNumInternal(const SeqNum &seqNum, SharedPtrSeqNumWindow seqNumWindow,
                                                       SharedPtrCheckWindow checkWindow) {
  metadataStorage_->writeInTransaction(LAST_STABLE_SEQ_NUM, (char *) &seqNum, sizeof(seqNum));
  seqNumWindow.get()->advanceActiveWindow(seqNum + 1);
  checkWindow.get()->advanceActiveWindow(seqNum);
  serializeAndSaveSeqNumWindow(seqNumWindow);
  serializeAndSaveCheckWindow(checkWindow);
  seqNumWindowBeginning_ = seqNumWindow.get()->getBeginningOfActiveWindow();
  checkWindowBeginning_ = checkWindow.get()->getBeginningOfActiveWindow();
}

/***** Public functions *****/

void PersistentStorageImp::clearSeqNumWindow() {
  SharedPtrSeqNumWindow seqNumWindow(new SeqNumWindow(seqNumWindowFirst_));
  serializeAndSaveSeqNumWindow(seqNumWindow);
}

void PersistentStorageImp::setLastStableSeqNum(const SeqNum seqNum) {
  SharedPtrSeqNumWindow seqNumWindow = getSeqNumWindow();
  SharedPtrCheckWindow checkWindow = getCheckWindow();
  setLastStableSeqNumInternal(seqNum, seqNumWindow, checkWindow);
}

void PersistentStorageImp::setMsgInSeqNumWindow(const SeqNum seqNum, const SeqNum parameterId, MessageBase *msg) const {
  const size_t bufLen = SeqNumData::maxSize();
  char *buf = new char[bufLen];
  char *movablePtr = buf;
  const size_t actualSize = SeqNumData::serializeMsg(movablePtr, msg);
  Assert(actualSize != 0);
  metadataStorage_->writeInTransaction(
      SEQ_NUM_WINDOW + parameterId + SeqNumWindow::convertIndex(seqNum, seqNumWindowBeginning_), buf, actualSize);
  delete[] buf;
}

void PersistentStorageImp::setPrePrepareMsgInSeqNumWindow(const SeqNum seqNum, const PrePrepareMsg *const msg) {
  setMsgInSeqNumWindow(seqNum, PRE_PREPARE_MSG, (MessageBase *) msg);
}

void PersistentStorageImp::setFullCommitProofMsgInSeqNumWindow(
    const SeqNum seqNum, const FullCommitProofMsg *const msg) {
  setMsgInSeqNumWindow(seqNum, FULL_COMMIT_PROOF_MSG, (MessageBase *) msg);
}

void PersistentStorageImp::setPrepareFullMsgInSeqNumWindow(const SeqNum seqNum, const PrepareFullMsg *const msg) {
  setMsgInSeqNumWindow(seqNum, PRE_PREPARE_FULL_MSG, (MessageBase *) msg);
}

void PersistentStorageImp::setCommitFullMsgInSeqNumWindow(const SeqNum seqNum, const CommitFullMsg *const msg) {
  setMsgInSeqNumWindow(seqNum, COMMIT_FULL_MSG, (MessageBase *) msg);
}

void PersistentStorageImp::setBooleanInSeqNumWindow(
    const SeqNum seqNum, const SeqNum parameterId, const bool boolean) const {
  const size_t sizeofBoolean = sizeof(boolean);
  char buf[sizeofBoolean];
  char *movablePtr = buf;
  SeqNumData::serializeBoolean(movablePtr, boolean);
  metadataStorage_->writeInTransaction(
      SEQ_NUM_WINDOW + parameterId + SeqNumWindow::convertIndex(seqNum, seqNumWindowBeginning_), buf, sizeofBoolean);
}

void PersistentStorageImp::setForceCompletedInSeqNumWindow(const SeqNum seqNum, const bool forceCompleted) {
  setBooleanInSeqNumWindow(seqNum, FORCE_COMPLETED, forceCompleted);
}

void PersistentStorageImp::setSlowStartedInSeqNumWindow(const SeqNum seqNum, const bool slowStarted) {
  setBooleanInSeqNumWindow(seqNum, SLOW_STARTED, slowStarted);
}

void PersistentStorageImp::setCompletedMarkInCheckWindow(const SeqNum seqNum, const bool completed) {
  Assert(completed);
  const size_t sizeOfCompleted = sizeof(completed);
  char buf[sizeOfCompleted];
  char *movablePtr = buf;
  CheckData::serializeCompletedMark(movablePtr, completed);
  metadataStorage_->writeInTransaction(
      CHECK_WINDOW + COMPLETED_MARK + CheckWindow::convertIndex(seqNum, checkWindowBeginning_), buf, sizeOfCompleted);
}

void PersistentStorageImp::setCheckpointMsgInCheckWindow(const SeqNum seqNum, const CheckpointMsg *const msg) {
  size_t bufLen = CheckData::maxCheckpointMsgSize();
  char *buf = new char[bufLen];
  char *movablePtr = buf;
  size_t actualSize = CheckData::serializeCheckpointMsg(movablePtr, (CheckpointMsg *) msg);
  Assert(actualSize != 0);
  metadataStorage_->writeInTransaction(
      CHECK_WINDOW + CHECKPOINT_MSG + CheckWindow::convertIndex(seqNum, checkWindowBeginning_), buf, actualSize);
  delete[] buf;
}

/***** Getters *****/

string PersistentStorageImp::getStoredVersion() {
  Assert(!isInWriteTran());
  uint32_t outActualObjectSize = 0;
  char *outBuf = new char[maxVersionSize_];
  char *outBufPtr = outBuf;
  metadataStorage_->read(VERSION_PARAMETER, maxVersionSize_, outBufPtr, outActualObjectSize);
  if (!outActualObjectSize) // Parameter not found
    return string();

  uint32_t sizeOfVersion = 0;
  memcpy(&sizeOfVersion, outBufPtr, sizeof(sizeOfVersion));
  outBufPtr += sizeof(sizeOfVersion);
  string savedVersion;
  savedVersion.assign(outBufPtr, sizeOfVersion);
  Assert(version_ == savedVersion);

  delete[] outBuf;
  return version_;
}

ReplicaConfig PersistentStorageImp::getReplicaConfig() {
  uint32_t outActualObjectSize = 0;
  UniquePtrToChar outBuf(new char[ReplicaConfigSerializer::maxSize(numOfReplicas_)]);
  metadataStorage_->read(REPLICA_CONFIG, ReplicaConfigSerializer::maxSize(numOfReplicas_),
                         outBuf.get(), outActualObjectSize);
  SharedPtrToClass replicaConfigPtr = ReplicaConfigSerializer::deserialize(outBuf, outActualObjectSize);
  auto *configPtr = ((ReplicaConfigSerializer *) (replicaConfigPtr.get()))->getConfig();
  configSerializer_->setConfig(*configPtr);
  return *configPtr;
}

bool PersistentStorageImp::getFetchingState() {
  Assert(getIsAllowed());
  uint32_t outActualObjectSize = 0;
  metadataStorage_->read(FETCHING_STATE, sizeof(fetchingState_), (char *) &fetchingState_, outActualObjectSize);
  Assert(outActualObjectSize == sizeof(fetchingState_));
  return fetchingState_;
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
  return getSeqNum(LAST_VIEW_TRANSFERRED_SEQ_NUM, sizeof(lastViewTransferredSeqNum_));
}

bool PersistentStorageImp::hasReplicaConfig() const {
  return (configSerializer_->getConfig());
}

/***** Descriptors handling *****/

DescriptorOfLastExitFromView
PersistentStorageImp::getAndAllocateDescriptorOfLastExitFromView() {
  Assert(getIsAllowed());
  DescriptorOfLastExitFromView dbDesc;
  const size_t simpleParamsSize = DescriptorOfLastExitFromView::simpleParamsSize();
  uint32_t sizeInDb = 0;

  // Read first simple params.
  UniquePtrToChar simpleParamsBuf(new char[simpleParamsSize]);
  metadataStorage_->read(LAST_EXIT_FROM_VIEW_DESC, simpleParamsSize, simpleParamsBuf.get(), sizeInDb);
  Assert(sizeInDb == simpleParamsSize);
  uint32_t actualSize = 0;
  dbDesc.deserializeSimpleParams(simpleParamsBuf.get(), simpleParamsSize, actualSize);

  const size_t maxElementSize = DescriptorOfLastExitFromView::maxElementSize();
  UniquePtrToChar elementBuf(new char[maxElementSize]);
  uint32_t actualElementSize = 0;
  uint32_t elementsNum = dbDesc.elements.size();
  for (uint32_t i = 0; i < elementsNum; ++i) {
    metadataStorage_->read(LAST_EXIT_FROM_VIEW_DESC + 1 + i, maxElementSize, elementBuf.get(), actualSize);
    dbDesc.deserializeElement(i, elementBuf.get(), actualSize, actualElementSize);
    Assert(actualElementSize != 0);
  }
  return dbDesc;
}

DescriptorOfLastNewView
PersistentStorageImp::getAndAllocateDescriptorOfLastNewView() {
  Assert(getIsAllowed());
  DescriptorOfLastNewView dbDesc;
  const size_t simpleParamsSize = DescriptorOfLastNewView::simpleParamsSize();
  uint32_t actualSize = 0;

  // Read first simple params.
  UniquePtrToChar simpleParamsBuf(new char[simpleParamsSize]);
  metadataStorage_->read(LAST_NEW_VIEW_DESC, simpleParamsSize, simpleParamsBuf.get(), actualSize);
  dbDesc.deserializeSimpleParams(simpleParamsBuf.get(), simpleParamsSize, actualSize);

  size_t maxElementSize = DescriptorOfLastNewView::maxElementSize();
  UniquePtrToChar elementBuf(new char[maxElementSize]);
  size_t actualElementSize = 0;
  uint32_t viewChangeMsgsNum = DescriptorOfLastNewView::getViewChangeMsgsNum();
  for (uint32_t i = 0; i < viewChangeMsgsNum; ++i) {
    metadataStorage_->read(LAST_NEW_VIEW_DESC + 1 + i, maxElementSize, elementBuf.get(), actualSize);
    dbDesc.deserializeElement(i, elementBuf.get(), actualSize, actualElementSize);
    Assert(actualElementSize != 0);
  }
  return dbDesc;
}

DescriptorOfLastExecution PersistentStorageImp::getDescriptorOfLastExecution() {
  Assert(getIsAllowed());
  DescriptorOfLastExecution dbDesc;
  const size_t maxSize = DescriptorOfLastExecution::maxSize();
  uint32_t actualSize = 0;

  UniquePtrToChar buf(new char[maxSize]);
  metadataStorage_->read(LAST_EXEC_DESC, maxSize, buf.get(), actualSize);
  dbDesc.deserialize(buf.get(), maxSize, actualSize);
  Assert(actualSize != 0);
  return dbDesc;
}

bool PersistentStorageImp::hasDescriptorOfLastExitFromView() {
  if (!hasDescriptorOfLastExitFromView_) {
    DescriptorOfLastExitFromView defaultDesc;
    DescriptorOfLastExitFromView storedDesc = getAndAllocateDescriptorOfLastExitFromView();
    if (!storedDesc.equals(defaultDesc))
      hasDescriptorOfLastExitFromView_ = true;
    storedDesc.clean();
    defaultDesc.clean();
  }
  return hasDescriptorOfLastExitFromView_;
}

bool PersistentStorageImp::hasDescriptorOfLastNewView() {
  if (!hasDescriptorOfLastNewView_) {
    DescriptorOfLastNewView defaultDesc;
    DescriptorOfLastNewView storedDesc = getAndAllocateDescriptorOfLastNewView();
    if (!storedDesc.equals(defaultDesc))
      hasDescriptorOfLastNewView_ = true;
    storedDesc.clean();
    defaultDesc.clean();
  }
  return hasDescriptorOfLastNewView_;
}

bool PersistentStorageImp::hasDescriptorOfLastExecution() {
  if (!hasDescriptorOfLastExecution_) {
    DescriptorOfLastExecution defaultDesc;
    DescriptorOfLastExecution storedDesc = getDescriptorOfLastExecution();
    if (!storedDesc.equals(defaultDesc))
      hasDescriptorOfLastExecution_ = true;
  }
  return hasDescriptorOfLastExecution_;
}

/***** Windows handling *****/

/***** Private functions *****/

void PersistentStorageImp::readSeqNumDataElementFromDisk(SeqNum index, char *buf, SharedPtrSeqNumWindow seqNumWindow) {
  uint32_t actualElementSize = 0;
  uint32_t actualParameterSize = 0;
  char *movablePtr = buf;
  SeqNum shift = (index - 1) * numOfSeqNumWinParameters;
  for (auto i = 0; i < numOfSeqNumWinParameters; ++i) {
    metadataStorage_->read(SEQ_NUM_WINDOW + PRE_PREPARE_MSG + i + shift, SeqNumData::maxSize(),
                           movablePtr, actualParameterSize);
    movablePtr += actualParameterSize;
    actualElementSize += actualParameterSize;
  }
  uint32_t actualSize = 0;
  seqNumWindow.get()->deserializeElement(index, buf, actualElementSize, actualSize);
}

void PersistentStorageImp::readCheckDataElementFromDisk(SeqNum index, char *buf, SharedPtrCheckWindow checkWindow) {
  uint32_t actualElementSize = 0;
  uint32_t actualParameterSize = 0;
  char *movablePtr = buf;
  const SeqNum shift = index * numOfCheckWinParameters;
  for (auto i = 0; i < numOfCheckWinParameters; ++i) {
    metadataStorage_->read(CHECK_WINDOW + COMPLETED_MARK + i + shift, CheckData::maxSize(),
                           movablePtr, actualParameterSize);
    movablePtr += actualParameterSize;
    actualElementSize += actualParameterSize;
  }
  uint32_t actualSize = 0;
  checkWindow.get()->deserializeElement(index, buf, CheckData::maxSize(), actualSize);
  Assert(actualSize == actualElementSize);
}

bool PersistentStorageImp::readBooleanFromDisk(SeqNum seqNum, SeqNum parameterId) const {
  bool boolean = false;
  uint32_t actualSize = 0;
  metadataStorage_->read(SEQ_NUM_WINDOW + parameterId + SeqNumWindow::convertIndex(seqNum, seqNumWindowBeginning_),
                         sizeof(boolean), (char *) &boolean, actualSize);
  return boolean;
}

MessageBase *PersistentStorageImp::readMsgFromDisk(SeqNum seqNum, SeqNum parameterId) const {
  const size_t bufLen = SeqNumData::maxSize();
  char *buf = new char[bufLen];
  uint32_t actualMsgSize = 0;
  metadataStorage_->read(SEQ_NUM_WINDOW + parameterId + SeqNumWindow::convertIndex(seqNum, seqNumWindowBeginning_),
                         bufLen, buf, actualMsgSize);
  size_t actualSize = 0;
  char *movablePtr = buf;
  auto *msg = SeqNumData::deserializeMsg(movablePtr, bufLen, actualSize);
  Assert(actualSize == actualMsgSize);

  delete[] buf;
  return msg;
}

PrePrepareMsg *PersistentStorageImp::readPrePrepareMsgFromDisk(SeqNum seqNum) const {
  return (PrePrepareMsg *) readMsgFromDisk(seqNum, PRE_PREPARE_MSG);
}

FullCommitProofMsg *PersistentStorageImp::readFullCommitProofMsgFromDisk(SeqNum seqNum) const {
  return (FullCommitProofMsg *) readMsgFromDisk(seqNum, FULL_COMMIT_PROOF_MSG);
}

PrepareFullMsg *PersistentStorageImp::readPrepareFullMsgFromDisk(SeqNum seqNum) const {
  return (PrepareFullMsg *) readMsgFromDisk(seqNum, PRE_PREPARE_FULL_MSG);
}

CommitFullMsg *PersistentStorageImp::readCommitFullMsgFromDisk(SeqNum seqNum) const {
  return (CommitFullMsg *) readMsgFromDisk(seqNum, COMMIT_FULL_MSG);
}

bool PersistentStorageImp::readCompletedMarkFromDisk(SeqNum seqNum) const {
  bool completedMark = false;
  CheckWindow checkWindow(checkWindowFirst_);
  uint32_t actualSize = 0;
  metadataStorage_->read(CHECK_WINDOW + COMPLETED_MARK + CheckWindow::convertIndex(seqNum, checkWindowBeginning_),
                         sizeof(completedMark), (char *) &completedMark, actualSize);
  return completedMark;
}

CheckpointMsg *PersistentStorageImp::readCheckpointMsgFromDisk(SeqNum seqNum) const {
  const size_t bufLen = CheckData::maxSize();
  char *buf = new char[bufLen];
  uint32_t actualMsgSize = 0;
  CheckWindow checkWindow(checkWindowFirst_);
  metadataStorage_->read(CHECK_WINDOW + CHECKPOINT_MSG + CheckWindow::convertIndex(seqNum, checkWindowBeginning_),
                         bufLen, buf, actualMsgSize);
  size_t actualSize = 0;
  auto *checkpointMsg = CheckData::deserializeCheckpointMsg(buf, bufLen, actualSize);
  Assert(actualSize == actualMsgSize);

  delete[] buf;
  return checkpointMsg;
}

/***** Public functions *****/

SharedPtrSeqNumWindow PersistentStorageImp::getSeqNumWindow() {
  const size_t paramsSize = SeqNumWindow::simpleParamsSize();
  char *paramsBuf = new char[paramsSize];

  uint32_t actualSize = 0;
  metadataStorage_->read(SEQ_NUM_WINDOW, paramsSize, paramsBuf, actualSize);
  SharedPtrSeqNumWindow seqNumWindow(new SeqNumWindow(seqNumWindowFirst_));
  seqNumWindow.get()->deserializeBeginningOfActiveWindow(paramsBuf);
  Assert(actualSize == paramsSize);
  delete[] paramsBuf;

  char *winBuf = new char[SeqNumWindow::maxElementSize()];
  for (uint32_t i = seqNumWindowFirst_; i < seqNumWindowLast_; ++i)
    readSeqNumDataElementFromDisk(i, winBuf, seqNumWindow);
  delete[] winBuf;
  return seqNumWindow;
}

SharedPtrCheckWindow PersistentStorageImp::getCheckWindow() {
  const size_t paramsSize = CheckWindow::simpleParamsSize();
  char *paramsBuf = new char[paramsSize];

  uint32_t actualSize = 0;
  metadataStorage_->read(CHECK_WINDOW, paramsSize, paramsBuf, actualSize);
  SharedPtrCheckWindow checkWindow(new CheckWindow(checkWindowFirst_));
  checkWindow.get()->deserializeBeginningOfActiveWindow(paramsBuf);
  Assert(actualSize == paramsSize);
  delete[] paramsBuf;

  char *winBuf = new char[CheckData::maxSize()];
  for (uint32_t i = checkWindowFirst_; i < checkWindowLast_; ++i)
    readCheckDataElementFromDisk(i, winBuf, checkWindow);
  delete[] winBuf;
  return checkWindow;
}

CheckpointMsg *PersistentStorageImp::getAndAllocateCheckpointMsgInCheckWindow(const SeqNum seqNum) {
  Assert(getIsAllowed());
  return readCheckpointMsgFromDisk(seqNum);
}

bool PersistentStorageImp::getCompletedMarkInCheckWindow(const SeqNum seqNum) {
  Assert(getIsAllowed());
  return readCompletedMarkFromDisk(seqNum);
}

PrePrepareMsg *PersistentStorageImp::getAndAllocatePrePrepareMsgInSeqNumWindow(const SeqNum seqNum) {
  Assert(getIsAllowed());
  return readPrePrepareMsgFromDisk(seqNum);
}

FullCommitProofMsg *PersistentStorageImp::getAndAllocateFullCommitProofMsgInSeqNumWindow(const SeqNum seqNum) {
  Assert(getIsAllowed());
  return readFullCommitProofMsgFromDisk(seqNum);
}

PrepareFullMsg *PersistentStorageImp::getAndAllocatePrepareFullMsgInSeqNumWindow(const SeqNum seqNum) {
  Assert(getIsAllowed());
  return readPrepareFullMsgFromDisk(seqNum);
}

CommitFullMsg *PersistentStorageImp::getAndAllocateCommitFullMsgInSeqNumWindow(const SeqNum seqNum) {
  Assert(getIsAllowed());
  return readCommitFullMsgFromDisk(seqNum);
}

bool PersistentStorageImp::getForceCompletedInSeqNumWindow(const SeqNum seqNum) {
  Assert(getIsAllowed());
  return readBooleanFromDisk(seqNum, FORCE_COMPLETED);
}

bool PersistentStorageImp::getSlowStartedInSeqNumWindow(const SeqNum seqNum) {
  Assert(getIsAllowed());
  return readBooleanFromDisk(seqNum, SLOW_STARTED);
}

/***** Verification/helper functions *****/

void PersistentStorageImp::verifySetDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &desc) const {
  Assert(setIsAllowed());
  Assert(desc.view >= 0);
  // Here we assume that the first view is always (even if we load the initial state from the disk).
  Assert(hasDescriptorOfLastExecution_ || desc.view == 0);
  Assert(desc.elements.size() <= kWorkWindowSize);
}

void PersistentStorageImp::verifyPrevViewInfo(const DescriptorOfLastExitFromView &desc) const {
  for (auto elem : desc.elements) {
    Assert(elem.prePrepare->viewNumber() == desc.view);
    Assert(elem.prepareFull == nullptr || elem.prepareFull->viewNumber() == desc.view);
    Assert(elem.prepareFull == nullptr || elem.prepareFull->seqNumber() == elem.prePrepare->seqNumber());
  }
}

void PersistentStorageImp::verifyLastNewViewMsgs(const DescriptorOfLastNewView &desc) const {
  for (uint32_t i = 0; i < desc.viewChangeMsgs.size(); i++) {
    const ViewChangeMsg *viewChangeMsg = desc.viewChangeMsgs[i];
    Assert(viewChangeMsg->newView() == desc.view);
    Digest digestOfViewChangeMsg;
    viewChangeMsg->getMsgDigest(digestOfViewChangeMsg);
    const NewViewMsg *newViewMsg = desc.newViewMsg;
    if (newViewMsg->elementsCount()) Assert(newViewMsg->elementsCount() &&
        newViewMsg->includesViewChangeFromReplica(viewChangeMsg->idOfGeneratedReplica(), digestOfViewChangeMsg));
  }
}

void PersistentStorageImp::verifySetDescriptorOfLastNewView(const DescriptorOfLastNewView &desc) const {
  Assert(setIsAllowed());
  Assert(desc.view >= 1);
  Assert(hasDescriptorOfLastExitFromView_);
  Assert(desc.newViewMsg->newView() == desc.view);
  Assert(hasReplicaConfig());
  const size_t numOfVCMsgs = 2 * fVal_ + 2 * cVal_ + 1;
  Assert(desc.viewChangeMsgs.size() == numOfVCMsgs);
}

void PersistentStorageImp::verifyDescriptorOfLastExecution(const DescriptorOfLastExecution &desc) const {
  Assert(setIsAllowed());
  Assert(desc.validRequests.numOfBits() >= 1);
  Assert(desc.validRequests.numOfBits() <= maxNumOfRequestsInBatch);
}

// Helper function for getting different kinds of sequence numbers.
SeqNum PersistentStorageImp::getSeqNum(ConstMetadataParameterIds id, uint32_t size) {
  uint32_t actualObjectSize = 0;
  SeqNum seqNum = 0;
  metadataStorage_->read(id, size, (char *) &seqNum, actualObjectSize);
  Assert(actualObjectSize == size);
  return seqNum;
}

bool PersistentStorageImp::setIsAllowed() const {
  return (isInWriteTran() && configSerializer_->getConfig());
}

bool PersistentStorageImp::getIsAllowed() const {
  return (!isInWriteTran() && configSerializer_->getConfig());
}

bool PersistentStorageImp::nonExecSetIsAllowed() const {
  return setIsAllowed() && !hasDescriptorOfLastExecution_;
}

}  // namespace impl
}  // namespace bftEngine
