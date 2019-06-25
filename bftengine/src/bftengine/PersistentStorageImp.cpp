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
      version_(METADATA_PARAMS_VERSION), seqNumWindow_{seqNumWindowFirst_},
      checkWindow_{checkWindowFirst_} {
  DescriptorOfLastNewView::setViewChangeMsgsNum(fVal, cVal);
  configSerializer_ = new ReplicaConfigSerializer(nullptr); // ReplicaConfig placeholder
}

PersistentStorageImp::~PersistentStorageImp() {
  delete configSerializer_;
}

void PersistentStorageImp::init(MetadataStorage *&metadataStorage) {
  metadataStorage_ = metadataStorage;
  try {
    if (!getStoredVersion().empty())
      return;
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
  setLastStableSeqNumInternal(lastStableSeqNum_);

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
    metadataObjectsArray.get()[SEQ_NUM_WINDOW + 1 + i].maxSize = SeqNumWindow::maxElementSize();
  }

  uint32_t viewChangeMsgsNum = DescriptorOfLastNewView::getViewChangeMsgsNum();
  for (uint32_t i = 0; i < viewChangeMsgsNum; ++i) {
    metadataObjectsArray.get()[LAST_NEW_VIEW_DESC + 1 + i].maxSize = DescriptorOfLastNewView::maxElementSize();
  }

  for (auto i = 0; i < checkWinSize; ++i)
    metadataObjectsArray.get()[CHECK_WINDOW + 1 + i].maxSize = CheckWindow::maxElementSize();

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
  uint32_t sizeOfVersion = version_.size();
  uint32_t sizeOfSizeOfVersion = sizeof(sizeOfVersion);
  int64_t outBufSize = sizeOfVersion + sizeOfSizeOfVersion;
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

void PersistentStorageImp::setSeqNumDataElement(SeqNum index, char *buf) const {
  size_t actualSize = 0;
  seqNumWindow_.serializeElement(index, buf, SeqNumWindow::maxElementSize(), actualSize);
  Assert(actualSize != 0);
  metadataStorage_->writeInTransaction(SEQ_NUM_WINDOW + 1 + index, buf, actualSize);
}

void PersistentStorageImp::serializeAndSaveSeqNumWindow() const {
  size_t simpleParamsSize = SeqNumWindow::simpleParamsSize();
  char *simpleParamsBuf = new char[simpleParamsSize];
  seqNumWindow_.serializeSimpleParams(simpleParamsBuf, simpleParamsSize);
  metadataStorage_->writeInTransaction(SEQ_NUM_WINDOW, simpleParamsBuf, simpleParamsSize);
  delete[] simpleParamsBuf;

  char *winBuf = new char[SeqNumWindow::maxElementSize()];
  for (uint32_t i = seqNumWindowFirst_; i < seqNumWindowLast_; ++i)
    setSeqNumDataElement(i, winBuf);
  delete[] winBuf;
}

void PersistentStorageImp::setSeqNumDataElementBySeqNum(SeqNum seqNum) const {
  size_t bufLen = SeqNumWindow::maxElementSize();
  char *buf = new char[bufLen];
  setSeqNumDataElement(seqNum, buf);
  delete[] buf;
}

void PersistentStorageImp::serializeAndSaveCheckWindow() const {
  const size_t simpleParamsSize = CheckWindow::simpleParamsSize();
  char *simpleParamsBuf = new char[simpleParamsSize];
  checkWindow_.serializeSimpleParams(simpleParamsBuf, simpleParamsSize);
  metadataStorage_->writeInTransaction(CHECK_WINDOW, simpleParamsBuf, simpleParamsSize);
  delete[] simpleParamsBuf;

  char *winBuf = new char[CheckWindow::maxElementSize()];
  for (uint32_t i = checkWindowFirst_; i < checkWindowLast_; ++i)
    setCheckDataElement(i, winBuf);
  delete[] winBuf;
}

void PersistentStorageImp::setCheckDataElement(SeqNum index, char *buf) const {
  size_t actualSize = 0;
  checkWindow_.serializeElement(index, buf, CheckWindow::maxElementSize(), actualSize);
  Assert(actualSize != 0);
  metadataStorage_->writeInTransaction(CHECK_WINDOW + 1 + index, buf, actualSize);
}

void PersistentStorageImp::setCheckDataElementBySeqNum(SeqNum seqNum) const {
  size_t bufLen = CheckWindow::maxElementSize();
  char *buf = new char[bufLen];
  setCheckDataElement(seqNum, buf);
  delete[] buf;
}

void PersistentStorageImp::setLastStableSeqNumInternal(const SeqNum &seqNum) {
  metadataStorage_->writeInTransaction(LAST_STABLE_SEQ_NUM, (char *) &seqNum, sizeof(seqNum));
  seqNumWindow_.advanceActiveWindow(seqNum + 1);
  checkWindow_.advanceActiveWindow(seqNum);
  serializeAndSaveSeqNumWindow();
  serializeAndSaveCheckWindow();
}

/***** Public functions *****/

void PersistentStorageImp::clearSeqNumWindow() {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  SeqNum s = seqNumWindow_.currentActiveWindow().first;
  seqNumWindow_.resetAll(s);
  serializeAndSaveSeqNumWindow();
}

void PersistentStorageImp::setLastStableSeqNum(const SeqNum seqNum) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  if (!checkWindowReadFromDisk_)
    getCheckWindow();
  setLastStableSeqNumInternal(seqNum);
}

void PersistentStorageImp::setPrePrepareMsgInSeqNumWindow(const SeqNum seqNum, const PrePrepareMsg *const msg) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  const SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  SeqNumData &seqNumData = seqNumWindow_.get(convertedIndex);
  seqNumData.setPrePrepareMsg((PrePrepareMsg *) msg->cloneObjAndMsg());
  setSeqNumDataElementBySeqNum(convertedIndex);
}

void PersistentStorageImp::setSlowStartedInSeqNumWindow(const SeqNum seqNum, const bool slowStarted) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  const SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  SeqNumData &seqNumData = seqNumWindow_.get(convertedIndex);
  seqNumData.setSlowStarted(slowStarted);
  setSeqNumDataElementBySeqNum(convertedIndex);
}

void PersistentStorageImp::setFullCommitProofMsgInSeqNumWindow(
    const SeqNum seqNum, const FullCommitProofMsg *const msg) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  const SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  SeqNumData &seqNumData = seqNumWindow_.get(convertedIndex);
  seqNumData.setFullCommitProofMsg((FullCommitProofMsg *) msg->cloneObjAndMsg());
  setSeqNumDataElementBySeqNum(convertedIndex);
}

void PersistentStorageImp::setForceCompletedInSeqNumWindow(const SeqNum seqNum, const bool forceCompleted) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  Assert(forceCompleted);
  const SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  SeqNumData &seqNumData = seqNumWindow_.get(convertedIndex);
  seqNumData.setForceCompleted(forceCompleted);
  setSeqNumDataElementBySeqNum(convertedIndex);
}

void PersistentStorageImp::setPrepareFullMsgInSeqNumWindow(const SeqNum seqNum, const PrepareFullMsg *const msg) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  const SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  SeqNumData &seqNumData = seqNumWindow_.get(convertedIndex);
  seqNumData.setPrepareFullMsg((PrepareFullMsg *) msg->cloneObjAndMsg());
  setSeqNumDataElementBySeqNum(convertedIndex);
}

void PersistentStorageImp::setCommitFullMsgInSeqNumWindow(const SeqNum seqNum, const CommitFullMsg *const msg) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  const SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  SeqNumData &seqNumData = seqNumWindow_.get(convertedIndex);
  seqNumData.setCommitFullMsg((CommitFullMsg *) msg->cloneObjAndMsg());
  setSeqNumDataElementBySeqNum(convertedIndex);
}

void PersistentStorageImp::setCheckpointMsgInCheckWindow(const SeqNum seqNum, const CheckpointMsg *const msg) {
  if (!checkWindowReadFromDisk_)
    getCheckWindow();
  const SeqNum convertedIndex = checkWindow_.convertIndex(seqNum);
  CheckData &checkData = checkWindow_.get(convertedIndex);
  checkData.setCheckpointMsg((CheckpointMsg *) msg->cloneObjAndMsg());
  setCheckDataElementBySeqNum(convertedIndex);
}

void PersistentStorageImp::setCompletedMarkInCheckWindow(const SeqNum seqNum, const bool completed) {
  if (!checkWindowReadFromDisk_)
    getCheckWindow();
  Assert(completed);
  const SeqNum convertedIndex = checkWindow_.convertIndex(seqNum);
  CheckData &checkData = checkWindow_.get(convertedIndex);
  checkData.setCompletedMark(completed);
  setCheckDataElementBySeqNum(convertedIndex);
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
  auto *configPtr = ((ReplicaConfigSerializer *)(replicaConfigPtr.get()))->getConfig();
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
  size_t simpleParamsSize = DescriptorOfLastExitFromView::simpleParamsSize();
  uint32_t sizeInDb = 0;

  // Read first simple params.
  UniquePtrToChar simpleParamsBuf(new char[simpleParamsSize]);
  metadataStorage_->read(LAST_EXIT_FROM_VIEW_DESC, simpleParamsSize, simpleParamsBuf.get(), sizeInDb);
  Assert(sizeInDb == simpleParamsSize);
  uint32_t actualSize = 0;
  dbDesc.deserializeSimpleParams(simpleParamsBuf.get(), simpleParamsSize, actualSize);

  size_t maxElementSize = DescriptorOfLastExitFromView::maxElementSize();
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
  size_t simpleParamsSize = DescriptorOfLastNewView::simpleParamsSize();
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
  size_t maxSize = DescriptorOfLastExecution::maxSize();
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

void PersistentStorageImp::getSeqNumDataElement(SeqNum index, char *buf) {
  uint32_t actualElementSize = 0;
  metadataStorage_->read(SEQ_NUM_WINDOW + 1 + index, SeqNumWindow::maxElementSize(), buf, actualElementSize);
  uint32_t actualSize = 0;
  seqNumWindow_.deserializeElement(index, buf, actualElementSize, actualSize);
  Assert(actualSize == actualElementSize);
}

SeqNumWindow &PersistentStorageImp::getSeqNumWindow() {
  if (seqNumWindowReadFromDisk_)
    return seqNumWindow_;

  size_t paramsSize = SeqNumWindow::simpleParamsSize();
  char *paramsBuf = new char[paramsSize];

  uint32_t actualSize = 0;
  metadataStorage_->read(SEQ_NUM_WINDOW, paramsSize, paramsBuf, actualSize);
  seqNumWindow_.deserializeSimpleParams(paramsBuf, paramsSize, actualSize);
  Assert(actualSize == paramsSize);
  delete[] paramsBuf;

  char *winBuf = new char[SeqNumWindow::maxElementSize()];
  for (uint32_t i = seqNumWindowFirst_; i < seqNumWindowLast_; ++i)
    getSeqNumDataElement(i, winBuf);
  delete[] winBuf;
  seqNumWindowReadFromDisk_ = true;
  return seqNumWindow_;
}

void PersistentStorageImp::getCheckDataElement(SeqNum index, char *buf) {
  uint32_t actualElementSize = 0;
  metadataStorage_->read(CHECK_WINDOW + 1 + index, CheckWindow::maxElementSize(), buf, actualElementSize);
  uint32_t actualSize = 0;
  checkWindow_.deserializeElement(index, buf, actualElementSize, actualSize);
  Assert(actualSize == actualElementSize);
}

void PersistentStorageImp::getCheckDataElementBySeqNum(SeqNum seqNum) {
  size_t bufLen = CheckWindow::maxElementSize();
  char *buf = new char[bufLen];
  SeqNum convertedIndex = checkWindow_.convertIndex(seqNum);
  getCheckDataElement(convertedIndex, buf);
  delete[] buf;
}

void PersistentStorageImp::getSeqNumDataElementBySeqNum(SeqNum seqNum) {
  size_t bufLen = SeqNumWindow::maxElementSize();
  char *buf = new char[bufLen];
  SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  getSeqNumDataElement(convertedIndex, buf);
  delete[] buf;
}

/***** Public functions *****/

CheckWindow &PersistentStorageImp::getCheckWindow() {
  if (checkWindowReadFromDisk_)
    return checkWindow_;

  size_t paramsSize = CheckWindow::simpleParamsSize();
  char *paramsBuf = new char[paramsSize];

  uint32_t actualSize = 0;
  metadataStorage_->read(CHECK_WINDOW, paramsSize, paramsBuf, actualSize);
  checkWindow_.deserializeSimpleParams(paramsBuf, paramsSize, actualSize);
  Assert(actualSize == paramsSize);
  delete[] paramsBuf;

  char *winBuf = new char[CheckWindow::maxElementSize()];
  for (uint32_t i = checkWindowFirst_; i < checkWindowLast_; ++i)
    getCheckDataElement(i, winBuf);
  delete[] winBuf;
  checkWindowReadFromDisk_ = true;
  return checkWindow_;
}

CheckpointMsg *PersistentStorageImp::getAndAllocateCheckpointMsgInCheckWindow(const SeqNum seqNum) {
  if (!checkWindowReadFromDisk_)
    getCheckWindow();
  Assert(getIsAllowed());
  SeqNum convertedIndex = checkWindow_.convertIndex(seqNum);
  return (CheckpointMsg *)checkWindow_.get(convertedIndex).getCheckpointMsg()->cloneObjAndMsg();
}

bool PersistentStorageImp::getCompletedMarkInCheckWindow(const SeqNum seqNum) {
  if (!checkWindowReadFromDisk_)
    getCheckWindow();
  Assert(getIsAllowed());
  SeqNum convertedIndex = checkWindow_.convertIndex(seqNum);
  return checkWindow_.get(convertedIndex).getCompletedMark();
}

bool PersistentStorageImp::getSlowStartedInSeqNumWindow(const SeqNum seqNum) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  Assert(getIsAllowed());
  SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  return seqNumWindow_.get(convertedIndex).getSlowStarted();
}

bool PersistentStorageImp::getForceCompletedInSeqNumWindow(const SeqNum seqNum) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  Assert(getIsAllowed());
  SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  return seqNumWindow_.get(convertedIndex).getForceCompleted();
}

PrePrepareMsg *PersistentStorageImp::getAndAllocatePrePrepareMsgInSeqNumWindow(const SeqNum seqNum) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  Assert(getIsAllowed());
  SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  return (PrePrepareMsg *) seqNumWindow_.get(convertedIndex).getPrePrepareMsg()->cloneObjAndMsg();
}

FullCommitProofMsg *PersistentStorageImp::getAndAllocateFullCommitProofMsgInSeqNumWindow(const SeqNum seqNum) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  Assert(getIsAllowed());
  SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  return (FullCommitProofMsg *)seqNumWindow_.get(convertedIndex).getFullCommitProofMsg()->cloneObjAndMsg();
}

PrepareFullMsg *PersistentStorageImp::getAndAllocatePrepareFullMsgInSeqNumWindow(const SeqNum seqNum) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  Assert(getIsAllowed());
  SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  return (PrepareFullMsg *)seqNumWindow_.get(convertedIndex).getPrepareFullMsg()->cloneObjAndMsg();
}

CommitFullMsg *PersistentStorageImp::getAndAllocateCommitFullMsgInSeqNumWindow(const SeqNum seqNum) {
  if (!seqNumWindowReadFromDisk_)
    getSeqNumWindow();
  Assert(getIsAllowed());
  SeqNum convertedIndex = seqNumWindow_.convertIndex(seqNum);
  return (CommitFullMsg *)seqNumWindow_.get(convertedIndex).getCommitFullMsg()->cloneObjAndMsg();
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
