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
  seqNumWindowBeginning_ = readBeginningOfActiveWindow(BEGINNING_OF_SEQ_NUM_WINDOW);
  checkWindowBeginning_ = readBeginningOfActiveWindow(BEGINNING_OF_CHECK_WINDOW);
}

bool PersistentStorageImp::init(MetadataStorage *&metadataStorage) {
  metadataStorage_ = metadataStorage;
  try {
    if (!getStoredVersion().empty()) {
      retrieveWindowsMetadata();
      return false;
    }
  } catch (const runtime_error &exc) {}
  // DB is not populated yet with default metadata parameter values.
  setDefaultsInMetadataStorage();
  return true;
}

void PersistentStorageImp::setDefaultsInMetadataStorage() {
  beginWriteTran();

  setVersion();
  setFetchingStateInternal(fetchingState_);
  setLastExecutedSeqNumInternal(lastExecutedSeqNum_);
  setPrimaryLastUsedSeqNumInternal(primaryLastUsedSeqNum_);
  setStrictLowerBoundOfSeqNumsInternal(strictLowerBoundOfSeqNums_);
  setLastViewTransferredSeqNumbersInternal(lastViewTransferredSeqNum_);

  metadataStorage_->writeInTransaction(LAST_STABLE_SEQ_NUM, (char *) &lastStableSeqNum_, sizeof(lastStableSeqNum_));
  setDefaultWindowsValues();

  initDescriptorOfLastExitFromView();
  initDescriptorOfLastNewView();
  initDescriptorOfLastExecution();

  endWriteTran();
}

// This function is used by an external code to initialize MetadataStorage and enable StateTransfer using the same DB.
ObjectDescUniquePtr PersistentStorageImp::getDefaultMetadataObjectDescriptors(uint16_t &numOfObjects) const {
  numOfObjects = MAX_METADATA_PARAMS_NUM;
  ObjectDescUniquePtr metadataObjectsArray(new MetadataStorage::ObjectDesc[MAX_METADATA_PARAMS_NUM]);

  for (uint16_t i = FIRST_METADATA_PARAMETER; i < MAX_METADATA_PARAMS_NUM; ++i) {
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

  metadataObjectsArray.get()[BEGINNING_OF_SEQ_NUM_WINDOW].maxSize = sizeof(SeqNum);
  metadataObjectsArray.get()[BEGINNING_OF_CHECK_WINDOW].maxSize = sizeof(SeqNum);

  for (auto i = 0; i < kWorkWindowSize; ++i) {
    metadataObjectsArray.get()[LAST_EXIT_FROM_VIEW_DESC + 1 + i].maxSize =
        DescriptorOfLastExitFromView::maxElementSize();
  }

  for (auto i = 0; i < kWorkWindowSize * numOfSeqNumWinParameters; ++i) {
    metadataObjectsArray.get()[BEGINNING_OF_SEQ_NUM_WINDOW + SEQ_NUM_FIRST_PARAM + i].maxSize =
        SeqNumWindow::maxElementSize();
  }

  uint32_t viewChangeMsgsNum = DescriptorOfLastNewView::getViewChangeMsgsNum();
  for (uint32_t i = 0; i < viewChangeMsgsNum; ++i) {
    metadataObjectsArray.get()[LAST_NEW_VIEW_DESC + 1 + i].maxSize = DescriptorOfLastNewView::maxElementSize();
  }

  for (auto i = 0; i < checkWinSize * numOfCheckWinParameters; ++i)
    metadataObjectsArray.get()[BEGINNING_OF_CHECK_WINDOW + CHECK_DATA_FIRST_PARAM + i].maxSize =
        CheckWindow::maxElementSize();

  metadataObjectsArray.get()[LAST_EXIT_FROM_VIEW_DESC].maxSize = DescriptorOfLastExitFromView::simpleParamsSize();
  metadataObjectsArray.get()[LAST_EXEC_DESC].maxSize = DescriptorOfLastExecution::maxSize();
  metadataObjectsArray.get()[LAST_NEW_VIEW_DESC].maxSize = DescriptorOfLastNewView::simpleParamsSize();

  return metadataObjectsArray;
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

  size_t actualSize = 0;
  newDesc.serializeSimpleParams(simpleParamsBuf, simpleParamsSize, actualSize);
  metadataStorage_->writeInTransaction(LAST_EXIT_FROM_VIEW_DESC, simpleParamsBuf, simpleParamsSize);
  delete[] simpleParamsBuf;

  size_t actualElementSize = 0;
  uint32_t elementsNum = newDesc.elements.size();
  uint32_t maxElementSize = DescriptorOfLastExitFromView::maxElementSize();
  char *elementBuf = new char[maxElementSize];
  for (size_t i = 0; i < elementsNum; ++i) {
    newDesc.serializeElement(i, elementBuf, maxElementSize, actualElementSize);
    Assert(actualElementSize != 0);
    uint32_t itemId = LAST_EXIT_FROM_VIEW_DESC + 1 + i;
    Assert(itemId < LAST_EXEC_DESC);
    metadataStorage_->writeInTransaction(itemId, elementBuf, actualElementSize);
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

void PersistentStorageImp::serializeAndSaveSeqNumWindow(const SharedPtrSeqNumWindow &seqNumWindow) {
  writeBeginningOfActiveWindow(BEGINNING_OF_SEQ_NUM_WINDOW, seqNumWindow.get()->getBeginningOfActiveWindow());
  for (uint32_t i = seqNumWindowFirst_; i < seqNumWindowLast_; ++i)
    setSeqNumDataElement(i, seqNumWindow);
}

void PersistentStorageImp::setSeqNumDataElement(const SeqNum &index, const SeqNumData &seqNumData) const {
  char *buf = new char[SeqNumData::maxSize()];
  SeqNum shift = (index - 1) * numOfSeqNumWinParameters;
  char *movablePtr = buf;
  size_t actualSize = seqNumData.serializePrePrepareMsg(movablePtr);
  uint32_t itemId = BEGINNING_OF_SEQ_NUM_WINDOW + PRE_PREPARE_MSG + shift;
  Assert(itemId < BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInTransaction(itemId, buf, actualSize);
  Assert(actualSize != 0);

  movablePtr = buf;
  actualSize = seqNumData.serializeFullCommitProofMsg(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + FULL_COMMIT_PROOF_MSG + shift;
  Assert(itemId < BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInTransaction(itemId, buf, actualSize);
  Assert(actualSize != 0);

  movablePtr = buf;
  actualSize = seqNumData.serializePrepareFullMsg(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + PRE_PREPARE_FULL_MSG + shift;
  Assert(itemId < BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInTransaction(itemId, buf, actualSize);
  Assert(actualSize != 0);

  movablePtr = buf;
  actualSize = seqNumData.serializeCommitFullMsg(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + COMMIT_FULL_MSG + shift;
  Assert(itemId < BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInTransaction(itemId, buf, actualSize);
  Assert(actualSize != 0);

  movablePtr = buf;
  actualSize = seqNumData.serializeForceCompleted(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + FORCE_COMPLETED + shift;
  Assert(itemId < BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInTransaction(itemId, buf, actualSize);

  movablePtr = buf;
  actualSize = seqNumData.serializeSlowStarted(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + SLOW_STARTED + shift;
  Assert(itemId < BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInTransaction(itemId, buf, actualSize);
  delete[] buf;
}

void PersistentStorageImp::setSeqNumDataElement(const SeqNum &index, const SharedPtrSeqNumWindow &seqNumWindow) const {
  setSeqNumDataElement(index, seqNumWindow.get()->getByRealIndex(index));
}

void PersistentStorageImp::serializeAndSaveCheckWindow(const SharedPtrCheckWindow &checkWindow) {
  writeBeginningOfActiveWindow(BEGINNING_OF_CHECK_WINDOW, checkWindow.get()->getBeginningOfActiveWindow());
  for (uint32_t i = checkWindowFirst_; i < checkWindowLast_; ++i)
    setCheckDataElement(i, checkWindow);
}

void PersistentStorageImp::setCheckDataElement(const SeqNum &index, const CheckData &checkData) const {
  char *buf = new char[CheckData::maxSize()];
  char *movablePtr = buf;
  SeqNum shift = index * numOfCheckWinParameters;
  size_t actualSize = checkData.serializeCheckpointMsg(movablePtr);
  uint32_t itemId = BEGINNING_OF_CHECK_WINDOW + CHECK_DATA_FIRST_PARAM + shift;
  Assert(itemId < WIN_PARAMETERS_NUM);
  metadataStorage_->writeInTransaction(itemId, buf, actualSize);
  Assert(actualSize != 0);
  movablePtr = buf;
  actualSize = checkData.serializeCompletedMark(movablePtr);
  itemId = BEGINNING_OF_CHECK_WINDOW + COMPLETED_MARK + shift;
  Assert(itemId < WIN_PARAMETERS_NUM);
  metadataStorage_->writeInTransaction(itemId, buf, actualSize);
  delete[] buf;
}

void PersistentStorageImp::setCheckDataElement(const SeqNum &index, const SharedPtrCheckWindow &checkWindow) const {
  setCheckDataElement(index, checkWindow.get()->getByRealIndex(index));
}

void PersistentStorageImp::setDefaultWindowsValues() {
  SharedPtrSeqNumWindow seqNumWindow(new SeqNumWindow(seqNumWindowFirst_));
  SharedPtrCheckWindow checkWindow(new CheckWindow(checkWindowFirst_));
  serializeAndSaveSeqNumWindow(seqNumWindow);
  serializeAndSaveCheckWindow(checkWindow);
  seqNumWindowBeginning_ = seqNumWindowFirst_;
  checkWindowBeginning_ = checkWindowFirst_;
}

void PersistentStorageImp::writeBeginningOfActiveWindow(const uint32_t &index, const SeqNum &beginning) const {
  metadataStorage_->writeInTransaction(index, (char *) &beginning, sizeof(beginning));
}

/***** Public functions *****/

void PersistentStorageImp::clearSeqNumWindow() {
  SharedPtrSeqNumWindow seqNumWindow(new SeqNumWindow(seqNumWindowFirst_));
  serializeAndSaveSeqNumWindow(seqNumWindow);
}

void PersistentStorageImp::setLastStableSeqNum(const SeqNum seqNum) {
  SharedPtrCheckWindow checkWindow(new CheckWindow(checkWindowBeginning_));
  SharedPtrSeqNumWindow seqNumWindow(new SeqNumWindow(seqNumWindowBeginning_));

  metadataStorage_->writeInTransaction(LAST_STABLE_SEQ_NUM, (char *) &seqNum, sizeof(seqNum));
  list<SeqNum> cleanedCheckWindowItems = checkWindow.get()->advanceActiveWindow(seqNum);
  list<SeqNum> cleanedSeqNumWindowItems = seqNumWindow.get()->advanceActiveWindow(seqNum + 1);

  writeBeginningOfActiveWindow(BEGINNING_OF_CHECK_WINDOW, checkWindow.get()->getBeginningOfActiveWindow());
  writeBeginningOfActiveWindow(BEGINNING_OF_SEQ_NUM_WINDOW, seqNumWindow.get()->getBeginningOfActiveWindow());
  checkWindowBeginning_ = checkWindow.get()->getBeginningOfActiveWindow();
  seqNumWindowBeginning_ = seqNumWindow.get()->getBeginningOfActiveWindow();

  CheckData emptyCheckDataElement;
  for (auto id : cleanedCheckWindowItems)
    setCheckDataElement(id, emptyCheckDataElement);

  SeqNumData emptySeqNumDataElement;
  for (auto id : cleanedSeqNumWindowItems)
    setSeqNumDataElement(id, emptySeqNumDataElement);
}

void PersistentStorageImp::setMsgInSeqNumWindow(const SeqNum &seqNum, const SeqNum &parameterId, MessageBase *msg,
                                                const size_t &msgSize) const {
  char *buf = new char[msgSize];
  char *movablePtr = buf;
  const size_t actualSize = SeqNumData::serializeMsg(movablePtr, msg);
  Assert(actualSize != 0);
  const SeqNum convertedIndex = BEGINNING_OF_SEQ_NUM_WINDOW + parameterId + convertSeqNumWindowIndex(seqNum);
  Assert(convertedIndex < BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInTransaction(convertedIndex, buf, actualSize);
  delete[] buf;
}

void PersistentStorageImp::setPrePrepareMsgInSeqNumWindow(const SeqNum seqNum, const PrePrepareMsg *const msg) {
  setMsgInSeqNumWindow(seqNum, PRE_PREPARE_MSG, (MessageBase *) msg, SeqNumData::maxPrePrepareMsgSize());
}

void PersistentStorageImp::setFullCommitProofMsgInSeqNumWindow(const SeqNum seqNum,
                                                               const FullCommitProofMsg *const msg) {
  setMsgInSeqNumWindow(seqNum, FULL_COMMIT_PROOF_MSG, (MessageBase *) msg, SeqNumData::maxFullCommitProofMsgSize());
}

void PersistentStorageImp::setPrepareFullMsgInSeqNumWindow(const SeqNum seqNum, const PrepareFullMsg *const msg) {
  setMsgInSeqNumWindow(seqNum, PRE_PREPARE_FULL_MSG, (MessageBase *) msg, SeqNumData::maxPrepareFullMsgSize());
}

void PersistentStorageImp::setCommitFullMsgInSeqNumWindow(const SeqNum seqNum, const CommitFullMsg *const msg) {
  setMsgInSeqNumWindow(seqNum, COMMIT_FULL_MSG, (MessageBase *) msg, SeqNumData::maxCommitFullMsgSize());
}

void PersistentStorageImp::setBooleanInSeqNumWindow(const SeqNum &seqNum, const SeqNum &parameterId,
                                                    const bool &boolean) const {
  const SeqNum convertedIndex = BEGINNING_OF_SEQ_NUM_WINDOW + parameterId + convertSeqNumWindowIndex(seqNum);
  metadataStorage_->writeInTransaction(convertedIndex, (char *) &boolean, sizeof(boolean));
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
  const SeqNum convertedIndex = BEGINNING_OF_CHECK_WINDOW + COMPLETED_MARK + convertCheckWindowIndex(seqNum);
  Assert(convertedIndex < WIN_PARAMETERS_NUM);
  metadataStorage_->writeInTransaction(convertedIndex, buf, sizeOfCompleted);
}

void PersistentStorageImp::setCheckpointMsgInCheckWindow(const SeqNum seqNum, const CheckpointMsg *const msg) {
  size_t bufLen = CheckData::maxCheckpointMsgSize();
  char *buf = new char[bufLen];
  char *movablePtr = buf;
  size_t actualSize = CheckData::serializeCheckpointMsg(movablePtr, (CheckpointMsg *) msg);
  Assert(actualSize != 0);
  const SeqNum convertedIndex = BEGINNING_OF_CHECK_WINDOW + CHECKPOINT_MSG + convertCheckWindowIndex(seqNum);
  Assert(convertedIndex < WIN_PARAMETERS_NUM);
  metadataStorage_->writeInTransaction(convertedIndex, buf, actualSize);
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
    uint32_t itemId = LAST_EXIT_FROM_VIEW_DESC + 1 + i;
    Assert(itemId < LAST_EXEC_DESC);
    metadataStorage_->read(itemId, maxElementSize, elementBuf.get(), actualSize);
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

void PersistentStorageImp::readSeqNumDataElementFromDisk(const SeqNum &index,
                                                         const SharedPtrSeqNumWindow &seqNumWindow) {
  uint32_t actualElementSize = 0;
  uint32_t actualParameterSize = 0;
  char *buf = new char[SeqNumWindow::maxElementSize()];
  SeqNum shift = (index - 1) * numOfSeqNumWinParameters;
  char *movablePtr = buf;
  for (auto i = 0; i < numOfSeqNumWinParameters; ++i) {
    uint32_t itemId = BEGINNING_OF_SEQ_NUM_WINDOW + SEQ_NUM_FIRST_PARAM + i + shift;
    Assert(itemId < BEGINNING_OF_CHECK_WINDOW);
    metadataStorage_->read(itemId, SeqNumData::maxSize(), movablePtr, actualParameterSize);
    movablePtr += actualParameterSize;
    actualElementSize += actualParameterSize;
  }
  uint32_t actualSize = 0;
  seqNumWindow.get()->deserializeElement(index, buf, actualElementSize, actualSize);
  delete[] buf;
}

void PersistentStorageImp::readCheckDataElementFromDisk(const SeqNum &index, const SharedPtrCheckWindow &checkWindow) {
  uint32_t actualElementSize = 0;
  uint32_t actualParameterSize = 0;
  char *buf = new char[CheckData::maxSize()];
  char *movablePtr = buf;
  const SeqNum shift = index * numOfCheckWinParameters;
  for (auto i = 0; i < numOfCheckWinParameters; ++i) {
    uint32_t itemId = BEGINNING_OF_CHECK_WINDOW + CHECK_DATA_FIRST_PARAM + i + shift;
    Assert(itemId < WIN_PARAMETERS_NUM);
    metadataStorage_->read(itemId, CheckData::maxSize(), movablePtr, actualParameterSize);
    movablePtr += actualParameterSize;
    actualElementSize += actualParameterSize;
  }
  uint32_t actualSize = 0;
  checkWindow.get()->deserializeElement(index, buf, CheckData::maxSize(), actualSize);
  Assert(actualSize == actualElementSize);
  delete[] buf;
}

const SeqNum PersistentStorageImp::convertSeqNumWindowIndex(const SeqNum &index) const {
  SeqNum shiftedIndex = 0;
  if (index >= seqNumWindowBeginning_)
    shiftedIndex = index - seqNumWindowBeginning_;
  return SeqNumWindow::convertIndex(shiftedIndex, seqNumWindowFirst_) * numOfSeqNumWinParameters;
}

bool PersistentStorageImp::readBooleanFromDisk(const SeqNum &index, const SeqNum &parameterId) const {
  bool boolean = false;
  uint32_t actualSize = 0;
  const SeqNum convertedIndex = BEGINNING_OF_SEQ_NUM_WINDOW + parameterId + convertSeqNumWindowIndex(index);
  Assert(convertedIndex < BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->read(convertedIndex, sizeof(boolean), (char *) &boolean, actualSize);
  return boolean;
}

MessageBase *PersistentStorageImp::readMsgFromDisk(const SeqNum &index, const SeqNum &parameterId,
                                                   const size_t &msgSize) const {
  char *buf = new char[msgSize];
  uint32_t actualMsgSize = 0;
  const SeqNum convertedIndex = BEGINNING_OF_SEQ_NUM_WINDOW + parameterId + convertSeqNumWindowIndex(index);
  Assert(convertedIndex < BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->read(convertedIndex, msgSize, buf, actualMsgSize);
  size_t actualSize = 0;
  char *movablePtr = buf;
  auto *msg = SeqNumData::deserializeMsg(movablePtr, msgSize, actualSize);
  Assert(actualSize == actualMsgSize);
  delete[] buf;
  return msg;
}

PrePrepareMsg *PersistentStorageImp::readPrePrepareMsgFromDisk(const SeqNum &seqNum) const {
  return (PrePrepareMsg *) readMsgFromDisk(seqNum, PRE_PREPARE_MSG, SeqNumData::maxPrePrepareMsgSize());
}

FullCommitProofMsg *PersistentStorageImp::readFullCommitProofMsgFromDisk(const SeqNum &seqNum) const {
  return (FullCommitProofMsg *) readMsgFromDisk(seqNum, FULL_COMMIT_PROOF_MSG, SeqNumData::maxFullCommitProofMsgSize());
}

PrepareFullMsg *PersistentStorageImp::readPrepareFullMsgFromDisk(const SeqNum &seqNum) const {
  return (PrepareFullMsg *) readMsgFromDisk(seqNum, PRE_PREPARE_FULL_MSG, SeqNumData::maxPrepareFullMsgSize());
}

CommitFullMsg *PersistentStorageImp::readCommitFullMsgFromDisk(const SeqNum &seqNum) const {
  return (CommitFullMsg *) readMsgFromDisk(seqNum, COMMIT_FULL_MSG, SeqNumData::maxCommitFullMsgSize());
}

const SeqNum PersistentStorageImp::convertCheckWindowIndex(const SeqNum &index) const {
  SeqNum shiftedIndex = 0;
  if (index >= checkWindowBeginning_)
    shiftedIndex = index - checkWindowBeginning_;
  return CheckWindow::convertIndex(shiftedIndex, checkWindowFirst_) * numOfCheckWinParameters;
}

bool PersistentStorageImp::readCompletedMarkFromDisk(const SeqNum &index) const {
  bool completedMark = false;
  uint32_t actualSize = 0;
  const SeqNum convertedIndex = BEGINNING_OF_CHECK_WINDOW + COMPLETED_MARK + convertCheckWindowIndex(index);
  Assert(convertedIndex < WIN_PARAMETERS_NUM);
  metadataStorage_->read(convertedIndex, sizeof(completedMark), (char *) &completedMark, actualSize);
  Assert(sizeof(completedMark) == actualSize);
  return completedMark;
}

CheckpointMsg *PersistentStorageImp::readCheckpointMsgFromDisk(const SeqNum &index) const {
  const size_t bufLen = CheckData::maxSize();
  char *buf = new char[bufLen];
  uint32_t actualMsgSize = 0;
  const SeqNum convertedIndex = BEGINNING_OF_CHECK_WINDOW + CHECKPOINT_MSG + convertCheckWindowIndex(index);
  Assert(convertedIndex < WIN_PARAMETERS_NUM);
  metadataStorage_->read(convertedIndex, bufLen, buf, actualMsgSize);
  size_t actualSize = 0;
  char *movablePtr = buf;
  auto *checkpointMsg = CheckData::deserializeCheckpointMsg(movablePtr, bufLen, actualSize);
  Assert(actualSize == actualMsgSize);
  delete[] buf;
  return checkpointMsg;
}

SeqNum PersistentStorageImp::readBeginningOfActiveWindow(const uint32_t &index) const {
  const size_t paramSize = sizeof(SeqNum);
  uint32_t actualSize = 0;
  SeqNum beginningOfActiveWindow = 0;
  metadataStorage_->read(index, paramSize, (char *) &beginningOfActiveWindow, actualSize);
  Assert(actualSize == paramSize);
  return beginningOfActiveWindow;
}

/***** Public functions *****/

SharedPtrSeqNumWindow PersistentStorageImp::getSeqNumWindow() {
  auto windowBeginning = readBeginningOfActiveWindow(BEGINNING_OF_SEQ_NUM_WINDOW);
  SharedPtrSeqNumWindow seqNumWindow(new SeqNumWindow(windowBeginning));

  for (auto i = seqNumWindowFirst_; i < seqNumWindowLast_; ++i)
    readSeqNumDataElementFromDisk(i, seqNumWindow);
  return seqNumWindow;
}

SharedPtrCheckWindow PersistentStorageImp::getCheckWindow() {
  auto windowBeginning = readBeginningOfActiveWindow(BEGINNING_OF_CHECK_WINDOW);
  SharedPtrCheckWindow checkWindow(new CheckWindow(windowBeginning));

  for (auto i = checkWindowFirst_; i < checkWindowLast_; ++i)
    readCheckDataElementFromDisk(i, checkWindow);
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
