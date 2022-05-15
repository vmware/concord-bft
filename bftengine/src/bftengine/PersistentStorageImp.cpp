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
#include "Logger.hpp"
#include "bftengine/EpochManager.hpp"
#include "bftengine/DbCheckpointMetadata.hpp"
#include "bftengine/ReplicaSpecificInfoManager.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include <list>
#include <sstream>
#include <stdexcept>

using namespace std;
using namespace concord::serialize;

namespace bftEngine {
namespace impl {

const string METADATA_PARAMS_VERSION = "1.1";
PersistentStorageImp::PersistentStorageImp(
    uint16_t numReplicas, uint16_t fVal, uint16_t cVal, uint64_t numOfPrinciples, uint64_t maxClientBatchSize)
    : numReplicas_(numReplicas),
      fVal_(fVal),
      cVal_(cVal),
      numPrinciples_(numOfPrinciples + numReplicas),  // numReplicas is for the internal clients
      maxClientBatchSize_(maxClientBatchSize),
      version_(METADATA_PARAMS_VERSION),
      pre_allocated_mem_buffers_{UniquePtrToChar(new char[DescriptorOfLastExitFromView::simpleParamsSize()]),
                                 UniquePtrToChar(new char[DescriptorOfLastExitFromView::maxElementSize()]),
                                 UniquePtrToChar(new char[DescriptorOfLastExitFromView::maxComplaintSize()]),
                                 UniquePtrToChar(new char[DescriptorOfLastNewView::simpleParamsSize()]),
                                 UniquePtrToChar(new char[DescriptorOfLastNewView::maxElementSize()]),
                                 UniquePtrToChar(new char[DescriptorOfLastExecution::maxSize()]),
                                 UniquePtrToChar(new char[DescriptorOfLastStableCheckpoint::maxSize(numReplicas_)]),
                                 UniquePtrToChar(new char[CheckData::maxSize()]),
                                 UniquePtrToChar(new char[bftEngine::ReplicaConfig::instance().maxExternalMessageSize]),
                                 UniquePtrToChar(new char[CheckData::maxSize()]),
                                 UniquePtrToChar(new char[SeqNumWindow::maxElementSize()])} {
  DescriptorOfLastNewView::setViewChangeMsgsNum(fVal, cVal);
}

void PersistentStorageImp::retrieveWindowsMetadata() {
  seqNumWindowBeginning_ = readBeginningOfActiveWindow(BEGINNING_OF_SEQ_NUM_WINDOW);
  checkWindowBeginning_ = readBeginningOfActiveWindow(BEGINNING_OF_CHECK_WINDOW);
}

bool PersistentStorageImp::init(unique_ptr<MetadataStorage> metadataStorage) {
  metadataStorage_ = move(metadataStorage);
  try {
    if (!getStoredVersion().empty()) {
      LOG_INFO(GL, "PersistentStorageImp::init version=" << version_.c_str());
      retrieveWindowsMetadata();
      // Retrieve metadata parameters stored in the memory
      getDescriptorOfLastExecution();
      return false;
    }
  } catch (const runtime_error &exc) {
  }
  // DB is not populated yet with default metadata parameter values.
  setDefaultsInMetadataStorage();
  return true;
}

void PersistentStorageImp::setDefaultsInMetadataStorage() {
  LOG_INFO(GL, "Set default values");
  beginWriteTran();

  setVersion();
  setLastExecutedSeqNumInternal(lastExecutedSeqNum_);
  setPrimaryLastUsedSeqNumInternal(primaryLastUsedSeqNum_);
  setStrictLowerBoundOfSeqNumsInternal(strictLowerBoundOfSeqNums_);
  setLastViewTransferredSeqNumbersInternal(lastViewTransferredSeqNum_);

  metadataStorage_->writeInBatch(LAST_STABLE_SEQ_NUM, (char *)&lastStableSeqNum_, sizeof(lastStableSeqNum_));
  setDefaultWindowsValues();

  initDescriptorOfLastExitFromView();
  initDescriptorOfLastNewView();
  initDescriptorOfLastExecution();
  initDescriptorOfLastStableCheckpoint();
  endWriteTran();
}

// This function is used by an external code to initialize MetadataStorage and enable StateTransfer using the same DB.
ObjectDescMap PersistentStorageImp::getDefaultMetadataObjectDescriptors(uint16_t &numOfObjects) const {
  ObjectDescMap metadataObjectsArray;
  for (uint16_t i = FIRST_METADATA_PARAMETER; i < MAX_METADATA_PARAMS_NUM; ++i) {
    metadataObjectsArray[i].id = i;
    metadataObjectsArray[i].maxSize = 0;
  }

  metadataObjectsArray[VERSION_PARAMETER].maxSize = maxVersionSize_;
  metadataObjectsArray[LAST_EXEC_SEQ_NUM].maxSize = sizeof(lastExecutedSeqNum_);
  metadataObjectsArray[PRIMARY_LAST_USED_SEQ_NUM].maxSize = sizeof(primaryLastUsedSeqNum_);
  metadataObjectsArray[LOWER_BOUND_OF_SEQ_NUM].maxSize = sizeof(strictLowerBoundOfSeqNums_);
  metadataObjectsArray[LAST_VIEW_TRANSFERRED_SEQ_NUM].maxSize = sizeof(lastViewTransferredSeqNum_);
  metadataObjectsArray[LAST_STABLE_SEQ_NUM].maxSize = sizeof(lastStableSeqNum_);
  metadataObjectsArray[BEGINNING_OF_SEQ_NUM_WINDOW].maxSize = sizeof(SeqNum);
  metadataObjectsArray[BEGINNING_OF_CHECK_WINDOW].maxSize = sizeof(SeqNum);
  metadataObjectsArray[ERASE_METADATA_ON_STARTUP].maxSize = sizeof(bool);
  metadataObjectsArray[USER_DATA].maxSize = kMaxUserDataSizeBytes;
  metadataObjectsArray[START_NEW_EPOCH].maxSize = sizeof(bool);
  metadataObjectsArray[DB_CHECKPOINT_DESCRIPTOR].maxSize = DB_CHECKPOINT_METADATA_MAX_SIZE;

  for (auto i = 0; i < kWorkWindowSize; ++i) {
    metadataObjectsArray[LAST_EXIT_FROM_VIEW_DESC + 1 + i].maxSize = DescriptorOfLastExitFromView::maxElementSize();
  }

  for (auto i = 0; i < kWorkWindowSize * numOfSeqNumWinParameters; ++i) {
    metadataObjectsArray[BEGINNING_OF_SEQ_NUM_WINDOW + SEQ_NUM_FIRST_PARAM + i].maxSize =
        SeqNumWindow::maxElementSize();
  }

  uint32_t viewChangeMsgsNum = DescriptorOfLastNewView::getViewChangeMsgsNum();
  for (uint32_t i = 0; i < viewChangeMsgsNum; ++i) {
    metadataObjectsArray[LAST_NEW_VIEW_DESC + 1 + i].maxSize = DescriptorOfLastNewView::maxElementSize();
  }

  for (auto i = 0; i < checkWinSize * numOfCheckWinParameters; ++i)
    metadataObjectsArray[BEGINNING_OF_CHECK_WINDOW + CHECK_DATA_FIRST_PARAM + i].maxSize =
        CheckWindow::maxElementSize();

  metadataObjectsArray[LAST_EXIT_FROM_VIEW_DESC].maxSize = DescriptorOfLastExitFromView::simpleParamsSize();
  for (uint32_t i = 0; i < reservedComplaintsNum; ++i) {
    metadataObjectsArray[LAST_COMPLAINTS_DESC + i].maxSize = DescriptorOfLastExitFromView::maxComplaintSize();
  }
  metadataObjectsArray[LAST_EXEC_DESC].maxSize = DescriptorOfLastExecution::maxSize();
  metadataObjectsArray[LAST_NEW_VIEW_DESC].maxSize = DescriptorOfLastNewView::simpleParamsSize();
  metadataObjectsArray[LAST_STABLE_CHECKPOINT_DESC].maxSize = DescriptorOfLastStableCheckpoint::maxSize(numReplicas_);

  for (auto i = 0; i < numPrinciples_; i++) {
    uint32_t baseDescNum = REPLICA_SPECIFIC_INFO_BASE + viewChangeMsgsNum + maxClientBatchSize_ * i;
    for (auto j = 0; j < maxClientBatchSize_; j++) {
      metadataObjectsArray[baseDescNum + j].maxSize = (RsiItem::RSI_DATA_PREFIX_SIZE + replicaSpecificInfoMaxSize);
    }
  }
  numOfObjects = metadataObjectsArray.size() - FIRST_METADATA_PARAMETER;
  return metadataObjectsArray;
}

uint8_t PersistentStorageImp::beginWriteTran() {
  if (numOfNestedTransactions_ == 0) {
    metadataStorage_->beginAtomicWriteOnlyBatch();
  }
  return ++numOfNestedTransactions_;
}

uint8_t PersistentStorageImp::endWriteTran(bool sync) {
  ConcordAssertNE(numOfNestedTransactions_, 0);
  if (--numOfNestedTransactions_ == 0) {
    metadataStorage_->commitAtomicWriteOnlyBatch(sync);
  }
  return numOfNestedTransactions_;
}

bool PersistentStorageImp::isInWriteTran() const { return (numOfNestedTransactions_ != 0); }

/***** Setters *****/

void PersistentStorageImp::setVersion() const {
  ConcordAssert(isInWriteTran());
  const uint32_t sizeOfVersion = version_.size();
  const uint32_t sizeOfSizeOfVersion = sizeof(sizeOfVersion);
  const int64_t outBufSize = sizeOfVersion + sizeOfSizeOfVersion;
  UniquePtrToChar outBuf(new char[outBufSize]);
  char *outBufPtr = outBuf.get();
  memcpy(outBufPtr, &sizeOfVersion, sizeOfSizeOfVersion);
  outBufPtr += sizeOfSizeOfVersion;
  memcpy(outBufPtr, version_.c_str(), sizeOfVersion);
  metadataStorage_->writeInBatch(VERSION_PARAMETER, outBuf.get(), outBufSize);
}

void PersistentStorageImp::setLastExecutedSeqNumInternal(SeqNum seqNum) {
  metadataStorage_->writeInBatch(LAST_EXEC_SEQ_NUM, (char *)&seqNum, sizeof(seqNum));
  lastExecutedSeqNum_ = seqNum;
}

void PersistentStorageImp::setLastExecutedSeqNum(SeqNum seqNum) {
  ConcordAssert(setIsAllowed());
  ConcordAssertLE(lastExecutedSeqNum_, seqNum);
  setLastExecutedSeqNumInternal(seqNum);
}

void PersistentStorageImp::setPrimaryLastUsedSeqNumInternal(SeqNum seqNum) {
  metadataStorage_->writeInBatch(PRIMARY_LAST_USED_SEQ_NUM, (char *)&seqNum, sizeof(seqNum));
  primaryLastUsedSeqNum_ = seqNum;
}

void PersistentStorageImp::setPrimaryLastUsedSeqNum(SeqNum seqNum) {
  ConcordAssert(nonExecSetIsAllowed());
  setPrimaryLastUsedSeqNumInternal(seqNum);
}

void PersistentStorageImp::setStrictLowerBoundOfSeqNumsInternal(SeqNum seqNum) {
  metadataStorage_->writeInBatch(LOWER_BOUND_OF_SEQ_NUM, (char *)&seqNum, sizeof(seqNum));
  strictLowerBoundOfSeqNums_ = seqNum;
}

void PersistentStorageImp::setStrictLowerBoundOfSeqNums(SeqNum seqNum) {
  ConcordAssert(nonExecSetIsAllowed());
  setStrictLowerBoundOfSeqNumsInternal(seqNum);
}

void PersistentStorageImp::setLastViewTransferredSeqNumbersInternal(ViewNum view) {
  metadataStorage_->writeInBatch(LAST_VIEW_TRANSFERRED_SEQ_NUM, (char *)&view, sizeof(view));
  lastViewTransferredSeqNum_ = view;
}

void PersistentStorageImp::setLastViewThatTransferredSeqNumbersFullyExecuted(ViewNum view) {
  ConcordAssert(nonExecSetIsAllowed());
  ConcordAssertLE(lastViewTransferredSeqNum_, view);
  setLastViewTransferredSeqNumbersInternal(view);
}

/***** Descriptors handling *****/

void PersistentStorageImp::saveDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &newDesc) {
  const size_t simpleParamsSize = DescriptorOfLastExitFromView::simpleParamsSize();
  memset(pre_allocated_mem_buffers_.last_exit_from_view_simple_element_buf.get(), 0, simpleParamsSize);
  size_t actualSize = 0;
  newDesc.serializeSimpleParams(
      pre_allocated_mem_buffers_.last_exit_from_view_simple_element_buf.get(), simpleParamsSize, actualSize);
  metadataStorage_->writeInBatch(LAST_EXIT_FROM_VIEW_DESC,
                                 pre_allocated_mem_buffers_.last_exit_from_view_simple_element_buf.get(),
                                 simpleParamsSize);

  size_t actualElementSize = 0;
  uint32_t elementsNum = newDesc.elements.size();
  uint32_t maxElementSize = DescriptorOfLastExitFromView::maxElementSize();
  for (size_t i = 0; i < elementsNum; ++i) {
    newDesc.serializeElement(
        i, pre_allocated_mem_buffers_.last_exit_fron_view_element_buf.get(), maxElementSize, actualElementSize);
    ConcordAssertNE(actualElementSize, 0);
    uint32_t itemId = LAST_EXIT_FROM_VIEW_DESC + 1 + i;
    ConcordAssertLT(itemId, LAST_COMPLAINTS_DESC);
    metadataStorage_->writeInBatch(
        itemId, pre_allocated_mem_buffers_.last_exit_fron_view_element_buf.get(), actualElementSize);
  }

  size_t actualComplaintSize = 0;
  uint32_t complaintsNum = newDesc.complaints.size();
  uint32_t maxComplaintSize = DescriptorOfLastExitFromView::maxComplaintSize();
  for (size_t i = 0; i < complaintsNum; ++i) {
    newDesc.serializeComplaint(
        i, pre_allocated_mem_buffers_.last_exit_from_view_complaint_buf.get(), maxComplaintSize, actualComplaintSize);
    ConcordAssertNE(actualComplaintSize, 0);
    uint32_t itemId = LAST_COMPLAINTS_DESC + i;
    ConcordAssertLT(itemId, LAST_EXEC_DESC);
    metadataStorage_->writeInBatch(
        itemId, pre_allocated_mem_buffers_.last_exit_from_view_complaint_buf.get(), actualComplaintSize);
  }
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
  size_t actualSize = 0;
  newDesc.serializeSimpleParams(
      pre_allocated_mem_buffers_.last_new_view_simple_element_buf.get(), simpleParamsSize, actualSize);
  metadataStorage_->writeInBatch(
      LAST_NEW_VIEW_DESC, pre_allocated_mem_buffers_.last_new_view_simple_element_buf.get(), actualSize);

  size_t actualElementSize = 0;
  uint32_t numOfMessages = DescriptorOfLastNewView::getViewChangeMsgsNum();
  uint32_t maxElementSize = DescriptorOfLastNewView::maxElementSize();
  for (uint32_t i = 0; i < numOfMessages; ++i) {
    newDesc.serializeElement(
        i, pre_allocated_mem_buffers_.last_new_view_element_buf.get(), maxElementSize, actualElementSize);
    ConcordAssertNE(actualElementSize, 0);
    metadataStorage_->writeInBatch(
        LAST_NEW_VIEW_DESC + 1 + i, pre_allocated_mem_buffers_.last_new_view_element_buf.get(), actualElementSize);
  }
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
  char *descBuf = pre_allocated_mem_buffers_.descriptor_of_last_execution_buf.get();
  size_t actualSize = 0;
  newDesc.serialize(descBuf, bufLen, actualSize);
  ConcordAssertNE(actualSize, 0);
  metadataStorage_->writeInBatch(
      LAST_EXEC_DESC, pre_allocated_mem_buffers_.descriptor_of_last_execution_buf.get(), actualSize);
}

void PersistentStorageImp::setDescriptorOfLastExecution(const DescriptorOfLastExecution &desc, bool init) {
  if (!init) verifyDescriptorOfLastExecution(desc);
  saveDescriptorOfLastExecution(desc);
}

void PersistentStorageImp::setDescriptorOfLastStableCheckpoint(
    const DescriptorOfLastStableCheckpoint &stableCheckDesc) {
  const size_t bufLen = DescriptorOfLastStableCheckpoint::maxSize(numReplicas_);
  char *descBuf = pre_allocated_mem_buffers_.descriptor_og_last_stable_cp_buf.get();
  size_t actualSize = 0;
  stableCheckDesc.serialize(descBuf, bufLen, actualSize);
  ConcordAssertNE(actualSize, 0);
  metadataStorage_->writeInBatch(
      LAST_STABLE_CHECKPOINT_DESC, pre_allocated_mem_buffers_.descriptor_og_last_stable_cp_buf.get(), actualSize);
}

void PersistentStorageImp::initDescriptorOfLastExecution() {
  DescriptorOfLastExecution desc;
  setDescriptorOfLastExecution(desc, true);
}

void PersistentStorageImp::initDescriptorOfLastStableCheckpoint() {
  DescriptorOfLastStableCheckpoint desc{numReplicas_, {}};
  setDescriptorOfLastStableCheckpoint(desc);
}

void PersistentStorageImp::setDescriptorOfLastExecution(const DescriptorOfLastExecution &desc) {
  setDescriptorOfLastExecution(desc, false);
  hasDescriptorOfLastExecution_ = true;
  descriptorOfLastExecution_ = DescriptorOfLastExecution{desc.executedSeqNum, desc.validRequests, desc.timeInTicks};
}

/***** Windows handling *****/

/***** Private functions *****/

void PersistentStorageImp::saveDefaultsInSeqNumWindow() {
  writeBeginningOfActiveWindow(BEGINNING_OF_SEQ_NUM_WINDOW, seqNumWindowBeginning_);
  const SeqNumData seqNumData;
  for (uint32_t i = 0; i < seqWinSize; ++i) setSeqNumDataElement(i, seqNumData);
}

void PersistentStorageImp::setSeqNumDataElement(SeqNum index, const SeqNumData &seqNumData) const {
  char *movablePtr = pre_allocated_mem_buffers_.seq_num_element_buf.get();
  SeqNum shift = index * numOfSeqNumWinParameters;
  size_t actualSize = seqNumData.serializePrePrepareMsg(movablePtr);
  uint32_t itemId = BEGINNING_OF_SEQ_NUM_WINDOW + PRE_PREPARE_MSG + shift;
  ConcordAssertLT(itemId, BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInBatch(itemId, pre_allocated_mem_buffers_.seq_num_element_buf.get(), actualSize);
  ConcordAssertNE(actualSize, 0);

  movablePtr = pre_allocated_mem_buffers_.seq_num_element_buf.get();
  actualSize = seqNumData.serializeFullCommitProofMsg(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + FULL_COMMIT_PROOF_MSG + shift;
  ConcordAssertLT(itemId, BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInBatch(itemId, pre_allocated_mem_buffers_.seq_num_element_buf.get(), actualSize);
  ConcordAssertNE(actualSize, 0);

  movablePtr = pre_allocated_mem_buffers_.seq_num_element_buf.get();
  actualSize = seqNumData.serializePrepareFullMsg(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + PRE_PREPARE_FULL_MSG + shift;
  ConcordAssertLT(itemId, BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInBatch(itemId, pre_allocated_mem_buffers_.seq_num_element_buf.get(), actualSize);
  ConcordAssertNE(actualSize, 0);

  movablePtr = pre_allocated_mem_buffers_.seq_num_element_buf.get();
  actualSize = seqNumData.serializeCommitFullMsg(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + COMMIT_FULL_MSG + shift;
  ConcordAssertLT(itemId, BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInBatch(itemId, pre_allocated_mem_buffers_.seq_num_element_buf.get(), actualSize);
  ConcordAssertNE(actualSize, 0);

  movablePtr = pre_allocated_mem_buffers_.seq_num_element_buf.get();
  actualSize = seqNumData.serializeForceCompleted(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + FORCE_COMPLETED + shift;
  ConcordAssertLT(itemId, BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInBatch(itemId, pre_allocated_mem_buffers_.seq_num_element_buf.get(), actualSize);

  movablePtr = pre_allocated_mem_buffers_.seq_num_element_buf.get();
  actualSize = seqNumData.serializeSlowStarted(movablePtr);
  itemId = BEGINNING_OF_SEQ_NUM_WINDOW + SLOW_STARTED + shift;
  ConcordAssertLT(itemId, BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->writeInBatch(itemId, pre_allocated_mem_buffers_.seq_num_element_buf.get(), actualSize);
}

void PersistentStorageImp::saveDefaultsInCheckWindow() {
  writeBeginningOfActiveWindow(BEGINNING_OF_CHECK_WINDOW, checkWindowBeginning_);
  const CheckData checkData;
  for (uint32_t i = 0; i < checkWinSize; ++i) setCheckDataElement(i, checkData);
}

void PersistentStorageImp::setCheckDataElement(SeqNum index, const CheckData &checkData) const {
  char *movablePtr = pre_allocated_mem_buffers_.check_data_element_buf.get();
  SeqNum shift = index * numOfCheckWinParameters;
  size_t actualSize = checkData.serializeCheckpointMsg(movablePtr);

  uint32_t itemId = BEGINNING_OF_CHECK_WINDOW + CHECK_DATA_FIRST_PARAM + shift;
  ConcordAssertLT(itemId, WIN_PARAMETERS_NUM);
  metadataStorage_->writeInBatch(itemId, pre_allocated_mem_buffers_.check_data_element_buf.get(), actualSize);
  ConcordAssertNE(actualSize, 0);

  movablePtr = pre_allocated_mem_buffers_.seq_num_element_buf.get();
  actualSize = checkData.serializeCompletedMark(movablePtr);
  itemId = BEGINNING_OF_CHECK_WINDOW + COMPLETED_MARK + shift;
  ConcordAssertLT(itemId, WIN_PARAMETERS_NUM);
  metadataStorage_->writeInBatch(itemId, pre_allocated_mem_buffers_.check_data_element_buf.get(), actualSize);
}

void PersistentStorageImp::setDefaultWindowsValues() {
  seqNumWindowBeginning_ = seqNumWindowFirst_;
  checkWindowBeginning_ = checkWindowFirst_;
  saveDefaultsInSeqNumWindow();
  saveDefaultsInCheckWindow();
}

void PersistentStorageImp::writeBeginningOfActiveWindow(uint32_t index, SeqNum beginning) const {
  LOG_DEBUG(GL, "PersistentStorageImp::writeBeginningOfActiveWindow index=" << index << "beginning=" << beginning);
  metadataStorage_->writeInBatch(index, (char *)&beginning, sizeof(beginning));
}

/***** Public functions *****/

void PersistentStorageImp::clearSeqNumWindow() { saveDefaultsInSeqNumWindow(); }

void PersistentStorageImp::setLastStableSeqNum(SeqNum seqNum) {
  SharedPtrCheckWindow checkWindow(new CheckWindow(checkWindowBeginning_));
  SharedPtrSeqNumWindow seqNumWindow(new SeqNumWindow(seqNumWindowBeginning_));

  metadataStorage_->writeInBatch(LAST_STABLE_SEQ_NUM, (char *)&seqNum, sizeof(seqNum));
  list<SeqNum> cleanedCheckWindowItems = checkWindow.get()->advanceActiveWindow(seqNum);
  list<SeqNum> cleanedSeqNumWindowItems = seqNumWindow.get()->advanceActiveWindow(seqNum + 1);
  checkWindowBeginning_ = checkWindow.get()->getBeginningOfActiveWindow();
  seqNumWindowBeginning_ = seqNumWindow.get()->getBeginningOfActiveWindow();
  writeBeginningOfActiveWindow(BEGINNING_OF_CHECK_WINDOW, checkWindowBeginning_);
  writeBeginningOfActiveWindow(BEGINNING_OF_SEQ_NUM_WINDOW, seqNumWindowBeginning_);

  const CheckData emptyCheckDataElement;
  for (auto id : cleanedCheckWindowItems) {
    LOG_DEBUG(GL, "PersistentStorageImp::setLastStableSeqNum (setCheckDataElement) id=" << id);
    setCheckDataElement(id, emptyCheckDataElement);
  }

  const SeqNumData emptySeqNumDataElement;
  for (auto id : cleanedSeqNumWindowItems) {
    LOG_DEBUG(GL, "PersistentStorageImp::setLastStableSeqNum (setSeqNumDataElement) id=" << id);
    setSeqNumDataElement(id, emptySeqNumDataElement);
  }
}

void PersistentStorageImp::setMsgInSeqNumWindow(SeqNum seqNum,
                                                SeqNum parameterId,
                                                MessageBase *msg,
                                                size_t msgSize) const {
  char *movablePtr = pre_allocated_mem_buffers_.msg_element_buf.get();
  const size_t actualSize = SeqNumData::serializeMsg(movablePtr, msg);
  ConcordAssertNE(actualSize, 0);
  const SeqNum convertedIndex = BEGINNING_OF_SEQ_NUM_WINDOW + parameterId + convertSeqNumWindowIndex(seqNum);
  ConcordAssertLT(convertedIndex, BEGINNING_OF_CHECK_WINDOW);
  LOG_DEBUG(GL, "PersistentStorageImp::setMsgInSeqNumWindow convertedIndex=" << convertedIndex);
  metadataStorage_->writeInBatch(convertedIndex, pre_allocated_mem_buffers_.msg_element_buf.get(), actualSize);
}

void PersistentStorageImp::setPrePrepareMsgInSeqNumWindow(SeqNum seqNum, PrePrepareMsg *msg) {
  setMsgInSeqNumWindow(seqNum, PRE_PREPARE_MSG, (MessageBase *)msg, SeqNumData::maxMessageSize<PrePrepareMsg>());
}

void PersistentStorageImp::setFullCommitProofMsgInSeqNumWindow(SeqNum seqNum, FullCommitProofMsg *msg) {
  setMsgInSeqNumWindow(
      seqNum, FULL_COMMIT_PROOF_MSG, (MessageBase *)msg, SeqNumData::maxMessageSize<FullCommitProofMsg>());
}

void PersistentStorageImp::setPrepareFullMsgInSeqNumWindow(SeqNum seqNum, PrepareFullMsg *msg) {
  setMsgInSeqNumWindow(seqNum, PRE_PREPARE_FULL_MSG, (MessageBase *)msg, SeqNumData::maxMessageSize<PrepareFullMsg>());
}

void PersistentStorageImp::setCommitFullMsgInSeqNumWindow(SeqNum seqNum, CommitFullMsg *msg) {
  setMsgInSeqNumWindow(seqNum, COMMIT_FULL_MSG, (MessageBase *)msg, SeqNumData::maxMessageSize<CommitFullMsg>());
}

void PersistentStorageImp::setOneByteInSeqNumWindow(SeqNum seqNum, SeqNum parameterId, uint8_t oneByte) const {
  const SeqNum convertedIndex = BEGINNING_OF_SEQ_NUM_WINDOW + parameterId + convertSeqNumWindowIndex(seqNum);
  metadataStorage_->writeInBatch(convertedIndex, (char *)&oneByte, sizeof(oneByte));
}

void PersistentStorageImp::setForceCompletedInSeqNumWindow(SeqNum seqNum, bool forceCompleted) {
  setOneByteInSeqNumWindow(seqNum, FORCE_COMPLETED, forceCompleted);
}

void PersistentStorageImp::setSlowStartedInSeqNumWindow(SeqNum seqNum, bool slowStarted) {
  setOneByteInSeqNumWindow(seqNum, SLOW_STARTED, slowStarted);
}

void PersistentStorageImp::setCompletedMarkInCheckWindow(SeqNum seqNum, bool completed) {
  ConcordAssert(completed);
  const size_t sizeOfCompleted = sizeof(uint8_t);
  char buf[sizeOfCompleted];
  char *movablePtr = buf;
  CheckData::serializeCompletedMark(movablePtr, completed);
  const SeqNum convertedIndex = BEGINNING_OF_CHECK_WINDOW + COMPLETED_MARK + convertCheckWindowIndex(seqNum);
  ConcordAssertLT(convertedIndex, WIN_PARAMETERS_NUM);
  metadataStorage_->writeInBatch(convertedIndex, buf, sizeOfCompleted);
}

void PersistentStorageImp::setCheckpointMsgInCheckWindow(SeqNum seqNum, CheckpointMsg *msg) {
  char *movablePtr = pre_allocated_mem_buffers_.check_data_element_buf.get();
  size_t actualSize = CheckData::serializeCheckpointMsg(movablePtr, (CheckpointMsg *)msg);
  ConcordAssertNE(actualSize, 0);
  const SeqNum convertedIndex = BEGINNING_OF_CHECK_WINDOW + CHECKPOINT_MSG + convertCheckWindowIndex(seqNum);
  ConcordAssertLT(convertedIndex, WIN_PARAMETERS_NUM);
  LOG_DEBUG(GL, "PersistentStorageImp::setCheckpointMsgInCheckWindow convertedIndex=" << convertedIndex);
  metadataStorage_->writeInBatch(convertedIndex, pre_allocated_mem_buffers_.check_data_element_buf.get(), actualSize);
}

void PersistentStorageImp::setUserDataAtomically(const void *data, std::size_t numberOfBytes) {
  if (numberOfBytes > kMaxUserDataSizeBytes) {
    throw std::invalid_argument{"Metadata user data is too big"};
  }
  metadataStorage_->atomicWrite(USER_DATA, static_cast<const char *>(data), numberOfBytes);
}

void PersistentStorageImp::setUserDataInTransaction(const void *data, std::size_t numberOfBytes) {
  ConcordAssert(setIsAllowed());
  if (numberOfBytes > kMaxUserDataSizeBytes) {
    throw std::invalid_argument{"Metadata user data is too big"};
  }
  metadataStorage_->writeInBatch(USER_DATA, static_cast<const char *>(data), numberOfBytes);
}

bool PersistentStorageImp::setReplicaSpecificInfo(uint32_t index, const std::vector<uint8_t> &data) {
  uint32_t viewChangeMsgsNum = DescriptorOfLastNewView::getViewChangeMsgsNum();
  metadataStorage_->writeInBatch(
      ReplicaSpecificInfoParameterIds::REPLICA_SPECIFIC_INFO_BASE + viewChangeMsgsNum + index,
      (char *)(data.data()),
      data.size());
  return true;
}
/***** Getters *****/
std::vector<uint8_t> PersistentStorageImp::getReplicaSpecificInfo(uint32_t index) {
  uint32_t viewChangeMsgsNum = DescriptorOfLastNewView::getViewChangeMsgsNum();
  std::vector<uint8_t> data;
  data.resize(replicaSpecificInfoMaxSize + RsiItem::RSI_DATA_PREFIX_SIZE);
  uint32_t outActualObjectSize = 0;
  metadataStorage_->read(
      REPLICA_SPECIFIC_INFO_BASE + viewChangeMsgsNum + index, data.size(), (char *)(data.data()), outActualObjectSize);
  if (!outActualObjectSize) return {};
  data.resize(outActualObjectSize);
  return data;
}
string PersistentStorageImp::getStoredVersion() {
  ConcordAssert(!isInWriteTran());
  uint32_t outActualObjectSize = 0;
  UniquePtrToChar outBuf(new char[maxVersionSize_]);
  char *outBufPtr = outBuf.get();
  metadataStorage_->read(VERSION_PARAMETER, maxVersionSize_, outBufPtr, outActualObjectSize);
  if (!outActualObjectSize)  // Parameter not found
    return string();

  uint32_t sizeOfVersion = 0;
  memcpy(&sizeOfVersion, outBufPtr, sizeof(sizeOfVersion));
  outBufPtr += sizeof(sizeOfVersion);
  string savedVersion;
  savedVersion.assign(outBufPtr, sizeOfVersion);
  ConcordAssertEQ(version_, savedVersion);

  return version_;
}

SeqNum PersistentStorageImp::getLastExecutedSeqNum() {
  ConcordAssert(getIsAllowed());
  lastExecutedSeqNum_ = getSeqNum(LAST_EXEC_SEQ_NUM, sizeof(lastExecutedSeqNum_));
  return lastExecutedSeqNum_;
}

SeqNum PersistentStorageImp::getPrimaryLastUsedSeqNum() {
  ConcordAssert(getIsAllowed());
  primaryLastUsedSeqNum_ = getSeqNum(PRIMARY_LAST_USED_SEQ_NUM, sizeof(primaryLastUsedSeqNum_));
  return primaryLastUsedSeqNum_;
}

SeqNum PersistentStorageImp::getStrictLowerBoundOfSeqNums() {
  ConcordAssert(getIsAllowed());
  strictLowerBoundOfSeqNums_ = getSeqNum(LOWER_BOUND_OF_SEQ_NUM, sizeof(strictLowerBoundOfSeqNums_));
  return strictLowerBoundOfSeqNums_;
}

SeqNum PersistentStorageImp::getLastStableSeqNum() {
  ConcordAssert(getIsAllowed());
  lastStableSeqNum_ = getSeqNum(LAST_STABLE_SEQ_NUM, sizeof(lastStableSeqNum_));
  return lastStableSeqNum_;
}

ViewNum PersistentStorageImp::getLastViewThatTransferredSeqNumbersFullyExecuted() {
  ConcordAssert(getIsAllowed());
  lastViewTransferredSeqNum_ = getSeqNum(LAST_VIEW_TRANSFERRED_SEQ_NUM, sizeof(lastViewTransferredSeqNum_));
  return lastViewTransferredSeqNum_;
}

/***** Descriptors handling *****/

DescriptorOfLastExitFromView PersistentStorageImp::getAndAllocateDescriptorOfLastExitFromView() {
  ConcordAssert(getIsAllowed());
  DescriptorOfLastExitFromView dbDesc;
  const size_t simpleParamsSize = DescriptorOfLastExitFromView::simpleParamsSize();
  uint32_t sizeInDb = 0;

  // Read first simple params.
  metadataStorage_->read(LAST_EXIT_FROM_VIEW_DESC,
                         simpleParamsSize,
                         pre_allocated_mem_buffers_.last_exit_from_view_simple_element_buf.get(),
                         sizeInDb);
  ConcordAssertEQ(sizeInDb, simpleParamsSize);
  uint32_t actualSize = 0;
  dbDesc.deserializeSimpleParams(
      pre_allocated_mem_buffers_.last_exit_from_view_simple_element_buf.get(), simpleParamsSize, actualSize);

  const size_t maxElementSize = DescriptorOfLastExitFromView::maxElementSize();
  uint32_t actualElementSize = 0;
  uint32_t elementsNum = dbDesc.elements.size();
  for (uint32_t i = 0; i < elementsNum; ++i) {
    uint32_t itemId = LAST_EXIT_FROM_VIEW_DESC + 1 + i;
    ConcordAssertLT(itemId, LAST_COMPLAINTS_DESC);
    metadataStorage_->read(
        itemId, maxElementSize, pre_allocated_mem_buffers_.last_exit_fron_view_element_buf.get(), actualSize);
    dbDesc.deserializeElement(
        i, pre_allocated_mem_buffers_.last_exit_fron_view_element_buf.get(), actualSize, actualElementSize);
    ConcordAssertNE(actualElementSize, 0);
  }

  const size_t maxComplaintSize = DescriptorOfLastExitFromView::maxComplaintSize();
  uint32_t actualComplaintSize = 0;
  uint32_t complaintsNum = dbDesc.complaints.size();
  for (uint32_t i = 0; i < complaintsNum; ++i) {
    uint32_t itemId = LAST_COMPLAINTS_DESC + i;
    ConcordAssertLT(itemId, LAST_EXEC_DESC);
    metadataStorage_->read(
        itemId, maxComplaintSize, pre_allocated_mem_buffers_.last_exit_from_view_complaint_buf.get(), actualSize);
    dbDesc.deserializeComplaint(
        i, pre_allocated_mem_buffers_.last_exit_from_view_complaint_buf.get(), actualSize, actualComplaintSize);
    ConcordAssertNE(actualComplaintSize, 0);
  }

  return dbDesc;
}

DescriptorOfLastNewView PersistentStorageImp::getAndAllocateDescriptorOfLastNewView() {
  DescriptorOfLastNewView dbDesc;
  const size_t simpleParamsSize = DescriptorOfLastNewView::simpleParamsSize();
  uint32_t actualSize = 0;

  // Read first simple params.
  metadataStorage_->read(LAST_NEW_VIEW_DESC,
                         simpleParamsSize,
                         pre_allocated_mem_buffers_.last_new_view_simple_element_buf.get(),
                         actualSize);
  dbDesc.deserializeSimpleParams(
      pre_allocated_mem_buffers_.last_new_view_simple_element_buf.get(), simpleParamsSize, actualSize);

  size_t maxElementSize = DescriptorOfLastNewView::maxElementSize();
  size_t actualElementSize = 0;
  uint32_t viewChangeMsgsNum = DescriptorOfLastNewView::getViewChangeMsgsNum();
  for (uint32_t i = 0; i < viewChangeMsgsNum; ++i) {
    metadataStorage_->read(LAST_NEW_VIEW_DESC + 1 + i,
                           maxElementSize,
                           pre_allocated_mem_buffers_.last_new_view_element_buf.get(),
                           actualSize);
    dbDesc.deserializeElement(
        i, pre_allocated_mem_buffers_.last_new_view_element_buf.get(), actualSize, actualElementSize);
    ConcordAssertNE(actualElementSize, 0);
  }
  return dbDesc;
}

DescriptorOfLastExecution PersistentStorageImp::getDescriptorOfLastExecution() {
  DescriptorOfLastExecution dbDesc;
  const size_t maxSize = DescriptorOfLastExecution::maxSize();
  uint32_t actualSize = 0;

  metadataStorage_->read(
      LAST_EXEC_DESC, maxSize, pre_allocated_mem_buffers_.descriptor_of_last_execution_buf.get(), actualSize);
  dbDesc.deserialize(pre_allocated_mem_buffers_.descriptor_of_last_execution_buf.get(), maxSize, actualSize);
  ConcordAssertNE(actualSize, 0);
  if (!dbDesc.equals(emptyDescriptorOfLastExecution_)) {
    descriptorOfLastExecution_ = dbDesc;
    hasDescriptorOfLastExecution_ = true;
  }
  return dbDesc;
}

DescriptorOfLastStableCheckpoint PersistentStorageImp::getDescriptorOfLastStableCheckpoint() {
  ConcordAssert(getIsAllowed());
  DescriptorOfLastStableCheckpoint dbDesc{numReplicas_, {}};
  uint32_t dbDescSize = DescriptorOfLastStableCheckpoint::maxSize(numReplicas_);
  uint32_t sizeInDb = 0;

  metadataStorage_->read(LAST_STABLE_CHECKPOINT_DESC,
                         dbDescSize,
                         pre_allocated_mem_buffers_.descriptor_og_last_stable_cp_buf.get(),
                         sizeInDb);

  size_t actualSize = 0;
  dbDesc.deserialize(pre_allocated_mem_buffers_.descriptor_og_last_stable_cp_buf.get(), dbDescSize, actualSize);

  return dbDesc;
}

bool PersistentStorageImp::hasDescriptorOfLastExitFromView() {
  if (!hasDescriptorOfLastExitFromView_) {
    DescriptorOfLastExitFromView storedDesc = getAndAllocateDescriptorOfLastExitFromView();
    if (!storedDesc.isDefault) hasDescriptorOfLastExitFromView_ = true;
    storedDesc.clean();
  }
  return hasDescriptorOfLastExitFromView_;
}

bool PersistentStorageImp::hasDescriptorOfLastNewView() {
  if (!hasDescriptorOfLastNewView_) {
    DescriptorOfLastNewView storedDesc = getAndAllocateDescriptorOfLastNewView();
    if (!storedDesc.isDefault) hasDescriptorOfLastNewView_ = true;
    storedDesc.clean();
  }
  return hasDescriptorOfLastNewView_;
}

bool PersistentStorageImp::hasDescriptorOfLastExecution() { return hasDescriptorOfLastExecution_; }

/***** Windows handling *****/

/***** Private functions *****/

void PersistentStorageImp::readSeqNumDataElementFromDisk(SeqNum index, const SharedPtrSeqNumWindow &seqNumWindow) {
  uint32_t actualElementSize = 0;
  uint32_t actualParameterSize = 0;
  char *buf = pre_allocated_mem_buffers_.seq_num_element_buf.get();
  SeqNum shift = index * numOfSeqNumWinParameters;
  for (auto i = 0; i < numOfSeqNumWinParameters; ++i) {
    uint32_t itemId = BEGINNING_OF_SEQ_NUM_WINDOW + SEQ_NUM_FIRST_PARAM + i + shift;
    ConcordAssertLT(itemId, BEGINNING_OF_CHECK_WINDOW);
    metadataStorage_->read(itemId, SeqNumData::maxSize(), buf, actualParameterSize);
    buf += actualParameterSize;
    actualElementSize += actualParameterSize;
  }
  uint32_t actualSize = 0;
  seqNumWindow.get()->deserializeElement(
      index, pre_allocated_mem_buffers_.seq_num_element_buf.get(), actualElementSize, actualSize);
}

void PersistentStorageImp::readCheckDataElementFromDisk(SeqNum index, const SharedPtrCheckWindow &checkWindow) {
  uint32_t actualElementSize = 0;
  uint32_t actualParameterSize = 0;
  char *buf = pre_allocated_mem_buffers_.check_data_element_buf.get();
  const SeqNum shift = index * numOfCheckWinParameters;
  for (auto i = 0; i < numOfCheckWinParameters; ++i) {
    uint32_t itemId = BEGINNING_OF_CHECK_WINDOW + CHECK_DATA_FIRST_PARAM + i + shift;
    ConcordAssertLT(itemId, WIN_PARAMETERS_NUM);
    metadataStorage_->read(itemId, CheckData::maxSize(), buf, actualParameterSize);
    buf += actualParameterSize;
    actualElementSize += actualParameterSize;
  }
  uint32_t actualSize = 0;
  checkWindow.get()->deserializeElement(
      index, pre_allocated_mem_buffers_.check_data_element_buf.get(), CheckData::maxSize(), actualSize);
  ConcordAssertEQ(actualSize, actualElementSize);
}

const SeqNum PersistentStorageImp::convertSeqNumWindowIndex(SeqNum seqNum) const {
  SeqNum convertedIndex = SeqNumWindow::convertIndex(seqNum, seqNumWindowBeginning_);
  LOG_DEBUG(GL,
            "convertSeqNumWindowIndex seqNumWindowBeginning_=" << seqNumWindowBeginning_ << " seqNum=" << seqNum
                                                               << " convertedIndex=" << convertedIndex);
  return convertedIndex * numOfSeqNumWinParameters;
}

uint8_t PersistentStorageImp::readOneByteFromDisk(SeqNum index, SeqNum parameterId) const {
  uint8_t oneByte = 0;
  uint32_t actualSize = 0;
  const SeqNum convertedIndex = BEGINNING_OF_SEQ_NUM_WINDOW + parameterId + convertSeqNumWindowIndex(index);
  ConcordAssertLT(convertedIndex, BEGINNING_OF_CHECK_WINDOW);
  metadataStorage_->read(convertedIndex, sizeof(oneByte), (char *)&oneByte, actualSize);
  return oneByte;
}

MessageBase *PersistentStorageImp::readMsgFromDisk(SeqNum seqNum, SeqNum parameterId, size_t msgSize) const {
  char *buf = pre_allocated_mem_buffers_.msg_element_buf.get();
  uint32_t actualMsgSize = 0;
  const SeqNum convertedIndex = BEGINNING_OF_SEQ_NUM_WINDOW + parameterId + convertSeqNumWindowIndex(seqNum);
  ConcordAssertLT(convertedIndex, BEGINNING_OF_CHECK_WINDOW);
  LOG_DEBUG(GL, "PersistentStorageImp::readMsgFromDisk seqNum=" << seqNum << " dbIndex=" << convertedIndex);
  metadataStorage_->read(convertedIndex, msgSize, buf, actualMsgSize);
  size_t actualSize = 0;
  buf = pre_allocated_mem_buffers_.msg_element_buf.get();
  auto *msg = SeqNumData::deserializeMsg(buf, msgSize, actualSize);
  ConcordAssertEQ(actualSize, actualMsgSize);
  return msg;
}

PrePrepareMsg *PersistentStorageImp::readPrePrepareMsgFromDisk(SeqNum seqNum) const {
  return (PrePrepareMsg *)readMsgFromDisk(seqNum, PRE_PREPARE_MSG, SeqNumData::maxMessageSize<PrePrepareMsg>());
}

FullCommitProofMsg *PersistentStorageImp::readFullCommitProofMsgFromDisk(SeqNum seqNum) const {
  return (FullCommitProofMsg *)readMsgFromDisk(
      seqNum, FULL_COMMIT_PROOF_MSG, SeqNumData::maxMessageSize<FullCommitProofMsg>());
}

PrepareFullMsg *PersistentStorageImp::readPrepareFullMsgFromDisk(SeqNum seqNum) const {
  return (PrepareFullMsg *)readMsgFromDisk(seqNum, PRE_PREPARE_FULL_MSG, SeqNumData::maxMessageSize<PrepareFullMsg>());
}

CommitFullMsg *PersistentStorageImp::readCommitFullMsgFromDisk(SeqNum seqNum) const {
  return (CommitFullMsg *)readMsgFromDisk(seqNum, COMMIT_FULL_MSG, SeqNumData::maxMessageSize<CommitFullMsg>());
}

const SeqNum PersistentStorageImp::convertCheckWindowIndex(SeqNum index) const {
  SeqNum convertedIndex = CheckWindow::convertIndex(index, checkWindowBeginning_);
  return convertedIndex * numOfCheckWinParameters;
}

uint8_t PersistentStorageImp::readCompletedMarkFromDisk(SeqNum index) const {
  uint8_t completedMark = 0;
  uint32_t actualSize = 0;
  const SeqNum convertedIndex = BEGINNING_OF_CHECK_WINDOW + COMPLETED_MARK + convertCheckWindowIndex(index);
  ConcordAssertLT(convertedIndex, WIN_PARAMETERS_NUM);
  metadataStorage_->read(convertedIndex, sizeof(completedMark), (char *)&completedMark, actualSize);
  ConcordAssertEQ(sizeof(completedMark), actualSize);
  return completedMark;
}

CheckpointMsg *PersistentStorageImp::readCheckpointMsgFromDisk(SeqNum index) const {
  const size_t bufLen = CheckData::maxSize();
  char *buf = pre_allocated_mem_buffers_.check_data_element_buf.get();
  uint32_t actualMsgSize = 0;
  const SeqNum convertedIndex = BEGINNING_OF_CHECK_WINDOW + CHECKPOINT_MSG + convertCheckWindowIndex(index);
  ConcordAssertLT(convertedIndex, WIN_PARAMETERS_NUM);
  LOG_DEBUG(GL, "PersistentStorageImp::readCheckpointMsgFromDisk convertedIndex=" << convertedIndex);
  metadataStorage_->read(convertedIndex, bufLen, buf, actualMsgSize);
  size_t actualSize = 0;
  buf = pre_allocated_mem_buffers_.check_data_element_buf.get();
  auto *checkpointMsg = CheckData::deserializeCheckpointMsg(buf, bufLen, actualSize);
  ConcordAssertEQ(actualSize, actualMsgSize);
  return checkpointMsg;
}

SeqNum PersistentStorageImp::readBeginningOfActiveWindow(uint32_t index) const {
  const size_t paramSize = sizeof(SeqNum);
  uint32_t actualSize = 0;
  SeqNum beginningOfActiveWindow = 0;
  metadataStorage_->read(index, paramSize, (char *)&beginningOfActiveWindow, actualSize);
  ConcordAssertEQ(actualSize, paramSize);
  return beginningOfActiveWindow;
}

/***** Public functions *****/

SharedPtrSeqNumWindow PersistentStorageImp::getSeqNumWindow() {
  auto windowBeginning = readBeginningOfActiveWindow(BEGINNING_OF_SEQ_NUM_WINDOW);
  SharedPtrSeqNumWindow seqNumWindow(new SeqNumWindow(windowBeginning));

  for (auto i = 0; i < seqWinSize; ++i) readSeqNumDataElementFromDisk(i, seqNumWindow);
  return seqNumWindow;
}

SharedPtrCheckWindow PersistentStorageImp::getCheckWindow() {
  auto windowBeginning = readBeginningOfActiveWindow(BEGINNING_OF_CHECK_WINDOW);
  SharedPtrCheckWindow checkWindow(new CheckWindow(windowBeginning));

  for (auto i = 0; i < checkWinSize; ++i) readCheckDataElementFromDisk(i, checkWindow);
  return checkWindow;
}

CheckpointMsg *PersistentStorageImp::getAndAllocateCheckpointMsgInCheckWindow(SeqNum seqNum) {
  ConcordAssert(getIsAllowed());
  return readCheckpointMsgFromDisk(seqNum);
}

bool PersistentStorageImp::getCompletedMarkInCheckWindow(SeqNum seqNum) {
  ConcordAssert(getIsAllowed());
  return readCompletedMarkFromDisk(seqNum);
}

std::optional<std::vector<std::uint8_t>> PersistentStorageImp::getUserData() const {
  auto buf = std::vector<std::uint8_t>(kMaxUserDataSizeBytes);
  auto actualSize = std::uint32_t{0};
  metadataStorage_->read(USER_DATA, buf.size(), reinterpret_cast<char *>(buf.data()), actualSize);
  if (0 == actualSize) {
    return std::nullopt;
  }
  buf.resize(actualSize);
  return buf;
}

PrePrepareMsg *PersistentStorageImp::getAndAllocatePrePrepareMsgInSeqNumWindow(SeqNum seqNum) {
  ConcordAssert(getIsAllowed());
  return readPrePrepareMsgFromDisk(seqNum);
}

FullCommitProofMsg *PersistentStorageImp::getAndAllocateFullCommitProofMsgInSeqNumWindow(SeqNum seqNum) {
  ConcordAssert(getIsAllowed());
  return readFullCommitProofMsgFromDisk(seqNum);
}

PrepareFullMsg *PersistentStorageImp::getAndAllocatePrepareFullMsgInSeqNumWindow(SeqNum seqNum) {
  ConcordAssert(getIsAllowed());
  return readPrepareFullMsgFromDisk(seqNum);
}

CommitFullMsg *PersistentStorageImp::getAndAllocateCommitFullMsgInSeqNumWindow(SeqNum seqNum) {
  ConcordAssert(getIsAllowed());
  return readCommitFullMsgFromDisk(seqNum);
}

bool PersistentStorageImp::getForceCompletedInSeqNumWindow(SeqNum seqNum) {
  ConcordAssert(getIsAllowed());
  return readOneByteFromDisk(seqNum, FORCE_COMPLETED);
}

bool PersistentStorageImp::getSlowStartedInSeqNumWindow(SeqNum seqNum) {
  ConcordAssert(getIsAllowed());
  return readOneByteFromDisk(seqNum, SLOW_STARTED);
}

/***** Verification/helper functions *****/

void PersistentStorageImp::verifySetDescriptorOfLastExitFromView(const DescriptorOfLastExitFromView &desc) {
  ConcordAssert(setIsAllowed());
  ConcordAssertGE(desc.view, 0);
  // Here we assume that the first view is always 0
  // (even if we load the initial state from disk)
  ConcordAssertOR(hasDescriptorOfLastNewView(), (desc.view == 0));
  ConcordAssertLE(desc.elements.size(), kWorkWindowSize);
}

void PersistentStorageImp::verifyPrevViewInfo(const DescriptorOfLastExitFromView &desc) const {
  for (auto elem : desc.elements) {
    ConcordAssertEQ(elem.prePrepare->viewNumber(), desc.view);
    ConcordAssertOR((elem.prepareFull == nullptr), (elem.prepareFull->viewNumber() == desc.view));
    ConcordAssertOR((elem.prepareFull == nullptr), (elem.prepareFull->seqNumber() == elem.prePrepare->seqNumber()));
  }
}

void PersistentStorageImp::verifyLastNewViewMsgs(const DescriptorOfLastNewView &desc) const {
  for (uint32_t i = 0; i < desc.viewChangeMsgs.size(); i++) {
    const ViewChangeMsg *viewChangeMsg = desc.viewChangeMsgs[i];
    ConcordAssertEQ(viewChangeMsg->newView(), desc.view);
    Digest digestOfViewChangeMsg;
    viewChangeMsg->getMsgDigest(digestOfViewChangeMsg);
    const NewViewMsg *newViewMsg = desc.newViewMsg;
    if (newViewMsg->elementsCount())
      ConcordAssert(
          newViewMsg->includesViewChangeFromReplica(viewChangeMsg->idOfGeneratedReplica(), digestOfViewChangeMsg));
  }
}

void PersistentStorageImp::verifySetDescriptorOfLastNewView(const DescriptorOfLastNewView &desc) {
  ConcordAssert(setIsAllowed());
  ConcordAssertGE(desc.view, 1);
  ConcordAssert(hasDescriptorOfLastExitFromView());
  ConcordAssertEQ(desc.newViewMsg->newView(), desc.view);
  const size_t numOfVCMsgs = 2 * fVal_ + 2 * cVal_ + 1;
  ConcordAssertEQ(desc.viewChangeMsgs.size(), numOfVCMsgs);
}

void PersistentStorageImp::verifyDescriptorOfLastExecution(const DescriptorOfLastExecution &desc) {
  ConcordAssert(setIsAllowed());
  ConcordAssertOR(!hasDescriptorOfLastExecution(), (descriptorOfLastExecution_.executedSeqNum < desc.executedSeqNum));
  ConcordAssertEQ(lastExecutedSeqNum_ + 1, desc.executedSeqNum);
  ConcordAssertGE(desc.validRequests.numOfBits(), 1);
  ConcordAssertLE(desc.validRequests.numOfBits(), maxNumOfRequestsInBatch);
}

// Helper function for getting different kinds of sequence numbers.
SeqNum PersistentStorageImp::getSeqNum(ConstMetadataParameterIds id, uint32_t size) {
  uint32_t actualObjectSize = 0;
  SeqNum seqNum = 0;
  metadataStorage_->read(id, size, (char *)&seqNum, actualObjectSize);
  ConcordAssertEQ(actualObjectSize, size);
  return seqNum;
}

bool PersistentStorageImp::setIsAllowed() const {
  LOG_DEBUG(GL, "isInWriteTran=" << isInWriteTran());
  return isInWriteTran();
}

bool PersistentStorageImp::getIsAllowed() const {
  LOG_DEBUG(GL, "isInWriteTran=" << isInWriteTran());
  return (!isInWriteTran());
}

bool PersistentStorageImp::nonExecSetIsAllowed() {
  return setIsAllowed() &&
         (!hasDescriptorOfLastExecution() || descriptorOfLastExecution_.executedSeqNum <= lastExecutedSeqNum_ + 1);
}
void PersistentStorageImp::setEraseMetadataStorageFlag() {
  bool eraseMtOnStartUp = true;
  metadataStorage_->atomicWrite(ERASE_METADATA_ON_STARTUP, (char *)&eraseMtOnStartUp, sizeof(eraseMtOnStartUp));
}

bool PersistentStorageImp::getEraseMetadataStorageFlag() {
  uint32_t actualObjectSize = 0;
  bool eraseMetaDataOnStartup = false;
  metadataStorage_->read(
      ERASE_METADATA_ON_STARTUP, sizeof(eraseMetaDataOnStartup), (char *)&eraseMetaDataOnStartup, actualObjectSize);
  if (actualObjectSize == 0) return false;
  return eraseMetaDataOnStartup;
}

void PersistentStorageImp::setNewEpochFlag(bool flag) {
  bool newEpoch = flag;
  metadataStorage_->atomicWrite(START_NEW_EPOCH, (char *)&newEpoch, sizeof(newEpoch));
}

bool PersistentStorageImp::getNewEpochFlag() {
  uint32_t actualObjectSize = 0;
  bool newEpoch = false;
  metadataStorage_->read(START_NEW_EPOCH, sizeof(newEpoch), (char *)&newEpoch, actualObjectSize);
  if (actualObjectSize == 0) return false;
  return newEpoch;
}

void PersistentStorageImp::eraseMetadata() { metadataStorage_->eraseData(); }

void PersistentStorageImp::setDbCheckpointMetadata(const std::vector<std::uint8_t> &v) {
  std::string data(v.begin(), v.end());
  metadataStorage_->atomicWrite(DB_CHECKPOINT_DESCRIPTOR, data.data(), data.size());
}
std::optional<std::vector<std::uint8_t>> PersistentStorageImp::getDbCheckpointMetadata(const uint32_t &maxBuffSz) {
  uint32_t outActualObjectSize = 0;
  UniquePtrToChar outBuf(new char[maxBuffSz]);
  char *outBufPtr = outBuf.get();
  metadataStorage_->read(DB_CHECKPOINT_DESCRIPTOR, maxBuffSz, outBufPtr, outActualObjectSize);
  if (!outActualObjectSize)  // Parameter not found
    return std::nullopt;
  std::vector<uint8_t> v(outActualObjectSize, 0);
  for (auto i = 0u; i < outActualObjectSize; i++) v[i] = static_cast<uint8_t>(outBuf[i]);
  return v;
}

}  // namespace impl
}  // namespace bftEngine
