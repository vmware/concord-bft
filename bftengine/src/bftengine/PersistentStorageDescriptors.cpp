// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "PersistentStorageDescriptors.hpp"

using namespace std;

namespace bftEngine {

/***** DescriptorOfLastExitFromView *****/

DescriptorOfLastExitFromView::~DescriptorOfLastExitFromView() {
  for (auto elem : elements) {
    delete elem.prepareFull;
    delete elem.prePrepare;
  }
}

void DescriptorOfLastExitFromView::setTo(
    const DescriptorOfLastExitFromView &other) {
  view = other.view;
  lastStable = other.lastStable;
  lastExecuted = other.lastExecuted;
  for (auto elem : elements) {
    delete elem.prePrepare;
    delete elem.prepareFull;
  }
  elements.clear();
  elements = other.elements;
}

bool DescriptorOfLastExitFromView::operator==(
    const DescriptorOfLastExitFromView &other) const {
  return (other.view == view &&
      other.lastStable == lastStable &&
      other.lastExecuted == lastExecuted &&
      other.elements == elements);
}

void DescriptorOfLastExitFromView::serializeSimpleParams(
    char *&buf, size_t bufLen) const {
  Assert(bufLen >= simpleParamsSize());

  memcpy(buf, &view, sizeof(view));
  buf += sizeof(view);

  memcpy(buf, &lastStable, sizeof(lastStable));
  buf += sizeof(lastStable);

  memcpy(buf, &lastExecuted, sizeof(lastExecuted));
  buf += sizeof(lastExecuted);

  uint32_t elementsNum = elements.size();
  memcpy(buf, &elementsNum, sizeof(elementsNum));
  buf += sizeof(elementsNum);
}

void DescriptorOfLastExitFromView::serializeElement(
    uint32_t id, char *&buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(id < elements.size());
  bool msgEmptyFlag = false;
  size_t msgEmptyFlagSize = sizeof(msgEmptyFlag);
  size_t hasAllRequestsSize = sizeof(elements[id].hasAllRequests);

  size_t prePrepareMsgSize = 0;
  if (elements[id].prePrepare)
    prePrepareMsgSize =
        elements[id].prePrepare->sizeNeededForObjAndMsgInLocalBuffer();

  size_t prepareFullMsgSize = 0;
  if (elements[id].prepareFull)
    prepareFullMsgSize =
        elements[id].prepareFull->sizeNeededForObjAndMsgInLocalBuffer();

  actualSize = 2 * msgEmptyFlagSize + prePrepareMsgSize + prepareFullMsgSize +
      hasAllRequestsSize;
  Assert(actualSize <= bufLen);

  // As messages could be empty (nullptr), an additional flag is required to
  // distinguish between empty and full ones.
  msgEmptyFlag = (elements[id].prePrepare == nullptr);
  memcpy(buf, &msgEmptyFlag, msgEmptyFlagSize);
  buf += msgEmptyFlagSize;

  size_t msgSize = 0;
  if (prePrepareMsgSize) {
    elements[id].prePrepare->writeObjAndMsgToLocalBuffer(buf, prePrepareMsgSize,
                                                         &msgSize);
    buf += prePrepareMsgSize;
  }

  msgEmptyFlag = (elements[id].prepareFull == nullptr);
  memcpy(buf, &msgEmptyFlag, msgEmptyFlagSize);
  buf += msgEmptyFlagSize;

  if (prepareFullMsgSize) {
    elements[id].prepareFull->writeObjAndMsgToLocalBuffer(
        buf, prepareFullMsgSize, &msgSize);
    buf += prepareFullMsgSize;
  }

  memcpy(buf, &elements[id].hasAllRequests, hasAllRequestsSize);
  buf += hasAllRequestsSize;
}

void DescriptorOfLastExitFromView::deserializeSimpleParams(
    char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  Assert(bufLen >= simpleParamsSize());

  size_t viewSize = sizeof(view);
  memcpy(&view, buf, viewSize);
  buf += viewSize;

  size_t lastStableSize = sizeof(lastStable);
  memcpy(&lastStable, buf, lastStableSize);
  buf += lastStableSize;

  size_t lastExecutedSize = sizeof(lastExecuted);
  memcpy(&lastExecuted, buf, lastExecutedSize);
  buf += lastExecutedSize;

  uint32_t elementsNum;
  size_t elementsNumSize = sizeof(elementsNum);
  memcpy(&elementsNum, buf, elementsNumSize);

  if (elementsNum)
    elements.resize(elementsNum);

  actualSize = viewSize + lastStableSize + lastExecutedSize + elementsNumSize;
}

void DescriptorOfLastExitFromView::deserializeElement(
    char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  Assert(bufLen >= maxElementSize());

  bool msgEmptyFlag = false;
  size_t msgEmptyFlagSize = sizeof(msgEmptyFlag);
  memcpy(&msgEmptyFlag, buf, msgEmptyFlagSize);
  buf += msgEmptyFlagSize;

  size_t prePrepareMsgSize = 0;
  MessageBase *prePrepareMsgPtr = nullptr;
  if (!msgEmptyFlag) {
    prePrepareMsgPtr = PrePrepareMsg::createObjAndMsgFromLocalBuffer(
        buf, bufLen, &prePrepareMsgSize);
    Assert(prePrepareMsgSize);
    buf += prePrepareMsgSize;
  }

  memcpy(&msgEmptyFlag, buf, msgEmptyFlagSize);
  buf += msgEmptyFlagSize;

  size_t prepareFullMsgSize = 0;
  MessageBase *prepareFullMsgPtr = nullptr;
  if (!msgEmptyFlag) {
    prepareFullMsgPtr = PrepareFullMsg::createObjAndMsgFromLocalBuffer(
        buf, bufLen, &prepareFullMsgSize);
    Assert(prepareFullMsgSize);
    buf += prepareFullMsgSize;
  }

  bool hasAllRequests = false;
  size_t hasAllRequestsSize = sizeof(hasAllRequestsSize);
  memcpy(&hasAllRequests, buf, hasAllRequestsSize);

  actualSize = 2 * msgEmptyFlagSize + prePrepareMsgSize + prepareFullMsgSize +
      hasAllRequestsSize;

  elements.push_back(
      ViewsManager::PrevViewInfo((PrePrepareMsg *) prePrepareMsgPtr,
                                 (PrepareFullMsg *) prepareFullMsgPtr,
                                 hasAllRequests));
}

/***** DescriptorOfLastNewView *****/

void DescriptorOfLastNewView::setTo(const DescriptorOfLastNewView &other) {
  view = other.view;
  maxSeqNumTransferredFromPrevViews = other.maxSeqNumTransferredFromPrevViews;
  delete newViewMsg;
  newViewMsg = (NewViewMsg *) other.newViewMsg->cloneObjAndMsg();
  for (auto elem : viewChangeMsgs)
    delete elem;
  viewChangeMsgs.clear();
  for (uint32_t i = 0; i < other.viewChangeMsgs.size(); ++i)
    viewChangeMsgs[i] =
        (ViewChangeMsg *) other.viewChangeMsgs[i]->cloneObjAndMsg();
}

DescriptorOfLastNewView::~DescriptorOfLastNewView() {
  delete newViewMsg;
  for (auto msg : viewChangeMsgs)
    delete msg;
}

bool DescriptorOfLastNewView::operator==(
    const DescriptorOfLastNewView &other) const {
  return (other.view == view &&
      *other.newViewMsg == *newViewMsg &&
      other.viewChangeMsgs == viewChangeMsgs &&
      other.maxSeqNumTransferredFromPrevViews ==
          maxSeqNumTransferredFromPrevViews);
}

void DescriptorOfLastNewView::serializeSimpleParams(
    char *&buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(bufLen >= simpleParamsSize());

  size_t viewSize = sizeof(view);
  memcpy(buf, &view, viewSize);
  buf += viewSize;

  size_t maxSeqNumSize = sizeof(maxSeqNumTransferredFromPrevViews);
  memcpy(buf, &maxSeqNumTransferredFromPrevViews, maxSeqNumSize);
  buf += maxSeqNumSize;

  bool msgEmptyFlag = (newViewMsg == nullptr);
  size_t msgEmptyFLagSize = sizeof(msgEmptyFlag);
  memcpy(&msgEmptyFlag, buf, msgEmptyFLagSize);
  buf += msgEmptyFLagSize;

  size_t msgSize = 0;
  if (newViewMsg) {
    newViewMsg->writeObjAndMsgToLocalBuffer(
        buf, newViewMsg->sizeNeededForObjAndMsgInLocalBuffer(), &msgSize);
    Assert(msgSize);
    buf += msgSize;
  }
  actualSize = viewSize + maxSeqNumSize + msgEmptyFLagSize + msgSize;
}

void DescriptorOfLastNewView::serializeElement(
    uint32_t id, char *&buf, size_t bufLen, size_t &actualSize) const {
  Assert(id < viewChangeMsgs.size());
  size_t msgSize = viewChangeMsgs[id]->sizeNeededForObjAndMsgInLocalBuffer();
  Assert(msgSize <= bufLen);
  viewChangeMsgs[id]->writeObjAndMsgToLocalBuffer(buf, msgSize, &actualSize);
  buf += msgSize;
}

void DescriptorOfLastNewView::deserializeSimpleParams(
    char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  Assert(bufLen >= simpleParamsSize());

  size_t viewSize = sizeof(view);
  memcpy(&view, buf, viewSize);
  buf += viewSize;

  size_t maxSeqNumSize = sizeof(maxSeqNumTransferredFromPrevViews);
  memcpy(&maxSeqNumTransferredFromPrevViews, buf, maxSeqNumSize);
  buf += maxSeqNumSize;

  bool msgEmptyFlag = false;
  size_t msgEmptyFlagSize = sizeof(msgEmptyFlag);
  memcpy(&msgEmptyFlag, buf, msgEmptyFlagSize);
  buf += msgEmptyFlagSize;

  size_t msgSize = 0;
  newViewMsg = nullptr;
  if (!msgEmptyFlag) {
    newViewMsg = (NewViewMsg *) NewViewMsg::createObjAndMsgFromLocalBuffer(
        buf, bufLen, &msgSize);
    Assert(msgSize);
  }
  actualSize = viewSize + maxSeqNumSize + msgEmptyFlagSize + msgSize;
}

void DescriptorOfLastNewView::deserializeElement(
    char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  Assert(bufLen >= maxElementSize());

  viewChangeMsgs.push_back(
      (ViewChangeMsg *) MessageBase::createObjAndMsgFromLocalBuffer(
          buf, bufLen, (size_t *) &actualSize));
}

/***** DescriptorOfLastExecution *****/

bool DescriptorOfLastExecution::operator==(
    const DescriptorOfLastExecution &other) const {
  return (other.executedSeqNum == executedSeqNum &&
      other.validRequests == validRequests);
}

void DescriptorOfLastExecution::serialize(
    char *&buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(bufLen >= maxSize());

  memcpy(buf, &executedSeqNum, sizeof(executedSeqNum));
  buf += sizeof(executedSeqNum);

  uint32_t bitMapSize = 0;
  validRequests.writeToBuffer(buf, validRequests.sizeNeededInBuffer(),
                              &bitMapSize);
  buf += bitMapSize;
  actualSize = sizeof(executedSeqNum) + bitMapSize;
}

void DescriptorOfLastExecution::deserialize(
    char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;

  Assert(bufLen >= maxSize())

  size_t sizeofSeqNum = sizeof(executedSeqNum);
  memcpy(&executedSeqNum, buf, sizeofSeqNum);
  buf += sizeofSeqNum;

  uint32_t bitMapSize = 0;
  validRequests = *Bitmap::createBitmapFromBuffer(
      buf, bufLen - sizeofSeqNum, &bitMapSize);
  Assert(bitMapSize);
  actualSize = sizeofSeqNum + bitMapSize;
}

}  // namespace bftEngine
