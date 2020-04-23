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
namespace impl {

/***** DescriptorOfLastExitFromView *****/

void DescriptorOfLastExitFromView::clean() {
  delete myViewChangeMsg;
  myViewChangeMsg = nullptr;
  for (auto elem : elements) {
    delete elem.prepareFull;
    delete elem.prePrepare;
  }
  elements.clear();
}

bool DescriptorOfLastExitFromView::equals(const DescriptorOfLastExitFromView &other) const {
  if (other.elements.size() != elements.size()) return false;

  if ((other.myViewChangeMsg && !myViewChangeMsg) || (!other.myViewChangeMsg && myViewChangeMsg)) return false;
  bool res = myViewChangeMsg ? (other.myViewChangeMsg->equals(*myViewChangeMsg)) : true;
  if (!res) return false;

  for (uint32_t i = 0; i < elements.size(); ++i)
    if (!elements[i].equals(other.elements[i])) return false;
  return (other.view == view && other.lastStable == lastStable && other.lastExecuted == lastExecuted &&
          other.stableLowerBoundWhenEnteredToView == stableLowerBoundWhenEnteredToView);
}

void DescriptorOfLastExitFromView::serializeSimpleParams(char *buf, size_t bufLen, size_t &actualSize) const {
  Assert(bufLen >= simpleParamsSize());

  size_t viewSize = sizeof(view);
  memcpy(buf, &view, viewSize);
  buf += viewSize;

  size_t lastStableSize = sizeof(lastStable);
  memcpy(buf, &lastStable, lastStableSize);
  buf += lastStableSize;

  size_t lastExecutedSize = sizeof(lastExecuted);
  memcpy(buf, &lastExecuted, lastExecutedSize);
  buf += lastExecutedSize;

  size_t stableLowerBoundWhenEnteredToViewSize = sizeof(stableLowerBoundWhenEnteredToView);
  memcpy(buf, &stableLowerBoundWhenEnteredToView, stableLowerBoundWhenEnteredToViewSize);
  buf += stableLowerBoundWhenEnteredToViewSize;

  size_t myViewChangeMsgSize = MessageBase::serializeMsg(buf, myViewChangeMsg);

  uint32_t elementsNum = elements.size();
  size_t elementsNumSize = sizeof(elementsNum);
  memcpy(buf, &elementsNum, elementsNumSize);

  actualSize = viewSize + lastStableSize + lastExecutedSize + stableLowerBoundWhenEnteredToViewSize +
               myViewChangeMsgSize + elementsNumSize;
}

void DescriptorOfLastExitFromView::serializeElement(uint32_t id, char *buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(id < elements.size());
  Assert(bufLen >= maxElementSize());

  PrePrepareMsg *prePrepareMsg = (id < elements.size()) ? elements[id].prePrepare : nullptr;
  PrepareFullMsg *prePrepareFullMsg = (id < elements.size()) ? elements[id].prepareFull : nullptr;

  actualSize += MessageBase::serializeMsg(buf, prePrepareMsg);
  actualSize += MessageBase::serializeMsg(buf, prePrepareFullMsg);

  size_t hasAllRequestsSize = sizeof(elements[id].hasAllRequests);
  memcpy(buf, &elements[id].hasAllRequests, hasAllRequestsSize);
  actualSize += hasAllRequestsSize;
}

void DescriptorOfLastExitFromView::deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize) {
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

  size_t stableLowerBoundWhenEnteredToViewSize = sizeof(stableLowerBoundWhenEnteredToView);
  memcpy(&stableLowerBoundWhenEnteredToView, buf, stableLowerBoundWhenEnteredToViewSize);
  buf += stableLowerBoundWhenEnteredToViewSize;

  size_t actualMsgSize = 0;
  myViewChangeMsg = (ViewChangeMsg *)MessageBase::deserializeMsg(buf, bufLen, actualMsgSize);

  uint32_t elementsNum;
  size_t elementsNumSize = sizeof(elementsNum);
  memcpy(&elementsNum, buf, elementsNumSize);

  if (elementsNum) elements.resize(elementsNum);

  actualSize = viewSize + lastStableSize + lastExecutedSize + stableLowerBoundWhenEnteredToViewSize + actualMsgSize +
               elementsNumSize;
}

void DescriptorOfLastExitFromView::deserializeElement(uint32_t id, char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;

  size_t msgSize1 = 0, msgSize2 = 0;
  auto *prePrepareMsgPtr = MessageBase::deserializeMsg(buf, bufLen, msgSize1);
  auto *prepareFullMsgPtr = MessageBase::deserializeMsg(buf, bufLen, msgSize2);

  bool hasAllRequests = false;
  size_t hasAllRequestsSize = sizeof(hasAllRequests);
  memcpy(&hasAllRequests, buf, hasAllRequestsSize);

  Assert(elements[id].prePrepare == nullptr);
  Assert(elements[id].prepareFull == nullptr);

  elements[id] = ViewsManager::PrevViewInfo(
      (PrePrepareMsg *)prePrepareMsgPtr, (PrepareFullMsg *)prepareFullMsgPtr, hasAllRequests);
  actualSize = msgSize1 + msgSize2 + hasAllRequestsSize;
}

/***** DescriptorOfLastNewView *****/

// This static variable is needed for an initial values population.
uint32_t DescriptorOfLastNewView::viewChangeMsgsNum = 0;

DescriptorOfLastNewView::DescriptorOfLastNewView() {
  for (uint32_t i = 0; i < viewChangeMsgsNum; ++i) viewChangeMsgs.push_back(nullptr);
}

void DescriptorOfLastNewView::clean() {
  delete newViewMsg;
  newViewMsg = nullptr;
  delete myViewChangeMsg;
  myViewChangeMsg = nullptr;
  for (auto msg : viewChangeMsgs) delete msg;
  viewChangeMsgs.clear();
}

bool DescriptorOfLastNewView::equals(const DescriptorOfLastNewView &other) const {
  if ((other.newViewMsg && !newViewMsg) || (!other.newViewMsg && newViewMsg)) return false;
  bool res = newViewMsg ? (other.newViewMsg->equals(*newViewMsg)) : true;
  if (!res) return false;

  if ((other.myViewChangeMsg && !myViewChangeMsg) || (!other.myViewChangeMsg && myViewChangeMsg)) return false;
  res = myViewChangeMsg ? (other.myViewChangeMsg->equals(*myViewChangeMsg)) : true;
  if (!res) return false;

  if (other.viewChangeMsgs.size() != viewChangeMsgs.size()) return false;
  for (uint32_t i = 0; i < viewChangeMsgs.size(); ++i) {
    if ((other.viewChangeMsgs[i] && !viewChangeMsgs[i]) || (!other.viewChangeMsgs[i] && viewChangeMsgs[i]))
      return false;
    res = viewChangeMsgs[i] ? (other.viewChangeMsgs[i]->equals(*viewChangeMsgs[i])) : true;
    if (!res) return false;
  }

  return (other.view == view && other.maxSeqNumTransferredFromPrevViews == maxSeqNumTransferredFromPrevViews &&
          other.stableLowerBoundWhenEnteredToView == stableLowerBoundWhenEnteredToView);
}

void DescriptorOfLastNewView::serializeSimpleParams(char *buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(bufLen >= simpleParamsSize());

  size_t viewSize = sizeof(view);
  memcpy(buf, &view, viewSize);
  buf += viewSize;

  size_t maxSeqNumSize = sizeof(maxSeqNumTransferredFromPrevViews);
  memcpy(buf, &maxSeqNumTransferredFromPrevViews, maxSeqNumSize);
  buf += maxSeqNumSize;

  size_t stableLowerBoundWhenEnteredToViewSize = sizeof(stableLowerBoundWhenEnteredToView);
  memcpy(buf, &stableLowerBoundWhenEnteredToView, stableLowerBoundWhenEnteredToViewSize);
  buf += stableLowerBoundWhenEnteredToViewSize;

  size_t newViewMsgSize = MessageBase::serializeMsg(buf, newViewMsg);

  size_t myViewChangeMsgSize = MessageBase::serializeMsg(buf, myViewChangeMsg);
  actualSize = viewSize + maxSeqNumSize + stableLowerBoundWhenEnteredToViewSize + newViewMsgSize + myViewChangeMsgSize;
}

void DescriptorOfLastNewView::serializeElement(uint32_t id, char *buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(id < viewChangeMsgsNum);
  Assert(bufLen >= maxElementSize());

  ViewChangeMsg *msg = (id < viewChangeMsgs.size()) ? viewChangeMsgs[id] : nullptr;
  actualSize = MessageBase::serializeMsg(buf, msg);
}

void DescriptorOfLastNewView::deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  Assert(bufLen >= simpleParamsSize());

  size_t viewSize = sizeof(view);
  memcpy(&view, buf, viewSize);
  buf += viewSize;

  size_t maxSeqNumSize = sizeof(maxSeqNumTransferredFromPrevViews);
  memcpy(&maxSeqNumTransferredFromPrevViews, buf, maxSeqNumSize);
  buf += maxSeqNumSize;

  size_t stableLowerBoundWhenEnteredToViewSize = sizeof(stableLowerBoundWhenEnteredToView);
  memcpy(&stableLowerBoundWhenEnteredToView, buf, stableLowerBoundWhenEnteredToViewSize);
  buf += stableLowerBoundWhenEnteredToViewSize;

  size_t newViewMsgSize = 0;
  newViewMsg = (NewViewMsg *)MessageBase::deserializeMsg(buf, bufLen, newViewMsgSize);

  size_t myViewChangeMsgSize = 0;
  myViewChangeMsg = (ViewChangeMsg *)MessageBase::deserializeMsg(buf, bufLen, myViewChangeMsgSize);
  actualSize = viewSize + maxSeqNumSize + stableLowerBoundWhenEnteredToViewSize + newViewMsgSize + myViewChangeMsgSize;
}

void DescriptorOfLastNewView::deserializeElement(uint32_t id, char *buf, size_t bufLen, size_t &actualSize) {
  actualSize = 0;
  Assert(id < viewChangeMsgsNum);

  auto *msg = MessageBase::deserializeMsg(buf, bufLen, actualSize);
  Assert(viewChangeMsgs[id] == nullptr);
  viewChangeMsgs[id] = ((ViewChangeMsg *)msg);
}

/***** DescriptorOfLastExecution *****/

bool DescriptorOfLastExecution::equals(const DescriptorOfLastExecution &other) const {
  return (other.executedSeqNum == executedSeqNum && other.validRequests.equals(validRequests));
}

void DescriptorOfLastExecution::serialize(char *&buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(bufLen >= maxSize());

  size_t executedSeqNumSize = sizeof(executedSeqNum);
  memcpy(buf, &executedSeqNum, executedSeqNumSize);
  buf += executedSeqNumSize;

  uint32_t bitMapSize = 0;
  validRequests.writeToBuffer(buf, validRequests.sizeNeededInBuffer(), &bitMapSize);
  buf += bitMapSize;
  actualSize = executedSeqNumSize + bitMapSize;
}

void DescriptorOfLastExecution::deserialize(char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;

  Assert(bufLen >= maxSize())

      size_t seqNumSize = sizeof(executedSeqNum);
  memcpy(&executedSeqNum, buf, seqNumSize);
  buf += seqNumSize;

  uint32_t bitMapSize = 0;
  auto *temp = Bitmap::createBitmapFromBuffer(buf, bufLen - seqNumSize, &bitMapSize);
  validRequests = std::move(*temp);
  delete temp;
  actualSize = seqNumSize + bitMapSize;
}

}  // namespace impl
}  // namespace bftEngine
