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
  for (auto elem : elements) {
    delete elem.prepareFull;
    delete elem.prePrepare;
  }
  elements.clear();
}

bool DescriptorOfLastExitFromView::equals(
    const DescriptorOfLastExitFromView &other) const {
  if (other.elements.size() != elements.size())
    return false;
  for (uint32_t i = 0; i < elements.size(); ++i)
    if (!elements[i].equals(other.elements[i]))
      return false;
  return (other.view == view && other.lastStable == lastStable &&
      other.lastExecuted == lastExecuted);
}

void DescriptorOfLastExitFromView::serializeSimpleParams(
    char *buf, size_t bufLen) const {
  Assert(bufLen >= simpleParamsSize());

  memcpy(buf, &view, sizeof(view));
  buf += sizeof(view);

  memcpy(buf, &lastStable, sizeof(lastStable));
  buf += sizeof(lastStable);

  memcpy(buf, &lastExecuted, sizeof(lastExecuted));
  buf += sizeof(lastExecuted);

  uint32_t elementsNum = elements.size();
  memcpy(buf, &elementsNum, sizeof(elementsNum));
}

void DescriptorOfLastExitFromView::serializeElement(
    uint32_t id, char *buf, size_t bufLen, size_t &actualSize) const {
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
    uint32_t id, char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;

  size_t msgSize1 = 0, msgSize2 = 0;
  auto *prePrepareMsgPtr = MessageBase::deserializeMsg(buf, bufLen, msgSize1);
  auto *prepareFullMsgPtr = MessageBase::deserializeMsg(buf, bufLen, msgSize2);

  bool hasAllRequests = false;
  size_t hasAllRequestsSize = sizeof(hasAllRequestsSize);
  memcpy(&hasAllRequests, buf, hasAllRequestsSize);

  Assert(elements[id].prePrepare == nullptr);
  Assert(elements[id].prepareFull == nullptr);

  elements[id] =
      ViewsManager::PrevViewInfo((PrePrepareMsg *) prePrepareMsgPtr,
                                 (PrepareFullMsg *) prepareFullMsgPtr,
                                 hasAllRequests);
  actualSize = msgSize1 + msgSize2 + hasAllRequestsSize;
}

/***** DescriptorOfLastNewView *****/

// This static variable is needed for an initial values population.
uint32_t DescriptorOfLastNewView::viewChangeMsgsNum_ = 0;

DescriptorOfLastNewView::DescriptorOfLastNewView() {
  for (uint32_t i = 0; i < viewChangeMsgsNum_; ++i)
    viewChangeMsgs.push_back(nullptr);
}

void DescriptorOfLastNewView::clean() {
  delete newViewMsg;
  newViewMsg = nullptr;
  for (auto msg : viewChangeMsgs)
    delete msg;
  viewChangeMsgs.clear();
}

bool DescriptorOfLastNewView::equals(
    const DescriptorOfLastNewView &other) const {
  if ((other.newViewMsg && !newViewMsg) || (!other.newViewMsg && newViewMsg))
    return false;
  bool res = newViewMsg ? (other.newViewMsg->equals(*newViewMsg)) : true;
  if (!res)
    return false;

  if (other.viewChangeMsgs.size() != viewChangeMsgs.size())
    return false;
  for (uint32_t i = 0; i < viewChangeMsgs.size(); ++i) {
    if ((other.viewChangeMsgs[i] && !viewChangeMsgs[i]) ||
        (!other.viewChangeMsgs[i] && viewChangeMsgs[i]))
      return false;
    res = viewChangeMsgs[i] ?
          (other.viewChangeMsgs[i]->equals(*viewChangeMsgs[i])) : true;
    if (!res)
      return false;
  }

  return (other.view == view && (other.maxSeqNumTransferredFromPrevViews ==
      maxSeqNumTransferredFromPrevViews));
}

void DescriptorOfLastNewView::serializeSimpleParams(
    char *buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(bufLen >= simpleParamsSize());

  size_t viewSize = sizeof(view);
  memcpy(buf, &view, viewSize);
  buf += viewSize;

  size_t maxSeqNumSize = sizeof(maxSeqNumTransferredFromPrevViews);
  memcpy(buf, &maxSeqNumTransferredFromPrevViews, maxSeqNumSize);
  buf += maxSeqNumSize;

  size_t msgSize = MessageBase::serializeMsg(buf, newViewMsg);

  actualSize = viewSize + maxSeqNumSize + msgSize;
}

void DescriptorOfLastNewView::serializeElement(
    uint32_t id, char *buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(id < viewChangeMsgsNum_);
  Assert(bufLen >= maxElementSize());

  ViewChangeMsg *msg =
      (id < viewChangeMsgs.size()) ? viewChangeMsgs[id] : nullptr;

  actualSize = MessageBase::serializeMsg(buf, msg);
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

  size_t actualMsgSize = 0;
  newViewMsg = (NewViewMsg *) MessageBase::deserializeMsg(
      buf, bufLen, actualMsgSize);
  actualSize = viewSize + maxSeqNumSize + actualMsgSize;
}

void DescriptorOfLastNewView::deserializeElement(
    uint32_t id, char *buf, size_t bufLen, size_t &actualSize) {
  actualSize = 0;
  Assert(id < viewChangeMsgsNum_);

  auto *msg = MessageBase::deserializeMsg(buf, bufLen, actualSize);
  Assert(viewChangeMsgs[id] == nullptr);
  viewChangeMsgs[id] = ((ViewChangeMsg *) msg);
}

/***** DescriptorOfLastExecution *****/

bool DescriptorOfLastExecution::equals(
    const DescriptorOfLastExecution &other) const {
  return (other.executedSeqNum == executedSeqNum &&
      other.validRequests.equals(validRequests));
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
  actualSize = sizeofSeqNum + bitMapSize;
}

}
}  // namespace bftEngine
