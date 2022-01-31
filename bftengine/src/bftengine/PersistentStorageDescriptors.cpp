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
  if (other.isDefault != isDefault) return false;

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
  ConcordAssert(bufLen >= simpleParamsSize());

  size_t isDefaultSize = sizeof(isDefault);
  memcpy(buf, &isDefault, isDefaultSize);
  buf += isDefaultSize;

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
  buf += elementsNumSize;

  uint32_t complaintsNum = complaints.size();
  size_t complaintsNumSize = sizeof(complaintsNum);
  memcpy(buf, &complaintsNum, complaintsNumSize);
  buf += complaintsNumSize;

  actualSize = isDefaultSize + viewSize + lastStableSize + lastExecutedSize + stableLowerBoundWhenEnteredToViewSize +
               myViewChangeMsgSize + elementsNumSize + complaintsNumSize;
}

void DescriptorOfLastExitFromView::serializeElement(uint32_t id, char *buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  ConcordAssert(id < elements.size());
  ConcordAssert(bufLen >= maxElementSize());

  PrePrepareMsg *prePrepareMsg = (id < elements.size()) ? elements[id].prePrepare : nullptr;
  PrepareFullMsg *prePrepareFullMsg = (id < elements.size()) ? elements[id].prepareFull : nullptr;

  actualSize += MessageBase::serializeMsg(buf, prePrepareMsg);
  actualSize += MessageBase::serializeMsg(buf, prePrepareFullMsg);

  size_t hasAllRequestsSize = sizeof(elements[id].hasAllRequests);
  memcpy(buf, &elements[id].hasAllRequests, hasAllRequestsSize);
  actualSize += hasAllRequestsSize;
}

void DescriptorOfLastExitFromView::serializeComplaint(uint32_t id, char *buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  ConcordAssert(id < complaints.size());
  ConcordAssert(bufLen >= maxComplaintSize());

  actualSize += MessageBase::serializeMsg(buf, complaints[id].get());
}

void DescriptorOfLastExitFromView::deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  ConcordAssert(bufLen >= simpleParamsSize());

  size_t isDefaultSize = sizeof(isDefault);
  memcpy(&isDefault, buf, isDefaultSize);
  buf += isDefaultSize;

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
  std::unique_ptr<MessageBase> baseMsg(MessageBase::deserializeMsg(buf, bufLen, actualMsgSize));
  myViewChangeMsg = nullptr;
  if (baseMsg) {
    myViewChangeMsg = new ViewChangeMsg(baseMsg.get());
  }
  uint32_t elementsNum;
  size_t elementsNumSize = sizeof(elementsNum);
  memcpy(&elementsNum, buf, elementsNumSize);
  buf += elementsNumSize;

  if (elementsNum) elements.resize(elementsNum);

  uint32_t complaintsNum;
  size_t complaintsNumSize = sizeof(complaintsNum);
  memcpy(&complaintsNum, buf, complaintsNumSize);
  buf += complaintsNumSize;

  if (complaintsNum) complaints.resize(complaintsNum);

  actualSize = isDefaultSize + viewSize + lastStableSize + lastExecutedSize + stableLowerBoundWhenEnteredToViewSize +
               complaintsNumSize + actualMsgSize + elementsNumSize;
}

void DescriptorOfLastExitFromView::deserializeElement(uint32_t id, char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;

  size_t msgSize1 = 0, msgSize2 = 0;
  PrePrepareMsg *prePrepareMsgPtr = nullptr;
  PrepareFullMsg *prepareFullMsgPtr = nullptr;
  {
    std::unique_ptr<MessageBase> baseMsg(MessageBase::deserializeMsg(buf, bufLen, msgSize1));
    if (baseMsg) {
      prePrepareMsgPtr = new PrePrepareMsg(baseMsg.get());
    }
  }
  {
    std::unique_ptr<MessageBase> baseMsg(MessageBase::deserializeMsg(buf, bufLen, msgSize2));
    if (baseMsg) {
      prepareFullMsgPtr = new PrepareFullMsg(baseMsg.get());
    }
  }

  bool hasAllRequests = false;
  size_t hasAllRequestsSize = sizeof(hasAllRequests);
  memcpy(&hasAllRequests, buf, hasAllRequestsSize);

  ConcordAssert(elements[id].prePrepare == nullptr);
  ConcordAssert(elements[id].prepareFull == nullptr);

  elements[id] = ViewsManager::PrevViewInfo(prePrepareMsgPtr, prepareFullMsgPtr, hasAllRequests);
  actualSize = msgSize1 + msgSize2 + hasAllRequestsSize;
}

void DescriptorOfLastExitFromView::deserializeComplaint(uint32_t id, char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  size_t msgSize = 0;

  std::unique_ptr<ReplicaAsksToLeaveViewMsg> replicaAsksToLeaveViewMsg;

  {
    std::unique_ptr<MessageBase> baseMsg(MessageBase::deserializeMsg(buf, bufLen, msgSize));
    if (baseMsg) {
      replicaAsksToLeaveViewMsg = make_unique<ReplicaAsksToLeaveViewMsg>(ReplicaAsksToLeaveViewMsg(baseMsg.get()));
    }
  }

  ConcordAssert(complaints[id] == nullptr);

  complaints[id] = move(replicaAsksToLeaveViewMsg);

  actualSize = msgSize;
}
/***** DescriptorOfLastNewView *****/

// This static variable is needed for an initial values population.
uint32_t DescriptorOfLastNewView::viewChangeMsgsNum = 0;

DescriptorOfLastNewView::DescriptorOfLastNewView() {
  isDefault = true;
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
  if (other.isDefault != isDefault) return false;

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
  ConcordAssert(bufLen >= simpleParamsSize());

  size_t isDefaultSize = sizeof(isDefault);
  memcpy(buf, &isDefault, isDefaultSize);
  buf += isDefaultSize;

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
  actualSize = isDefaultSize + viewSize + maxSeqNumSize + stableLowerBoundWhenEnteredToViewSize + newViewMsgSize +
               myViewChangeMsgSize;
}

void DescriptorOfLastNewView::serializeElement(uint32_t id, char *buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  ConcordAssert(id < viewChangeMsgsNum);
  ConcordAssert(bufLen >= maxElementSize());

  ViewChangeMsg *msg = (id < viewChangeMsgs.size()) ? viewChangeMsgs[id] : nullptr;
  actualSize = MessageBase::serializeMsg(buf, msg);
}

void DescriptorOfLastNewView::deserializeSimpleParams(char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  ConcordAssert(bufLen >= simpleParamsSize());

  size_t isDefaultSize = sizeof(isDefault);
  memcpy(&isDefault, buf, isDefaultSize);
  buf += isDefaultSize;

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
  {
    std::unique_ptr<MessageBase> baseMsg(MessageBase::deserializeMsg(buf, bufLen, newViewMsgSize));
    newViewMsg = nullptr;
    if (baseMsg) {
      newViewMsg = new NewViewMsg(baseMsg.get());
    }
  }
  size_t myViewChangeMsgSize = 0;
  {
    std::unique_ptr<MessageBase> baseMsg(MessageBase::deserializeMsg(buf, bufLen, myViewChangeMsgSize));
    myViewChangeMsg = nullptr;
    if (baseMsg) {
      myViewChangeMsg = new ViewChangeMsg(baseMsg.get());
    }
  }
  actualSize = isDefaultSize + viewSize + maxSeqNumSize + stableLowerBoundWhenEnteredToViewSize + newViewMsgSize +
               myViewChangeMsgSize;
}

void DescriptorOfLastNewView::deserializeElement(uint32_t id, char *buf, size_t bufLen, size_t &actualSize) {
  actualSize = 0;
  ConcordAssert(id < viewChangeMsgsNum);

  std::unique_ptr<MessageBase> msgBase(MessageBase::deserializeMsg(buf, bufLen, actualSize));
  ViewChangeMsg *msg = nullptr;
  if (msgBase) {
    msg = new ViewChangeMsg(msgBase.get());
  }
  ConcordAssert(viewChangeMsgs[id] == nullptr);
  viewChangeMsgs[id] = msg;
}

/***** DescriptorOfLastExecution *****/

bool DescriptorOfLastExecution::equals(const DescriptorOfLastExecution &other) const {
  return (other.executedSeqNum == executedSeqNum && other.validRequests.equals(validRequests) &&
          other.timeInTicks == timeInTicks);
}

void DescriptorOfLastExecution::serialize(char *&buf, size_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  ConcordAssert(bufLen >= maxSize());

  size_t executedSeqNumSize = sizeof(executedSeqNum);
  memcpy(buf, &executedSeqNum, executedSeqNumSize);
  buf += executedSeqNumSize;

  uint32_t bitMapSize = 0;
  validRequests.writeToBuffer(buf, validRequests.sizeNeededInBuffer(), &bitMapSize);
  buf += bitMapSize;

  auto sizeOfTick = sizeof(ConsensusTickRep);
  memcpy(buf, &timeInTicks, sizeOfTick);

  buf += sizeOfTick;
  actualSize = executedSeqNumSize + bitMapSize + sizeOfTick;
}

void DescriptorOfLastExecution::deserialize(char *buf, size_t bufLen, uint32_t &actualSize) {
  actualSize = 0;

  ConcordAssert(bufLen >= maxSize());

  size_t seqNumSize = sizeof(executedSeqNum);
  memcpy(&executedSeqNum, buf, seqNumSize);
  buf += seqNumSize;

  uint32_t bitMapSize = 0;
  validRequests = Bitmap(buf, bufLen - (seqNumSize + sizeof(ConsensusTickRep)), &bitMapSize);

  buf += bitMapSize;

  auto sizeOfTick = sizeof(ConsensusTickRep);
  memcpy(reinterpret_cast<char *>(&timeInTicks), buf, sizeOfTick);

  actualSize = seqNumSize + bitMapSize + sizeOfTick;
}

/***** DescriptorOfLastStableCheckpoint *****/

bool DescriptorOfLastStableCheckpoint::equals(const DescriptorOfLastStableCheckpoint &other) const {
  if (checkpointMsgs.size() != other.checkpointMsgs.size()) {
    return false;
  }
  for (size_t i = 0; i < checkpointMsgs.size(); i++) {
    if (other.checkpointMsgs[i] == nullptr || !CheckpointMsg::equivalent(checkpointMsgs[i], other.checkpointMsgs[i])) {
      return false;
    }
  }
  return true;
}

void DescriptorOfLastStableCheckpoint::serialize(char *&buf, size_t bufLen, size_t &actualSize) const {
  ConcordAssert(bufLen >= maxSize(numOfReplicas));

  actualSize = 0;

  numMsgs = checkpointMsgs.size();

  size_t numMsgsSize = sizeof(numMsgs);
  memcpy(buf, &numMsgs, numMsgsSize);
  buf += numMsgsSize;
  actualSize += numMsgsSize;

  for (auto *msg : checkpointMsgs) {
    actualSize += MessageBase::serializeMsg(buf, msg);
  }
}

void DescriptorOfLastStableCheckpoint::deserialize(char *buf, size_t bufLen, size_t &actualSize) {
  size_t numMsgsSize = sizeof(numMsgs);
  memcpy(&numMsgs, buf, numMsgsSize);
  buf += numMsgsSize;
  actualSize += numMsgsSize;

  for (size_t i = 0; i < numMsgs; i++) {
    std::unique_ptr<MessageBase> msgBase(MessageBase::deserializeMsg(buf, bufLen, actualSize));
    CheckpointMsg *msg = nullptr;
    if (msgBase) {
      msg = new CheckpointMsg(msgBase.get());
    }
    checkpointMsgs.push_back(msg);
  }
}

}  // namespace impl
}  // namespace bftEngine
