// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <string.h>
#include "ReplicaStatusMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {
static uint8_t powersOf2[] = {
    0x1,
    0x2,
    0x4,
    0x8,
    0x10,
    0x20,
    0x40,
    0x80,
};

// TODO(GG): here we assume that replica Ids are between 0 and MaxNumberOfReplicas-1 (should be changed to support
// dynamic reconfiguration)

MsgSize ReplicaStatusMsg::calcSizeOfReplicaStatusMsg(bool listOfPrePrepareMsgsInActiveWindow,
                                                     bool listOfMissingViewChangeMsgForViewChange,
                                                     bool listOfMissingPrePrepareMsgForViewChange) {
  if (listOfPrePrepareMsgsInActiveWindow)
    return sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader) + (kWorkWindowSize + 7) / 8;
  else if (listOfMissingViewChangeMsgForViewChange)
    return sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader) + (MaxNumberOfReplicas + 7) / 8;
  else if (listOfMissingPrePrepareMsgForViewChange)
    return sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader) + (kWorkWindowSize + 7) / 8;
  else
    return sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader);
}

ReplicaStatusMsg::ReplicaStatusMsg(ReplicaId senderId,
                                   ViewNum viewNumber,
                                   SeqNum lastStableSeqNum,
                                   SeqNum lastExecutedSeqNum,
                                   bool viewIsActive,
                                   bool hasNewChangeMsg,
                                   bool listOfPPInActiveWindow,
                                   bool listOfMissingVCForVC,
                                   bool listOfMissingPPForVC)
    : MessageBase(senderId,
                  MsgCode::ReplicaStatus,
                  calcSizeOfReplicaStatusMsg(listOfPPInActiveWindow, listOfMissingVCForVC, listOfMissingPPForVC)) {
  Assert(lastExecutedSeqNum >= lastStableSeqNum);
  Assert(lastStableSeqNum % checkpointWindowSize == 0);
  Assert(!viewIsActive || hasNewChangeMsg);         // viewIsActive --> hasNewChangeMsg
  Assert(!viewIsActive || !listOfMissingVCForVC);   // viewIsActive --> !listOfMissingVCForVC
  Assert(!viewIsActive || !listOfMissingPPForVC);   // viewIsActive --> !listOfMissingPPForVC
  Assert(viewIsActive || !listOfPPInActiveWindow);  // !viewIsActive --> !listOfPPInActiveWindow
  Assert((listOfPPInActiveWindow ? 1 : 0) + (listOfMissingVCForVC ? 1 : 0) + (listOfMissingPPForVC ? 1 : 0) <= 1);

  b()->viewNumber = viewNumber;
  b()->lastStableSeqNum = lastStableSeqNum;
  b()->lastExecutedSeqNum = lastExecutedSeqNum;
  b()->flags = 0;
  if (viewIsActive) b()->flags |= powersOf2[0];
  if (hasNewChangeMsg) b()->flags |= powersOf2[1];

  if (listOfPPInActiveWindow) {
    b()->flags |= powersOf2[2];
  } else if (listOfMissingVCForVC) {
    b()->flags |= powersOf2[3];
  } else if (listOfMissingPPForVC) {
    b()->flags |= powersOf2[4];
  }

  if (size() > sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader)) {
    // write zero to all bits in list
    MsgSize listSize = size() - sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader);
    char* p = body() + sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader);
    memset(p, 0, listSize);
  }
}

void ReplicaStatusMsg::validate(const ReplicasInfo& repInfo) const {
  if (size() < sizeof(ReplicaStatusMsgHeader) || senderId() == repInfo.myId() || !repInfo.isIdOfReplica(senderId()) ||
      (getLastStableSeqNum() % checkpointWindowSize != 0) || getLastExecutedSeqNum() < getLastStableSeqNum())
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": basic"));

  const bool viewIsActive = currentViewIsActive();
  const bool hasNewChangeMsg = currentViewHasNewViewMessage();
  const bool listOfPPInActiveWindow = hasListOfPrePrepareMsgsInActiveWindow();
  const bool listOfMissingVCForVC = hasListOfMissingViewChangeMsgForViewChange();
  const bool listOfMissingPPForVC = hasListOfMissingPrePrepareMsgForViewChange();

  if (!(!viewIsActive || hasNewChangeMsg) ||         // if NOT (viewIsActive --> hasNewChangeMsg)
      !(!viewIsActive || !listOfMissingVCForVC) ||   // if NOT (viewIsActive --> !listOfMissingVCForVC)
      !(!viewIsActive || !listOfMissingPPForVC) ||   // if NOT (viewIsActive --> !listOfMissingPPForVC)
      !(viewIsActive || !listOfPPInActiveWindow) ||  // if NOT (!viewIsActive --> !listOfPPInActiveWindow)
      (((listOfPPInActiveWindow ? 1 : 0) + (listOfMissingVCForVC ? 1 : 0) + (listOfMissingPPForVC ? 1 : 0)) >= 2) ||
      size() != calcSizeOfReplicaStatusMsg(listOfPPInActiveWindow, listOfMissingVCForVC, listOfMissingPPForVC))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": advanced"));
}

ViewNum ReplicaStatusMsg::getViewNumber() const { return b()->viewNumber; }

SeqNum ReplicaStatusMsg::getLastStableSeqNum() const { return b()->lastStableSeqNum; }

SeqNum ReplicaStatusMsg::getLastExecutedSeqNum() const { return b()->lastExecutedSeqNum; }

bool ReplicaStatusMsg::currentViewIsActive() const { return ((b()->flags & powersOf2[0]) != 0); }

bool ReplicaStatusMsg::currentViewHasNewViewMessage() const { return ((b()->flags & powersOf2[1]) != 0); }

bool ReplicaStatusMsg::hasListOfPrePrepareMsgsInActiveWindow() const { return ((b()->flags & powersOf2[2]) != 0); }

bool ReplicaStatusMsg::hasListOfMissingViewChangeMsgForViewChange() const { return ((b()->flags & powersOf2[3]) != 0); }

bool ReplicaStatusMsg::hasListOfMissingPrePrepareMsgForViewChange() const { return ((b()->flags & powersOf2[4]) != 0); }

bool ReplicaStatusMsg::isPrePrepareInActiveWindow(SeqNum seqNum) const {
  Assert(hasListOfPrePrepareMsgsInActiveWindow());
  Assert(seqNum > b()->lastStableSeqNum);
  Assert(seqNum <= b()->lastStableSeqNum + kWorkWindowSize);

  size_t index = (size_t)(seqNum - b()->lastStableSeqNum - 1);
  size_t byteIndex = index / 8;
  size_t bitIndex = index % 8;
  uint8_t* p = (uint8_t*)body() + sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader);
  return ((p[byteIndex] & powersOf2[bitIndex]) != 0);
}

bool ReplicaStatusMsg::isMissingViewChangeMsgForViewChange(ReplicaId replicaId) const {
  Assert(hasListOfMissingViewChangeMsgForViewChange());
  Assert(replicaId < MaxNumberOfReplicas);

  size_t index = replicaId;
  size_t byteIndex = index / 8;
  size_t bitIndex = index % 8;
  uint8_t* p = (uint8_t*)body() + sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader);
  return ((p[byteIndex] & powersOf2[bitIndex]) != 0);
}

bool ReplicaStatusMsg::isMissingPrePrepareMsgForViewChange(SeqNum seqNum) const {
  Assert(hasListOfMissingPrePrepareMsgForViewChange());
  Assert(seqNum > b()->lastStableSeqNum);
  Assert(seqNum <= b()->lastStableSeqNum + kWorkWindowSize);

  size_t index = (size_t)(seqNum - b()->lastStableSeqNum - 1);
  size_t byteIndex = index / 8;
  size_t bitIndex = index % 8;
  uint8_t* p = (uint8_t*)body() + sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader);
  return ((p[byteIndex] & powersOf2[bitIndex]) != 0);
}

void ReplicaStatusMsg::setPrePrepareInActiveWindow(SeqNum seqNum) const {
  Assert(hasListOfPrePrepareMsgsInActiveWindow());
  Assert(seqNum > b()->lastStableSeqNum);
  Assert(seqNum <= b()->lastStableSeqNum + kWorkWindowSize);
  size_t index = (size_t)(seqNum - b()->lastStableSeqNum - 1);
  size_t byteIndex = index / 8;
  size_t bitIndex = index % 8;
  uint8_t* p = (uint8_t*)body() + sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader);
  p[byteIndex] = p[byteIndex] | powersOf2[bitIndex];
}

void ReplicaStatusMsg::setMissingViewChangeMsgForViewChange(ReplicaId replicaId) {
  Assert(hasListOfMissingViewChangeMsgForViewChange());
  Assert(replicaId < MaxNumberOfReplicas);
  size_t index = replicaId;
  size_t byteIndex = index / 8;
  size_t bitIndex = index % 8;
  uint8_t* p = (uint8_t*)body() + sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader);
  p[byteIndex] = p[byteIndex] | powersOf2[bitIndex];
}

void ReplicaStatusMsg::setMissingPrePrepareMsgForViewChange(SeqNum seqNum) {
  Assert(hasListOfMissingPrePrepareMsgForViewChange());
  Assert(seqNum > b()->lastStableSeqNum);
  Assert(seqNum <= b()->lastStableSeqNum + kWorkWindowSize);
  size_t index = (size_t)(seqNum - b()->lastStableSeqNum - 1);
  size_t byteIndex = index / 8;
  size_t bitIndex = index % 8;
  uint8_t* p = (uint8_t*)body() + sizeof(ReplicaStatusMsg::ReplicaStatusMsgHeader);
  p[byteIndex] = p[byteIndex] | powersOf2[bitIndex];
}

}  // namespace impl
}  // namespace bftEngine
