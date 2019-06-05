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

#include "PersistentStorageWindows.hpp"

namespace bftEngine {

void SeqNumData::reset() {
  delete prePrepareMsg;
  delete fullCommitProofMsg;
  delete prepareFullMsg;
  delete commitFullMsg;

  prePrepareMsg = nullptr;
  fullCommitProofMsg = nullptr;
  prepareFullMsg = nullptr;
  commitFullMsg = nullptr;

  slowStarted = false;
  forceCompleted = false;
}

void SeqNumData::serialize(
    char *&buf, uint32_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(bufLen >= maxSize());

  actualSize += MessageBase::serializeMsg(buf, prePrepareMsg);
  actualSize += MessageBase::serializeMsg(buf, fullCommitProofMsg);
  actualSize += MessageBase::serializeMsg(buf, prepareFullMsg);
  actualSize += MessageBase::serializeMsg(buf, commitFullMsg);

  size_t slowStartedSize = sizeof(slowStarted);
  memcpy(buf, &slowStarted, slowStartedSize);
  buf += slowStartedSize;

  size_t forceCompletedSize = sizeof(forceCompleted);
  memcpy(buf, &forceCompleted, forceCompletedSize);
  buf += forceCompletedSize;

  actualSize += slowStartedSize + forceCompletedSize;
}

SeqNumData SeqNumData::deserialize(
    char *&buf, uint32_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  Assert(bufLen >= maxSize());

  size_t msgSize = 0;
  auto *prePrepareMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize);
  actualSize += msgSize;
  auto *fullCommitProofMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize);
  actualSize += msgSize;
  auto *prepareFullMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize);
  actualSize += msgSize;
  auto *commitFullMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize);
  actualSize += msgSize;

  bool forceCompleted = false;
  size_t forceCompletedSize = sizeof(forceCompleted);
  memcpy(&forceCompleted, buf, forceCompletedSize);
  buf += forceCompletedSize;

  bool slowStarted = false;
  size_t slowStartedSize = sizeof(slowStarted);
  memcpy(&slowStarted, buf, slowStartedSize);
  buf += slowStartedSize;

  actualSize += slowStartedSize + forceCompletedSize;
  return SeqNumData{(PrePrepareMsg *) prePrepareMsg,
                    (FullCommitProofMsg *) fullCommitProofMsg,
                    (PrepareFullMsg *) prepareFullMsg,
                    (CommitFullMsg *) commitFullMsg,
                    forceCompleted, slowStarted};
}

bool SeqNumData::compareMessages(MessageBase *msg, MessageBase *otherMsg) {
  if ((msg && !otherMsg) || (!msg && otherMsg))
    return false;
  return (msg ? (*msg == *otherMsg) : true);
}

bool SeqNumData::operator==(const SeqNumData &other) const {
  if (!compareMessages(prePrepareMsg, other.prePrepareMsg))
    return false;
  if (!compareMessages(fullCommitProofMsg, other.fullCommitProofMsg))
    return false;
  if (!compareMessages(prepareFullMsg, other.prepareFullMsg))
    return false;
  if (!compareMessages(commitFullMsg, other.commitFullMsg))
    return false;

  return ((slowStarted == other.slowStarted) &&
      (forceCompleted == other.forceCompleted));
}

SeqNumData &SeqNumData::operator=(const SeqNumData &other) {
  delete prePrepareMsg;
  delete fullCommitProofMsg;
  delete prepareFullMsg;
  delete commitFullMsg;

  prePrepareMsg = nullptr;
  fullCommitProofMsg = nullptr;
  prepareFullMsg = nullptr;
  commitFullMsg = nullptr;

  if (other.prePrepareMsg)
    prePrepareMsg = (PrePrepareMsg *) other.prePrepareMsg->cloneObjAndMsg();
  if (other.fullCommitProofMsg)
    fullCommitProofMsg =
        (FullCommitProofMsg *) other.fullCommitProofMsg->cloneObjAndMsg();
  if (other.prepareFullMsg)
    prepareFullMsg = (PrepareFullMsg *) other.prepareFullMsg->cloneObjAndMsg();
  if (other.commitFullMsg)
    commitFullMsg = (CommitFullMsg *) other.commitFullMsg->cloneObjAndMsg();

  slowStarted = other.slowStarted;
  forceCompleted = other.forceCompleted;
  return *this;
}

uint32_t SeqNumData::maxSize() {
  bool msgEmptyFlag;
  return (PrePrepareMsg::maxSizeOfPrePrepareMsgInLocalBuffer() +
      FullCommitProofMsg::maxSizeOfFullCommitProofMsgInLocalBuffer() +
      PrepareFullMsg::maxSizeOfPrepareFullInLocalBuffer() +
      CommitFullMsg::maxSizeOfCommitFullInLocalBuffer() +
      4 * sizeof(msgEmptyFlag) +
      sizeof(slowStarted) +
      sizeof(forceCompleted));
}

/*****************************************************************************/

void CheckData::reset() {
  delete checkpointMsg;
  checkpointMsg = nullptr;
  completedMark = false;
}

void CheckData::serialize(
    char *&buf, uint32_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  Assert(bufLen >= maxSize());

  size_t completedMarkSize = sizeof(completedMark);
  memcpy(buf, &completedMark, completedMarkSize);
  buf += completedMarkSize;

  actualSize += MessageBase::serializeMsg(buf, checkpointMsg) +
      completedMarkSize;
}

CheckData CheckData::deserialize(
    char *&buf, uint32_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  Assert(bufLen >= maxSize());

  bool completedMark = false;
  size_t completedMarkSize = sizeof(completedMark);
  memcpy(&completedMark, buf, completedMarkSize);
  buf += completedMarkSize;

  size_t msgSize = 0;
  auto *checkpointMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize);

  actualSize += completedMarkSize + msgSize;
  return CheckData{(CheckpointMsg *) checkpointMsg, completedMark};
}

bool CheckData::operator==(const CheckData &other) const {
  if ((checkpointMsg && !other.checkpointMsg) ||
      (!checkpointMsg && other.checkpointMsg))
    return false;
  bool res = checkpointMsg ? (*checkpointMsg == *other.checkpointMsg) : true;
  if (!res)
    return false;
  return (completedMark == other.completedMark);
}

CheckData &CheckData::operator=(const CheckData &other) {
  delete checkpointMsg;
  checkpointMsg = nullptr;
  if (other.checkpointMsg)
    checkpointMsg = (CheckpointMsg *) other.checkpointMsg->cloneObjAndMsg();
  completedMark = other.completedMark;
  return *this;
}

uint32_t CheckData::maxSize() {
  bool msgEmptyFlag;
  return (CheckpointMsg::maxSizeOfCheckpointMsgInLocalBuffer() +
      sizeof(msgEmptyFlag) + sizeof(completedMark));
}

}  // namespace bftEngine
