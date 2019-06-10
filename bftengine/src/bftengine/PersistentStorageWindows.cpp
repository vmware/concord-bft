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
namespace impl {

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
    char *buf, uint32_t bufLen, size_t &actualSize) const {
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

  actualSize += slowStartedSize + forceCompletedSize;
}

SeqNumData SeqNumData::deserialize(
    char *buf, uint32_t bufLen, uint32_t &actualSize) {
  actualSize = 0;

  size_t msgSize1 = 0, msgSize2 = 0, msgSize3 = 0, msgSize4 = 0;
  auto *prePrepareMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize1);
  auto *fullCommitProofMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize2);
  auto *prepareFullMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize3);
  auto *commitFullMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize4);

  bool forceCompleted = false;
  size_t forceCompletedSize = sizeof(forceCompleted);
  memcpy(&forceCompleted, buf, forceCompletedSize);
  buf += forceCompletedSize;

  bool slowStarted = false;
  size_t slowStartedSize = sizeof(slowStarted);
  memcpy(&slowStarted, buf, slowStartedSize);

  actualSize = msgSize1 + msgSize2 + msgSize3 + msgSize4 + slowStartedSize +
      forceCompletedSize;
  return SeqNumData{(PrePrepareMsg *) prePrepareMsg,
                    (FullCommitProofMsg *) fullCommitProofMsg,
                    (PrepareFullMsg *) prepareFullMsg,
                    (CommitFullMsg *) commitFullMsg,
                    forceCompleted, slowStarted};
}

bool SeqNumData::compareMessages(MessageBase *msg, MessageBase *otherMsg) {
  if ((msg && !otherMsg) || (!msg && otherMsg))
    return false;
  return (msg ? (msg->equals(*otherMsg)) : true);
}

bool SeqNumData::equals(const SeqNumData &other) const {
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

void CheckData::serialize(char *buf, uint32_t bufLen,
                          size_t &actualSize) const {
  actualSize = 0;
  Assert(bufLen >= maxSize());

  size_t completedMarkSize = sizeof(completedMark);
  memcpy(buf, &completedMark, completedMarkSize);
  buf += completedMarkSize;

  actualSize += MessageBase::serializeMsg(buf, checkpointMsg) +
      completedMarkSize;
}

CheckData CheckData::deserialize(
    char *buf, uint32_t bufLen, uint32_t &actualSize) {
  actualSize = 0;

  bool completedMark = false;
  size_t completedMarkSize = sizeof(completedMark);
  memcpy(&completedMark, buf, completedMarkSize);
  buf += completedMarkSize;

  size_t msgSize = 0;
  auto *checkpointMsg = MessageBase::deserializeMsg(buf, bufLen, msgSize);

  actualSize += completedMarkSize + msgSize;
  return CheckData{(CheckpointMsg *) checkpointMsg, completedMark};
}

bool CheckData::equals(const CheckData &other) const {
  if ((checkpointMsg && !other.checkpointMsg) ||
      (!checkpointMsg && other.checkpointMsg))
    return false;
  bool res =
      checkpointMsg ? (checkpointMsg->equals(*other.checkpointMsg)) : true;
  if (!res)
    return false;
  return (completedMark == other.completedMark);
}

uint32_t CheckData::maxSize() {
  bool msgEmptyFlag;
  return (CheckpointMsg::maxSizeOfCheckpointMsgInLocalBuffer() +
      sizeof(msgEmptyFlag) + sizeof(completedMark));
}

}
}  // namespace bftEngine
