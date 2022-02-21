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
  delete prePrepareMsg_;
  delete fullCommitProofMsg_;
  delete prepareFullMsg_;
  delete commitFullMsg_;

  prePrepareMsg_ = nullptr;
  fullCommitProofMsg_ = nullptr;
  prepareFullMsg_ = nullptr;
  commitFullMsg_ = nullptr;
  slowStarted_ = false;
  forceCompleted_ = false;
  requestsMap_.zeroAll();
}

size_t SeqNumData::serializeMsg(char *&buf, MessageBase *msg) { return MessageBase::serializeMsg(buf, msg); }

size_t SeqNumData::serializePrePrepareMsg(char *&buf) const { return serializeMsg(buf, prePrepareMsg_); }

size_t SeqNumData::serializeFullCommitProofMsg(char *&buf) const {
  return MessageBase::serializeMsg(buf, fullCommitProofMsg_);
}

size_t SeqNumData::serializePrepareFullMsg(char *&buf) const { return MessageBase::serializeMsg(buf, prepareFullMsg_); }

size_t SeqNumData::serializeCommitFullMsg(char *&buf) const { return MessageBase::serializeMsg(buf, commitFullMsg_); }

size_t SeqNumData::serializeForceCompleted(char *&buf) const { return serializeOneByte(buf, forceCompleted_); }

size_t SeqNumData::serializeSlowStarted(char *&buf) const { return serializeOneByte(buf, slowStarted_); }

size_t SeqNumData::serializesIsExecuted(char *&buf) const { return serializeOneByte(buf, isExecuted_); }
size_t SeqNumData::serializeRequestsMap(char *&buf) const {
  const std::size_t data_size = requestsMap_.sizeNeededInBuffer();
  uint32_t actual_data = 0;
  requestsMap_.writeToBuffer(buf, data_size, &actual_data);
  ConcordAssert(actual_data == data_size);
  return actual_data;
}

size_t SeqNumData::serializeOneByte(char *&buf, const uint8_t &oneByte) {
  const size_t oneByteSize = sizeof(oneByte);
  memcpy(buf, &oneByte, oneByteSize);
  buf += oneByte;
  return oneByteSize;
}

void SeqNumData::serialize(char *buf, uint32_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  ConcordAssert(bufLen >= maxSize());
  actualSize = serializePrePrepareMsg(buf) + serializeFullCommitProofMsg(buf) + serializePrepareFullMsg(buf) +
               serializeCommitFullMsg(buf) + serializeForceCompleted(buf) + serializeSlowStarted(buf) +
               serializeRequestsMap(buf) + serializesIsExecuted(buf);
}

MessageBase *SeqNumData::deserializeMsg(char *&buf, uint32_t bufLen, size_t &actualMsgSize) {
  return MessageBase::deserializeMsg(buf, bufLen, actualMsgSize);
}

uint8_t SeqNumData::deserializeOneByte(char *&buf) {
  uint8_t oneByte = 0;
  const size_t oneByteSize = sizeof(oneByte);
  memcpy(&oneByte, buf, oneByteSize);
  buf += oneByteSize;
  return oneByte;
}

SeqNumData SeqNumData::deserialize(char *buf, uint32_t bufLen, uint32_t &actualSize) {
  actualSize = 0;

  size_t msgSize1 = 0, msgSize2 = 0, msgSize3 = 0, msgSize4 = 0;
  auto *prePrepareMsg = deserializeMsg(buf, bufLen, msgSize1);
  auto *fullCommitProofMsg = deserializeMsg(buf, bufLen, msgSize2);
  auto *prepareFullMsg = deserializeMsg(buf, bufLen, msgSize3);
  auto *commitFullMsg = deserializeMsg(buf, bufLen, msgSize4);

  const bool forceCompleted = deserializeOneByte(buf);
  const bool slowStarted = deserializeOneByte(buf);
  uint32_t bitMapSize = 0;
  auto number_of_reqeusts = 4096;
  if (prePrepareMsg) {
    number_of_reqeusts = ((PrePrepareMsg *)prePrepareMsg)->numberOfRequests();
  }
  Bitmap requestsMap = Bitmap(buf, Bitmap::maxSizeNeededToStoreInBuffer(number_of_reqeusts), &bitMapSize);
  buf += bitMapSize;
  const bool isExecuted = deserializeOneByte(buf);
  actualSize = msgSize1 + msgSize2 + msgSize3 + msgSize4 + sizeof(slowStarted) + sizeof(forceCompleted) + bitMapSize +
               sizeof(isExecuted_);
  return SeqNumData{(PrePrepareMsg *)prePrepareMsg,
                    (FullCommitProofMsg *)fullCommitProofMsg,
                    (PrepareFullMsg *)prepareFullMsg,
                    (CommitFullMsg *)commitFullMsg,
                    forceCompleted,
                    slowStarted,
                    requestsMap,
                    isExecuted};
}

bool SeqNumData::compareMessages(MessageBase *msg, MessageBase *otherMsg) {
  if ((msg && !otherMsg) || (!msg && otherMsg)) return false;
  return (msg ? (msg->equals(*otherMsg)) : true);
}

bool SeqNumData::equals(const SeqNumData &other) const {
  if (!compareMessages(prePrepareMsg_, other.prePrepareMsg_)) return false;
  if (!compareMessages(fullCommitProofMsg_, other.fullCommitProofMsg_)) return false;
  if (!compareMessages(prepareFullMsg_, other.prepareFullMsg_)) return false;
  if (!compareMessages(commitFullMsg_, other.commitFullMsg_)) return false;

  return ((slowStarted_ == other.slowStarted_) && (forceCompleted_ == other.forceCompleted_));
}

uint32_t SeqNumData::maxSize() {
  return SeqNumData::maxMessageSize<PrePrepareMsg>() + SeqNumData::maxMessageSize<FullCommitProofMsg>() +
         SeqNumData::maxMessageSize<PrepareFullMsg>() + SeqNumData::maxMessageSize<CommitFullMsg>() +
         sizeof(slowStarted_) + sizeof(forceCompleted_) + Bitmap::maxSizeNeededToStoreInBuffer(4096) +
         sizeof(isExecuted_);  // TODO: configurable ?
}

/*****************************************************************************/

void CheckData::reset() {
  delete checkpointMsg_;

  checkpointMsg_ = nullptr;
  completedMark_ = false;
}

size_t CheckData::serializeCheckpointMsg(char *&buf) const { return serializeCheckpointMsg(buf, checkpointMsg_); }

size_t CheckData::serializeCompletedMark(char *&buf) const { return serializeCompletedMark(buf, completedMark_); }

size_t CheckData::serializeCheckpointMsg(char *&buf, CheckpointMsg *msg) { return MessageBase::serializeMsg(buf, msg); }

size_t CheckData::serializeCompletedMark(char *&buf, const uint8_t &completedMark) {
  const size_t sizeofCompletedMark = sizeof(completedMark);
  memcpy(buf, &completedMark, sizeofCompletedMark);
  buf += sizeofCompletedMark;
  return sizeofCompletedMark;
}

void CheckData::serialize(char *buf, uint32_t bufLen, size_t &actualSize) const {
  actualSize = 0;
  ConcordAssert(bufLen >= maxSize());
  actualSize += serializeCheckpointMsg(buf) + serializeCompletedMark(buf);
}

uint8_t CheckData::deserializeCompletedMark(char *&buf) {
  uint8_t completedMark = 0;
  const size_t completedMarkSize = sizeof(completedMark);
  memcpy(&completedMark, buf, completedMarkSize);
  buf += completedMarkSize;
  return completedMark;
}

CheckpointMsg *CheckData::deserializeCheckpointMsg(char *&buf, uint32_t bufLen, size_t &actualMsgSize) {
  std::unique_ptr<MessageBase> baseMsg(MessageBase::deserializeMsg(buf, bufLen, actualMsgSize));
  CheckpointMsg *msg = nullptr;
  if (baseMsg) {
    msg = new CheckpointMsg(baseMsg.get());
  }
  return msg;
}

CheckData CheckData::deserialize(char *buf, uint32_t bufLen, uint32_t &actualSize) {
  actualSize = 0;
  size_t msgSize = 0;

  auto *checkpointMsg = deserializeCheckpointMsg(buf, bufLen, msgSize);
  bool completedMark = deserializeCompletedMark(buf);

  actualSize += sizeof(completedMark) + msgSize;
  return CheckData{checkpointMsg, completedMark};
}

bool CheckData::equals(const CheckData &other) const {
  if ((checkpointMsg_ && !other.checkpointMsg_) || (!checkpointMsg_ && other.checkpointMsg_)) return false;
  bool res = checkpointMsg_ ? (checkpointMsg_->equals(*other.checkpointMsg_)) : true;
  if (!res) return false;
  return (completedMark_ == other.completedMark_);
}

uint32_t CheckData::maxSize() { return (maxCheckpointMsgSize() + sizeof(completedMark_)); }

uint32_t CheckData::maxCheckpointMsgSize() {
  bool msgEmptyFlag;
  return maxMessageSizeInLocalBuffer<CheckpointMsg>() + sizeof(msgEmptyFlag);
}

}  // namespace impl
}  // namespace bftEngine
