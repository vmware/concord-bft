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

#pragma once

#include "SerializableActiveWindow.hpp"
#include "SerializableActiveWindow.cpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/SignedShareMsgs.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "messages/CheckpointMsg.hpp"
#include "Bitmap.hpp"
#include <memory>

namespace bftEngine {
namespace impl {

enum SeqNumDataParameters {
  SEQ_NUM_FIRST_PARAM = 1,
  PRE_PREPARE_MSG = SEQ_NUM_FIRST_PARAM,
  FULL_COMMIT_PROOF_MSG = 2,
  PRE_PREPARE_FULL_MSG = 3,
  COMMIT_FULL_MSG = 4,
  FORCE_COMPLETED = 5,
  SLOW_STARTED = 6,
  BIT_MAP = 7,
  IS_EXECUTED = 8,
  SEQ_NUM_LAST_PARAM = IS_EXECUTED
};

class SeqNumData {
  static constexpr size_t EMPTY_MESSAGE_FLAG_SIZE = sizeof(bool);

 public:
  SeqNumData(PrePrepareMsg *prePrepare,
             FullCommitProofMsg *fullCommitProof,
             PrepareFullMsg *prepareFull,
             CommitFullMsg *commitFull,
             bool forceComplete,
             bool slowStart,
             const Bitmap &requestsMap,
             bool isExecuted)
      : prePrepareMsg_(prePrepare),
        fullCommitProofMsg_(fullCommitProof),
        prepareFullMsg_(prepareFull),
        commitFullMsg_(commitFull),
        forceCompleted_(forceComplete),
        slowStarted_(slowStart),
        requestsMap_(requestsMap),
        isExecuted_(isExecuted) {}

  SeqNumData() = default;

  bool equals(const SeqNumData &other) const;
  void reset();

  size_t serializePrePrepareMsg(char *&buf) const;
  size_t serializeFullCommitProofMsg(char *&buf) const;
  size_t serializePrepareFullMsg(char *&buf) const;
  size_t serializeCommitFullMsg(char *&buf) const;
  size_t serializeForceCompleted(char *&buf) const;
  size_t serializeSlowStarted(char *&buf) const;
  size_t serializeRequestsMap(char *&buf) const;
  size_t serializesIsExecuted(char *&buf) const;
  void serialize(char *buf, uint32_t bufLen, size_t &actualSize) const;

  bool isPrePrepareMsgSet() const { return (prePrepareMsg_ != nullptr); }
  bool isFullCommitProofMsgSet() const { return (fullCommitProofMsg_ != nullptr); }
  bool isPrepareFullMsgSet() const { return (prepareFullMsg_ != nullptr); }
  bool isCommitFullMsgSet() const { return (commitFullMsg_ != nullptr); }

  PrePrepareMsg *getPrePrepareMsg() const { return prePrepareMsg_; }
  FullCommitProofMsg *getFullCommitProofMsg() const { return fullCommitProofMsg_; }
  PrepareFullMsg *getPrepareFullMsg() const { return prepareFullMsg_; }
  CommitFullMsg *getCommitFullMsg() const { return commitFullMsg_; }
  bool getSlowStarted() const { return slowStarted_; }
  bool getForceCompleted() const { return forceCompleted_; }
  Bitmap getRequestsMap() const { return requestsMap_; }
  bool getIsExecuted() const { return isExecuted_; }

  void setPrePrepareMsg(MessageBase *msg) { prePrepareMsg_ = (PrePrepareMsg *)msg; }
  void setFullCommitProofMsg(MessageBase *msg) { fullCommitProofMsg_ = (FullCommitProofMsg *)msg; }
  void setPrepareFullMsg(MessageBase *msg) { prepareFullMsg_ = (PrepareFullMsg *)msg; }
  void setCommitFullMsg(MessageBase *msg) { commitFullMsg_ = (CommitFullMsg *)msg; }
  void setSlowStarted(const bool &slowStarted) { slowStarted_ = slowStarted; }
  void setForceCompleted(const bool &forceCompleted) { forceCompleted_ = forceCompleted; }
  void setRequestsMap(const Bitmap &requestsMap) { requestsMap_ = requestsMap; }
  void setIsExecuted(bool isExecuted) { isExecuted_ = isExecuted; }
  static size_t serializeMsg(char *&buf, MessageBase *msg);
  static size_t serializeOneByte(char *&buf, const uint8_t &oneByte);

  static MessageBase *deserializeMsg(char *&buf, uint32_t bufLen, size_t &actualMsgSize);
  static uint8_t deserializeOneByte(char *&buf);
  static SeqNumData deserialize(char *buf, uint32_t bufLen, uint32_t &actualSize);
  static uint32_t maxSize();
  template <typename MessageT>
  static uint32_t maxMessageSize() {
    return maxMessageSizeInLocalBuffer<MessageT>() + EMPTY_MESSAGE_FLAG_SIZE;
  }
  static constexpr uint16_t getNumOfParams() { return SEQ_NUM_LAST_PARAM; }

 private:
  static bool compareMessages(MessageBase *msg, MessageBase *otherMsg);

 private:
  PrePrepareMsg *prePrepareMsg_ = nullptr;
  FullCommitProofMsg *fullCommitProofMsg_ = nullptr;
  PrepareFullMsg *prepareFullMsg_ = nullptr;
  CommitFullMsg *commitFullMsg_ = nullptr;
  bool forceCompleted_ = false;
  bool slowStarted_ = false;
  Bitmap requestsMap_ = Bitmap();
  bool isExecuted_ = false;
};

enum CheckDataParameters {
  CHECK_DATA_FIRST_PARAM = 1,
  CHECKPOINT_MSG = CHECK_DATA_FIRST_PARAM,
  COMPLETED_MARK = 2,
  CHECK_DATA_LAST_PARAM = COMPLETED_MARK
};

class CheckData {
 public:
  CheckData(CheckpointMsg *checkpoint, bool completed) : checkpointMsg_(checkpoint), completedMark_(completed) {}

  CheckData() = default;

  bool equals(const CheckData &other) const;
  void reset();

  size_t serializeCheckpointMsg(char *&buf) const;
  size_t serializeCompletedMark(char *&buf) const;
  void serialize(char *buf, uint32_t bufLen, size_t &actualSize) const;

  bool isCheckpointMsgSet() const { return (checkpointMsg_ != nullptr); }
  CheckpointMsg *getCheckpointMsg() const { return checkpointMsg_; }
  bool getCompletedMark() const { return completedMark_; }

  void deleteCheckpointMsg() { delete checkpointMsg_; }
  void setCheckpointMsg(MessageBase *msg) { checkpointMsg_ = (CheckpointMsg *)msg; }
  void setCompletedMark(const bool &completedMark) { completedMark_ = completedMark; }

  static size_t serializeCheckpointMsg(char *&buf, CheckpointMsg *msg);
  static size_t serializeCompletedMark(char *&buf, const uint8_t &completedMark);

  static CheckpointMsg *deserializeCheckpointMsg(char *&buf, uint32_t bufLen, size_t &actualMsgSize);
  static uint8_t deserializeCompletedMark(char *&buf);
  static CheckData deserialize(char *buf, uint32_t bufLen, uint32_t &actualSize);

  static uint32_t maxSize();
  static uint32_t maxCheckpointMsgSize();
  static constexpr uint16_t getNumOfParams() { return CHECK_DATA_LAST_PARAM; }

 private:
  CheckpointMsg *checkpointMsg_ = nullptr;
  bool completedMark_ = false;
};

typedef SerializableActiveWindow<kWorkWindowSize, 1, SeqNumData> SeqNumWindow;
typedef SerializableActiveWindow<kWorkWindowSize + checkpointWindowSize, checkpointWindowSize, CheckData> CheckWindow;

typedef std::shared_ptr<SeqNumWindow> SharedPtrSeqNumWindow;
typedef std::shared_ptr<CheckWindow> SharedPtrCheckWindow;

}  // namespace impl

}  // namespace bftEngine
