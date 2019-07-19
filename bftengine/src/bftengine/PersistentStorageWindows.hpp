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
#include "PrePrepareMsg.hpp"
#include "SignedShareMsgs.hpp"
#include "FullCommitProofMsg.hpp"
#include "CheckpointMsg.hpp"
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
  SLOW_STARTED = 6
};

class SeqNumData {
 public:
  SeqNumData(PrePrepareMsg *prePrepare, FullCommitProofMsg *fullCommitProof,
             PrepareFullMsg *prepareFull, CommitFullMsg *commitFull, bool forceComplete, bool slowStart) :
      prePrepareMsg_(prePrepare), fullCommitProofMsg_(fullCommitProof),
      prepareFullMsg_(prepareFull), commitFullMsg_(commitFull),
      forceCompleted_(forceComplete), slowStarted_(slowStart) {}

  SeqNumData() = default;

  bool equals(const SeqNumData &other) const;
  void reset();

  size_t serializePrePrepareMsg(char *&buf) const;
  size_t serializeFullCommitProofMsg(char *&buf) const;
  size_t serializePrepareFullMsg(char *&buf) const;
  size_t serializeCommitFullMsg(char *&buf) const;
  size_t serializeForceCompleted(char *&buf) const;
  size_t serializeSlowStarted(char *&buf) const;
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

  void setPrePrepareMsg(MessageBase *msg) { prePrepareMsg_ = (PrePrepareMsg *) msg; }
  void setFullCommitProofMsg(MessageBase *msg) { fullCommitProofMsg_ = (FullCommitProofMsg *) msg; }
  void setPrepareFullMsg(MessageBase *msg) { prepareFullMsg_ = (PrepareFullMsg *) msg; }
  void setCommitFullMsg(MessageBase *msg) { commitFullMsg_ = (CommitFullMsg *) msg; }
  void setSlowStarted(const bool &slowStarted) { slowStarted_ = slowStarted; }
  void setForceCompleted(const bool &forceCompleted) { forceCompleted_ = forceCompleted; }

  static size_t serializeMsg(char *&buf, MessageBase *msg);
  static size_t serializeBoolean(char *&buf, const bool &boolean);

  static MessageBase *deserializeMsg(char *&buf, uint32_t bufLen, size_t &actualMsgSize);
  static bool deserializeBoolean(char *&buf);
  static SeqNumData deserialize(char *buf, uint32_t bufLen, uint32_t &actualSize);
  static uint32_t maxSize();
  static uint32_t maxPrePrepareMsgSize();
  static uint32_t maxFullCommitProofMsgSize();
  static uint32_t maxPrepareFullMsgSize();
  static uint32_t maxCommitFullMsgSize();
  static constexpr uint16_t getNumOfParams() { return 6; }

 private:
  static bool compareMessages(MessageBase *msg, MessageBase *otherMsg);

 private:
  PrePrepareMsg *prePrepareMsg_ = nullptr;
  FullCommitProofMsg *fullCommitProofMsg_ = nullptr;
  PrepareFullMsg *prepareFullMsg_ = nullptr;
  CommitFullMsg *commitFullMsg_ = nullptr;
  bool forceCompleted_ = false;
  bool slowStarted_ = false;
};

enum CheckDataParameters {
  CHECK_DATA_FIRST_PARAM = 1,
  CHECKPOINT_MSG = CHECK_DATA_FIRST_PARAM,
  COMPLETED_MARK = 2
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
  void setCheckpointMsg(MessageBase *msg) { checkpointMsg_ = (CheckpointMsg *) msg; }
  void setCompletedMark(const bool &completedMark) { completedMark_ = completedMark; }

  static size_t serializeCheckpointMsg(char *&buf, CheckpointMsg *msg);
  static size_t serializeCompletedMark(char *&buf, const bool &completedMark);

  static CheckpointMsg *deserializeCheckpointMsg(char *&buf, uint32_t bufLen, size_t &actualMsgSize);
  static bool deserializeCompletedMark(char *&buf);
  static CheckData deserialize(char *buf, uint32_t bufLen, uint32_t &actualSize);

  static uint32_t maxSize();
  static uint32_t maxCheckpointMsgSize();
  static constexpr uint16_t getNumOfParams() { return 2; }

 private:
  CheckpointMsg *checkpointMsg_ = nullptr;
  bool completedMark_ = false;
};

typedef SerializableActiveWindow<kWorkWindowSize, 1, SeqNumData> SeqNumWindow;
typedef SerializableActiveWindow<kWorkWindowSize + checkpointWindowSize, checkpointWindowSize, CheckData> CheckWindow;

typedef std::shared_ptr<SeqNumWindow> SharedPtrSeqNumWindow;
typedef std::shared_ptr<CheckWindow> SharedPtrCheckWindow;

}

}  // namespace bftEngine
