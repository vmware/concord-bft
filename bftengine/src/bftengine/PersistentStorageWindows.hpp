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
  PRE_PREPARE_MSG = 1,
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

  static size_t serializeMsg(char *&buf, MessageBase *msg);
  static size_t serializeBoolean(char *&buf, const bool& boolean);

  static MessageBase *deserializeMsg(char *&buf, uint32_t bufLen, size_t &actualMsgSize);
  static bool deserializeBoolean(char *&buf);
  static SeqNumData deserialize(char *buf, uint32_t bufLen, uint32_t &actualSize);
  static uint32_t maxSize();
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
  COMPLETED_MARK = 1,
  CHECKPOINT_MSG = 2
};

class CheckData {
 public:
  CheckData(CheckpointMsg *checkpoint, bool completed) : completedMark_(completed), checkpointMsg_(checkpoint) {}

  CheckData() = default;

  bool equals(const CheckData &other) const;
  void reset();

  size_t serializeCompletedMark(char *&buf) const;
  size_t serializeCheckpointMsg(char *&buf) const;
  void serialize(char *buf, uint32_t bufLen, size_t &actualSize) const;

  static size_t serializeCompletedMark(char *&buf, const bool &completedMark);
  static size_t serializeCheckpointMsg(char *&buf, CheckpointMsg *msg);

  static bool deserializeCompletedMark(char *&buf);
  static CheckpointMsg *deserializeCheckpointMsg(char *buf, uint32_t bufLen, size_t &actualMsgSize);
  static CheckData deserialize(char *buf, uint32_t bufLen, uint32_t &actualSize);

  static uint32_t maxSize();
  static uint32_t maxCheckpointMsgSize();
  static constexpr uint16_t getNumOfParams() { return 2; }

 private:
  bool completedMark_ = false;
  CheckpointMsg *checkpointMsg_ = nullptr;
};

typedef SerializableActiveWindow<kWorkWindowSize, 1, SeqNumData> SeqNumWindow;
typedef SerializableActiveWindow<kWorkWindowSize + checkpointWindowSize, checkpointWindowSize, CheckData> CheckWindow;

typedef std::shared_ptr<SeqNumWindow> SharedPtrSeqNumWindow;
typedef std::shared_ptr<CheckWindow> SharedPtrCheckWindow;

}

}  // namespace bftEngine
