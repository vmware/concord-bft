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

#include "SequenceWithActiveWindow.hpp"
#include "PrePrepareMsg.hpp"
#include "SignedShareMsgs.hpp"
#include "FullCommitProofMsg.hpp"
#include "CheckpointMsg.hpp"

namespace bftEngine {

struct SeqNumData {
  PrePrepareMsg *prePrepareMsg = nullptr;
  FullCommitProofMsg *fullCommitProofMsg = nullptr;
  PrepareFullMsg *prepareFullMsg = nullptr;
  CommitFullMsg *commitFullMsg = nullptr;
  bool forceCompleted = false;
  bool slowStarted = false;

  SeqNumData(PrePrepareMsg *prePrepare,
             FullCommitProofMsg *fullCommitProof,
             PrepareFullMsg *prepareFull,
             CommitFullMsg *commitFull,
             bool forceComplete,
             bool slowStart) :
      prePrepareMsg(prePrepare),
      fullCommitProofMsg(fullCommitProof),
      prepareFullMsg(prepareFull),
      commitFullMsg(commitFull),
      forceCompleted(forceComplete),
      slowStarted(slowStart) {}

  SeqNumData() = default;

  bool operator==(const SeqNumData &other) const;
  SeqNumData &operator=(const SeqNumData &other);
  void reset();
  void serialize(char *&buf, uint32_t bufLen, size_t &actualSize) const;

  static SeqNumData deserialize(
      char *&buf, uint32_t bufLen, uint32_t &actualSize);
  static uint32_t maxSize();

 private:
  static bool compareMessages(MessageBase *msg, MessageBase *otherMsg);
};

struct CheckData {
  CheckpointMsg *checkpointMsg = nullptr;
  bool completedMark = false;

  CheckData(CheckpointMsg *checkpoint, bool completed) :
      checkpointMsg(checkpoint),
      completedMark(completed) {}

  CheckData() = default;

  bool operator==(const CheckData &other) const;
  CheckData &operator=(const CheckData &other);
  void reset();
  void serialize(char *&buf, uint32_t bufLen, size_t &actualSize) const;

  static CheckData deserialize(
      char *&buf, uint32_t bufLen, uint32_t &actualSize);
  static uint32_t maxSize();
};

struct WindowFuncs {
  static void init(SeqNumData &seqNumData, void *data) {}
  static void free(SeqNumData &seqNumData) { reset(seqNumData); }
  static void reset(SeqNumData &seqNumData) { seqNumData.reset(); }

  static void init(CheckData &checkData, void *data) {}
  static void free(CheckData &checkData) { reset(checkData); }
  static void reset(CheckData &checkData) { checkData.reset(); }
};

typedef SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumData,
                                 WindowFuncs> SeqNumWindow;

typedef SequenceWithActiveWindow<kWorkWindowSize + checkpointWindowSize,
                                 checkpointWindowSize, SeqNum, CheckData,
                                 WindowFuncs> CheckWindow;

}  // namespace bftEngine
