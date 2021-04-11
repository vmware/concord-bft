// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "Replica.hpp"
#include "SysConsts.hpp"
#include "PrimitiveTypes.hpp"
#include "Bitmap.hpp"
#include "PersistentStorageWindows.hpp"

namespace bftEngine {
namespace impl {

class PersistentStorage;
class SigManager;
class ReplicasInfo;
class ViewsManager;
class PrePrepareMsg;
class FullCommitProofMsg;
class PrepareFullMsg;
class CommitFullMsg;
class CheckpointMsg;

// This struct represents the BFT data loaded from persistent storage
struct LoadedReplicaData {
  LoadedReplicaData() : repConfig(ReplicaConfig::instance()) {}
  ReplicaConfig &repConfig;
  ReplicasInfo *repsInfo = nullptr;
  ViewsManager *viewsManager = nullptr;
  SeqNum primaryLastUsedSeqNum = 0;
  SeqNum lastStableSeqNum = 0;
  SeqNum lastExecutedSeqNum = 0;
  SeqNum strictLowerBoundOfSeqNums = 0;

  ViewNum lastViewThatTransferredSeqNumbersFullyExecuted = 0;

  SeqNum maxSeqNumTransferredFromPrevViews = 0;
  SeqNumData seqNumWinArr[kWorkWindowSize];
  CheckData checkWinArr[1 + kWorkWindowSize / checkpointWindowSize];

  bool isExecuting = false;
  Bitmap validRequestsThatAreBeingExecuted;
};

class ReplicaLoader {
 public:
  enum class ErrorCode {
    Success = 0,
    NoDataInStorage = 0x100,
    InconsistentData = 0x200,
    // NB: consider to add specific error codes
  };

  static LoadedReplicaData loadReplica(std::shared_ptr<PersistentStorage> &p, ErrorCode &outErrCode);
};

}  // namespace impl
}  // namespace bftEngine
