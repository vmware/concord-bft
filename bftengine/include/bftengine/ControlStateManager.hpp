// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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
#include <optional>
#include "IStateTransfer.hpp"
#include "ReservedPagesClient.hpp"
#include "Serializable.h"
#include "SysConsts.hpp"
#include "callback_registry.hpp"

namespace bftEngine {
class ControlStateManager {
 public:
  enum RestartProofHandlerPriorities { HIGH = 0, DEFAULT = 20, LOW = 40 };
  static ControlStateManager& instance() {
    static ControlStateManager instance_;
    return instance_;
  }
  void setStopAtNextCheckpoint(int64_t currentSeqNum);
  std::optional<int64_t> getCheckpointToStopAt();

  void markRemoveMetadata(bool include_st = true) { removeMetadataCbRegistry_.invokeAll(include_st); }
  void setPruningProcess(bool onPruningProcess) { onPruningProcess_ = onPruningProcess; }
  bool getPruningProcessStatus() const { return onPruningProcess_; }
  bool getRestartBftFlag() const { return restartBftEnabled_; }
  void setRestartBftFlag(bool bft) { restartBftEnabled_ = bft; }

  void setRemoveMetadataFunc(std::function<void(bool)> fn) { removeMetadataCbRegistry_.add(fn); }
  void setRestartReadyFunc(std::function<void()> fn) { sendRestartReady_ = fn; }
  void sendRestartReadyToAllReplica() { sendRestartReady_(); }
  void addOnRestartProofCallBack(std::function<void()> cb,
                                 RestartProofHandlerPriorities priority = ControlStateManager::DEFAULT);
  void onRestartProof(const SeqNum&);
  void checkForReplicaReconfigurationAction();
  void restart();

 private:
  ControlStateManager() = default;
  ControlStateManager& operator=(const ControlStateManager&) = delete;
  ControlStateManager(const ControlStateManager&) = delete;

  uint64_t wedgePoint{0};
  std::atomic_bool restartBftEnabled_ = false;
  std::optional<SeqNum> hasRestartProofAtSeqNum_ = std::nullopt;
  std::atomic_bool onPruningProcess_ = false;
  concord::util::CallbackRegistry<bool> removeMetadataCbRegistry_;
  std::function<void()> sendRestartReady_;
  std::map<uint32_t, concord::util::CallbackRegistry<>> onRestartProofCbRegistry_;
};
}  // namespace bftEngine
