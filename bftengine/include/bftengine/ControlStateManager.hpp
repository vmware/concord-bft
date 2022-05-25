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
#include <atomic>
#include "IStateTransfer.hpp"
#include "ReservedPagesClient.hpp"
#include "Serializable.h"
#include "SysConsts.hpp"
#include "callback_registry.hpp"
#include <mutex>
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
  bool isWedged() { return wedged; }
  void wedge() { wedged = true; }
  void unwedge() { wedged = false; }
  void markRemoveMetadata(bool include_st = true) { removeMetadataCbRegistry_.invokeAll(include_st); }
  void setPruningProcess(bool onPruningProcess) {
    onPruningProcess ? pruning_lock_.lock() : pruning_lock_.unlock();
    onPruningProcess_ = onPruningProcess;
  }
  bool getPruningProcessStatus() const { return onPruningProcess_; }
  void waitForPruningIfNeeded() { std::unique_lock lock_(pruning_lock_); }
  bool getRestartBftFlag() const { return restartBftEnabled_; }
  void setRestartBftFlag(bool bft) { restartBftEnabled_ = bft; }
  void setRemoveMetadataFunc(std::function<void(bool)> fn) { removeMetadataCbRegistry_.add(fn); }
  void setRestartReadyFunc(std::function<void(uint8_t, const std::string&)> fn) { sendRestartReady_ = fn; }
  void setGetNewConfigurationCallBack(std::function<void(const std::string&, const std::string&)> fn) {
    getNewConfigurationRegistry_.add(fn);
  }
  void getNewConfiguration(const std::string& config_descriptor, const std::string& token) {
    getNewConfigurationRegistry_.invokeAll(config_descriptor, token);
  }
  void sendRestartReadyToAllReplica(uint8_t reason, const std::string& extraData) {
    sendRestartReady_(reason, extraData);
  }
  void addOnRestartProofCallBack(std::function<void()> cb,
                                 uint8_t reason,
                                 RestartProofHandlerPriorities priority = ControlStateManager::DEFAULT);
  void onRestartProof(const SeqNum&, uint8_t reason);
  void checkForReplicaReconfigurationAction();
  void restart();

 private:
  ControlStateManager() = default;
  ControlStateManager& operator=(const ControlStateManager&) = delete;
  ControlStateManager(const ControlStateManager&) = delete;

  uint64_t wedgePoint{0};
  bool wedged = false;
  std::atomic_bool restartBftEnabled_ = false;
  std::unordered_map<uint8_t, SeqNum> hasRestartProofAtSeqNum_;  // reason for restart is the key
  std::atomic_bool onPruningProcess_ = false;
  concord::util::CallbackRegistry<bool> removeMetadataCbRegistry_;
  concord::util::CallbackRegistry<const std::string&, const std::string&> getNewConfigurationRegistry_;
  std::function<void(uint8_t, const std::string&)> sendRestartReady_;
  // reason for restart is the key
  std::unordered_map<uint8_t, std::map<uint32_t, concord::util::CallbackRegistry<>>> onRestartProofCbRegistry_;
  std::mutex pruning_lock_;
};
}  // namespace bftEngine
