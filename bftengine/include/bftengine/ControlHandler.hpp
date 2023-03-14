// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "Replica.hpp"
#include <functional>

namespace bftEngine {
using CallbackPriorities = IControlHandler::CallbackPriorities;
// The controlHandler class is the default implementation of the IControlHandler.
// It mark and defines who the system behaves on superStableCheckpoint, stableCheckpoint and pruning
class ControlHandler : public IControlHandler {
 public:
  void onSuperStableCheckpoint() override {
    onNoutOfNCheckpoint_ = true;
    for (auto& [prio, cbs] : onSuperStableCheckpointCallBack) {
      (void)prio;
      for (auto& cb : cbs) cb();
    }
  };
  void onStableCheckpoint() override {
    onNMinusFOutOfNCheckpoint_ = true;
    for (auto& [prio, cbs] : onStableCheckpointCallBack) {
      (void)prio;
      for (auto& cb : cbs) cb();
    }
    ControlStateManager::instance().checkForReplicaReconfigurationAction();
  }
  bool onPruningProcess() override { return onPruningProcess_; }
  bool isOnNOutOfNCheckpoint() const override { return onNoutOfNCheckpoint_; }
  bool isOnStableCheckpoint() const override { return onNMinusFOutOfNCheckpoint_; }
  void setOnPruningProcess(bool inProcess) override { onPruningProcess_ = inProcess; }
  void addOnSuperStableCheckpointCallBack(const std::function<void()>& cb,
                                          CallbackPriorities prio = IControlHandler::DEFAULT) override {
    onSuperStableCheckpointCallBack[prio].emplace_back(cb);
  }
  void addOnStableCheckpointCallBack(const std::function<void()>& cb,
                                     CallbackPriorities prio = IControlHandler::DEFAULT) override {
    onStableCheckpointCallBack[prio].emplace_back(cb);
  }

  void resetState() override {
    onNoutOfNCheckpoint_ = false;
    onNMinusFOutOfNCheckpoint_ = false;
    onPruningProcess_ = false;
    onSuperStableCheckpointCallBack.clear();
    onStableCheckpointCallBack.clear();
  }

 private:
  bool onNoutOfNCheckpoint_ = false;
  bool onNMinusFOutOfNCheckpoint_ = false;
  bool onPruningProcess_ = false;
  std::map<CallbackPriorities, std::vector<std::function<void()>>> onSuperStableCheckpointCallBack;
  std::map<CallbackPriorities, std::vector<std::function<void()>>> onStableCheckpointCallBack;
};

}  // namespace bftEngine
