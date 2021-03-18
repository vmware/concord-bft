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

class ControlHandler : public IControlHandler {
 public:
  void onSuperStableCheckpoint() override {
    onNoutOfNCheckpoint = true;
    for (auto& cb : onSuperStableCheckpointCallBack) cb();
  };
  void onStableCheckpoint() override { onNMinusFOutOfNCheckpoint = true; }
  bool onPruningProcess() override { return onPruningProcess_; }
  bool isOnNOutOfNCheckpoint() const override { return onNoutOfNCheckpoint; }
  bool isOnStableCheckpoint() const override { return onNMinusFOutOfNCheckpoint; }
  void setOnPruningProcess(bool inProcess) override { onPruningProcess_ = inProcess; }
  void addOnSuperStableCheckpointCallBack(const std::function<void()>& cb) override {
    onSuperStableCheckpointCallBack.emplace_back(cb);
  }

 private:
  bool onNoutOfNCheckpoint = false;
  bool onNMinusFOutOfNCheckpoint = false;
  bool onPruningProcess_ = false;
  std::vector<std::function<void()>> onSuperStableCheckpointCallBack;
};

}  // namespace bftEngine
