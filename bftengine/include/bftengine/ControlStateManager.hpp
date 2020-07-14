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
#include <string>
#include <unordered_map>

namespace bftEngine {

class IControlStateManager {
 public:
  enum ControlStateId {
    STOP_AT_NEXT_CHECKPOINT,
  };

  virtual bool saveControlState(const ControlStateId id, const std::string& controlState) = 0;
  virtual std::string getControlState(const ControlStateId id) = 0;
  virtual ~IControlStateManager() = default;
};

class InMemoryControlStateManager : public IControlStateManager {
  std::unordered_map<ControlStateId, std::string> controlStateMap_;

 public:
  virtual bool saveControlState(const ControlStateId id, const std::string& controlState) override {
    controlStateMap_[id] = controlState;
    return true;
  }
  virtual std::string getControlState(const ControlStateId id) override { return controlStateMap_[id]; }
  virtual ~InMemoryControlStateManager() = default;
};
}  // namespace bftEngine
