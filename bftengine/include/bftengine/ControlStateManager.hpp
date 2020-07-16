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

namespace bftEngine {

class ControlStateManager {
 public:
  void setStopAtNextCheckpoint();
  std::optional<uint64_t> getStopCheckpointToStopAt();

  ControlStateManager(IStateTransfer& state_transfer);
  ControlStateManager& operator=(const ControlStateManager&) = delete;
  ControlStateManager(const ControlStateManager&) = delete;

 private:
  IStateTransfer& state_transfer_;
};
}  // namespace bftEngine
