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

#include "ControlStateManager.hpp"
namespace bftEngine {

void ControlStateManager::setStopAtNextCheckpoint() {}

std::optional<uint64_t> ControlStateManager::getStopCheckpointToStopAt() { return {}; }

ControlStateManager::ControlStateManager(IStateTransfer& state_transfer) : state_transfer_{state_transfer} {
  state_transfer_.getStatus();  // Temporary, to escape not-used compilation error
}

}  // namespace bftEngine