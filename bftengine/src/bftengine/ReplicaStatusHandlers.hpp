// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "ReplicaConfig.hpp"
#include "InternalReplicaApi.hpp"
#include "Metrics.hpp"

namespace bftEngine::impl {

class ReplicaStatusHandlers {
 public:
  ReplicaStatusHandlers(InternalReplicaApi &replica);
  virtual ~ReplicaStatusHandlers() = default;

  void registerStatusHandlers() const;

  // Generate diagnostics status replies
  std::string preExecutionStatus(std::shared_ptr<concordMetrics::Aggregator> aggregator) const;

 private:
  InternalReplicaApi &replica_;
};

}  // namespace bftEngine::impl