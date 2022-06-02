// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <unordered_set>

#include "Logger.hpp"
#include "TesterReplica/strategy/ByzantineStrategy.hpp"
#include "StrategyUtils.hpp"

namespace concord::kvbc::strategy {

// This strategy is used to corrupt checkpoint messages sent between replicas.
class CorruptCheckpointMsgStrategy : public IByzantineStrategy {
 public:
  static std::string strategyName() { return CLASSNAME(CorruptCheckpointMsgStrategy); }

  std::string getStrategyName() override;
  uint16_t getMessageCode() override;
  bool changeMessage(std::shared_ptr<bftEngine::impl::MessageBase>& msg) override;
  CorruptCheckpointMsgStrategy(logging::Logger& logger, std::unordered_set<ReplicaId>&& byzantine_replica_ids);
  virtual ~CorruptCheckpointMsgStrategy() = default;

 private:
  logging::Logger logger_;
  std::unordered_set<ReplicaId> byzantine_replica_ids_;
};

}  // namespace concord::kvbc::strategy
