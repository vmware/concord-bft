// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include "TesterReplica/strategy/ByzantineStrategy.hpp"

namespace concord::kvbc::strategy {
class ShufflePrePrepareMsgStrategy : public IByzantineStrategy {
 public:
  std::string getStrategyName() override;
  uint16_t getMessageCode() override;
  bool changeMessage(std::shared_ptr<MessageBase> &msg) override;
  ShufflePrePrepareMsgStrategy() = default;
  virtual ~ShufflePrePrepareMsgStrategy() = default;
};

}  // end of namespace concord::kvbc::strategy
