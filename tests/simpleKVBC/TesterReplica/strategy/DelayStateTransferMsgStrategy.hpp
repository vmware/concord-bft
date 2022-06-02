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

#include "Logger.hpp"
#include "TesterReplica/strategy/ByzantineStrategy.hpp"

namespace concord::kvbc::strategy {

// This strategy is used to add a delay between all State Transfer messages sent between replicas
// TODO - in practice, it would have been better to have a generel way to delay one of many/few of
// many/all messages. But currently, IByzantineStrategy supports only single message code per strategy.
class DelayStateTransferMsgStrategy : public IByzantineStrategy {
 public:
  std::string getStrategyName() override { return CLASSNAME(DelayStateTransferMsgStrategy); }
  uint16_t getMessageCode() override { return static_cast<uint16_t>(MsgCode::StateTransfer); }
  bool changeMessage(std::shared_ptr<bftEngine::impl::MessageBase>& msg) override {
    if (delayMillisec_ > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(delayMillisec_));
    }
    return false;
  }
  explicit DelayStateTransferMsgStrategy(logging::Logger& logger, size_t delayMillisec)
      : logger_(logger), delayMillisec_(delayMillisec) {}
  virtual ~DelayStateTransferMsgStrategy() = default;

 private:
  logging::Logger logger_;
  size_t delayMillisec_;
};

}  // namespace concord::kvbc::strategy
