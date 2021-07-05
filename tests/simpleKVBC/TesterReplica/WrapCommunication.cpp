// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <utility>

#include "messages/MsgCode.hpp"
#include "WrapCommunication.hpp"
#include "ReplicaConfig.hpp"

namespace bft::communication {

using bftEngine::impl::MessageBase;
using concord::kvbc::strategy::IByzantineStrategy;
using bftEngine::ReplicaConfig;

std::map<uint16_t, std::shared_ptr<IByzantineStrategy>> WrapCommunication::changeStrategy;

void WrapCommunication::addStrategy(uint16_t msgCode, std::shared_ptr<IByzantineStrategy> byzantineStrategy) {
  WrapCommunication::changeStrategy.insert(std::make_pair(msgCode, byzantineStrategy));
}

bool WrapCommunication::changeMesssage(std::vector<uint8_t> const& msg, std::shared_ptr<MessageBase>& newMsg) {
  bool is_strategy_changed = false;
  if (msg.size() <= ReplicaConfig::instance().getmaxExternalMessageSize() &&
      msg.size() >= sizeof(MessageBase::Header)) {
    auto* msgBody = (MessageBase::Header*)std::malloc(msg.size());
    memcpy(msgBody, msg.data(), msg.size());
    auto node = ReplicaConfig::instance().getreplicaId();
    newMsg = std::make_shared<MessageBase>(node, msgBody, msg.size(), true);
    if (newMsg) {
      auto it = changeStrategy.find(static_cast<uint16_t>(newMsg->type()));
      if (it != changeStrategy.end()) {
        is_strategy_changed = it->second->changeMessage(newMsg);
      }
    }
  }
  return is_strategy_changed;
}

void WrapCommunication::addStrategies(std::string const& strategies,
                                      char delim,
                                      std::vector<std::shared_ptr<IByzantineStrategy>> const& allStrategies) {
  std::stringstream stgs(strategies);
  while (stgs.good()) {
    std::string strategy;
    std::getline(stgs, strategy, delim);
    for (auto const& s : allStrategies) {
      if (s->getStrategyName() == strategy) {
        addStrategy(s->getMessageCode(), s);
      }
    }
  }
}

}  // namespace bft::communication
