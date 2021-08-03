// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "cre_interfaces.hpp"
#include "config.hpp"
#include "Logger.hpp"
#include "Metrics.hpp"

#include <vector>
#include <thread>
namespace concord::client::reconfiguration {
class IClientReconfigurationEngine {
 public:
  enum CreHandlerType : unsigned int { PRE, REGULAR, POST };
  virtual void registerHandler(std::shared_ptr<IStateHandler> handler, CreHandlerType type = REGULAR) = 0;
  virtual void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) = 0;
  virtual ~IClientReconfigurationEngine() = default;
  virtual void start() = 0;
  virtual void stop() = 0;
};
}  // namespace concord::client::reconfiguration