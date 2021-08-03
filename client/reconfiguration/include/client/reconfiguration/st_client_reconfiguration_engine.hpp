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
#include "iclient_reconfiguration_engine.hpp"
#include "cre_interfaces.hpp"
#include "config.hpp"
#include "Logger.hpp"
#include "Metrics.hpp"

#include <vector>
#include <thread>
namespace concord::client::reconfiguration {
class STClientReconfigurationEngine : public IClientReconfigurationEngine {
 public:
  STClientReconfigurationEngine(const Config& config,
                                IStateClient* stateClient,
                                std::shared_ptr<concordMetrics::Aggregator> aggregator);
  void registerHandler(std::shared_ptr<IStateHandler> handler, CreHandlerType type = REGULAR);
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    aggregator_ = aggregator;
    metrics_.SetAggregator(aggregator_);
  }
  ~STClientReconfigurationEngine();
  void start();
  void stop();

 private:
  void main();
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.client.reconfiguration.STClientReconfigurationEngine"));
    return logger_;
  }
  std::map<uint32_t, std::vector<std::shared_ptr<IStateHandler>>> handlers_;
  std::unique_ptr<IStateClient> stateClient_;
  Config config_;
  std::atomic_bool stopped_{true};
  uint64_t lastKnownBlock_{0};
  std::thread mainThread_;
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
  concordMetrics::Component metrics_;
  concordMetrics::CounterHandle invalid_handlers_;
  concordMetrics::CounterHandle errored_handlers_;
  concordMetrics::GaugeHandle last_known_block_;
};
}  // namespace concord::client::reconfiguration