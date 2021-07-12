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
namespace cre {
class ClientReconfigurationEngine {
 public:
  ClientReconfigurationEngine(const Config& config,
                              IStateClient* stateClient,
                              std::shared_ptr<concordMetrics::Aggregator> aggregator);
  void registerHandler(std::shared_ptr<IStateHandler> handler);
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    aggregator_ = aggregator;
    metrics_.SetAggregator(aggregator_);
  }
  ~ClientReconfigurationEngine();
  void start();
  void stop();

 private:
  void main();
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("cre.bft.ClientReconfigurationEngine"));
    return logger_;
  }
  std::vector<std::shared_ptr<IStateHandler>> handlers_;
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
}  // namespace cre