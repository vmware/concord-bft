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
#include "client/reconfiguration/st_client_reconfiguration_engine.hpp"

namespace concord::client::reconfiguration {

STClientReconfigurationEngine::STClientReconfigurationEngine(const Config& config,
                                                             IStateClient* stateClient,
                                                             std::shared_ptr<concordMetrics::Aggregator> aggregator)
    : stateClient_{stateClient},
      config_{config},
      aggregator_{aggregator},
      metrics_{concordMetrics::Component("client_reconfiguration_engine", aggregator_)},
      invalid_handlers_{metrics_.RegisterCounter("invalid_handlers")},
      errored_handlers_{metrics_.RegisterCounter("errored_handlers")},
      last_known_block_(metrics_.RegisterGauge("last_known_block", 0)) {
  metrics_.Register();
}

void STClientReconfigurationEngine::main() {
  while (!stopped_) {
    try {
      auto update = stateClient_->getNextState(lastKnownBlock_);
      if (stopped_) return;
      if (update.blockid <= lastKnownBlock_) continue;

      // Execute the reconfiguration command
      auto valid = true;
      auto execStatus = true;
      for (auto& [key, val] : handlers_) {
        (void)key;
        for (auto& h : val) {
          valid &= h->validate(update);
          if (!valid) {
            invalid_handlers_++;
            continue;
          }
          WriteState out_state;
          execStatus &= h->execute(update, out_state);
          // skip if previous handler failed to execute
          if (!execStatus) {
            LOG_ERROR(getLogger(), "error while executing the handlers");
            errored_handlers_++;
            continue;
          }
        }
      }
      if (valid && execStatus) {
        lastKnownBlock_ = update.blockid;
        last_known_block_.Get().Set(lastKnownBlock_);
      }
    } catch (const std::exception& e) {
      LOG_ERROR(getLogger(), "error while executing the handlers " << e.what());
      errored_handlers_++;
    }
    metrics_.UpdateAggregator();
  }
}
void STClientReconfigurationEngine::registerHandler(std::shared_ptr<IStateHandler> handler, CreHandlerType type) {
  if (handler != nullptr) handlers_[type].push_back(handler);
}

STClientReconfigurationEngine::~STClientReconfigurationEngine() {
  if (!stopped_) {
    try {
      stateClient_->stop();
      stopped_ = true;
      mainThread_.join();
    } catch (std::exception& e) {
      LOG_ERROR(getLogger(), e.what());
    }
  }
}
void STClientReconfigurationEngine::start() {
  auto initial_state = stateClient_->getLatestClientUpdate(config_.id_);
  lastKnownBlock_ = initial_state.blockid;
  last_known_block_.Get().Set(lastKnownBlock_);
  metrics_.UpdateAggregator();
  stateClient_->start(lastKnownBlock_);
  stopped_ = false;
  mainThread_ = std::thread([&] { main(); });
}
void STClientReconfigurationEngine::stop() {
  if (stopped_) return;
  stateClient_->stop();
  stopped_ = true;
  try {
    mainThread_.join();
  } catch (std::exception& e) {
    LOG_ERROR(getLogger(), e.what());
  }
}
}  // namespace concord::client::reconfiguration