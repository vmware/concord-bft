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
#include <client_reconfiguration_engine.hpp>

namespace cre {

ClientReconfigurationEngine::ClientReconfigurationEngine(const config::Config& config, state::IStateClient* stateClient)
    : stateClient_{stateClient},
      config_{config},
      aggregator_{std::make_shared<concordMetrics::Aggregator>()},
      metrics_{concordMetrics::Component("client_reconfiguration_engine", aggregator_)},
      invalid_handlers_{metrics_.RegisterCounter("invalid_handlers")},
      errored_handlers_{metrics_.RegisterCounter("errored_handlers")} {
  metrics_.Register();
}

void ClientReconfigurationEngine::stop() {
  stopped_ = true;
  mainThread_.join();
}

void ClientReconfigurationEngine::start() {
  stopped_ = false;
  mainThread_ = std::thread([&] { main(); });
}

void ClientReconfigurationEngine::main() {
  while (!stopped_) {
    try {
      if (config_.interval_timeout_ms_ > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.interval_timeout_ms_));
        metrics_.UpdateAggregator();
      }
      auto update = stateClient_->getNextState(lastKnownBlock_);
      if (update.block == lastKnownBlock_) continue;

      // Execute the reconfiguration command
      for (auto& h : handlers_) {
        if (!h->validate(update)) {
          invalid_handlers_.Get().Inc();
          continue;
        }
        if (!h->execute(update)) {
          LOG_ERROR(getLogger(), "error while executing the handlers");
          errored_handlers_.Get().Inc();
          continue;
        }
      }

      // Update the client state on the chain
      for (auto& h : updateStateHandlers_) {
        if (!h->validate(update)) {
          invalid_handlers_.Get().Inc();
          continue;
        }
        if (!h->execute(update)) {
          LOG_ERROR(getLogger(), "error while executing updating the state on the chain");
          errored_handlers_.Get().Inc();
          continue;
        }
      }
      lastKnownBlock_ = update.block;
    } catch (const std::exception& e) {
      LOG_ERROR(getLogger(), "error while executing the handlers " << e.what());
      errored_handlers_.Get().Inc();
    }
  }
}
void ClientReconfigurationEngine::registerHandler(std::shared_ptr<state::IStateHandler> handler) {
  if (handler != nullptr) handlers_.push_back(handler);
}

void ClientReconfigurationEngine::registerUpdateStateHandler(std::shared_ptr<state::IStateHandler> handler) {
  if (handler != nullptr) updateStateHandlers_.push_back(handler);
}
}  // namespace cre