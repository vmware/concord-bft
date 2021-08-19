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
#include "client/reconfiguration/client_reconfiguration_engine.hpp"

namespace concord::client::reconfiguration {

ClientReconfigurationEngine::ClientReconfigurationEngine(const Config& config,
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

void ClientReconfigurationEngine::main() {
  while (!stopped_) {
    try {
      auto update = stateClient_->getNextState();
      if (update.data.empty()) continue;
      if (stopped_) return;
      // Execute the reconfiguration command
      for (auto& h : handlers_) {
        if (!h->validate(update)) {
          invalid_handlers_++;
          continue;
        }
        WriteState out_state;
        if (!h->execute(update, out_state)) {
          LOG_ERROR(getLogger(), "error while executing the handlers");
          errored_handlers_++;
          continue;
        }
        if (!out_state.data.empty()) {
          stateClient_->updateState(out_state);
        }
      }
      if (last_known_block_.Get().Get() < update.blockid) last_known_block_.Get().Set(update.blockid);
    } catch (const std::exception& e) {
      LOG_ERROR(getLogger(), "error while executing the handlers " << e.what());
      errored_handlers_++;
    }
    metrics_.UpdateAggregator();
  }
}
void ClientReconfigurationEngine::registerHandler(std::shared_ptr<IStateHandler> handler) {
  if (handler != nullptr) handlers_.push_back(handler);
}

ClientReconfigurationEngine::~ClientReconfigurationEngine() {
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
void ClientReconfigurationEngine::start() {
  stateClient_->start();
  stopped_ = false;
  mainThread_ = std::thread([&] { main(); });
}
void ClientReconfigurationEngine::stop() {
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