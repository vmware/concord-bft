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

ClientReconfigurationEngine::ClientReconfigurationEngine(const config::Config& config,
                                                         std::shared_ptr<bft::client::Client> bftclient,
                                                         uint16_t id)
    : bftclient_{bftclient},
      stateClient_{std::make_unique<state::PullBasedStateClient>(bftclient_, id)},
      config_{config} {
  stopped_ = false;
  mainThread_ = std::thread([&] { main(); });
}
ClientReconfigurationEngine::~ClientReconfigurationEngine() {
  stopped_ = true;
  mainThread_.join();
}

void ClientReconfigurationEngine::main() {
  while (!stopped_) {
    try {
      std::this_thread::sleep_for(std::chrono::milliseconds(config_.interval_timeout_ms_));
      auto update = stateClient_->getNextState(lastKnownBlock_);
      if (update.block == lastKnownBlock_) continue;

      // Execute the reconfiguration command
      for (auto& h : handlers_) {
        if (!h->validate(update)) continue;
        if (!h->execute(update)) {
          LOG_ERROR(getLogger(), "error while executing the handlers");
          continue;
        }
      }

      // Update the client state on the chain
      for (auto& h : updateStateHandlers_) {
        if (!h->validate(update)) continue;
        if (!h->execute(update)) {
          LOG_ERROR(getLogger(), "error while executing updating the state on the chain");
          continue;
        }
      }
      lastKnownBlock_ = update.block;
    } catch (const std::exception& e) {
      LOG_ERROR(getLogger(), "error while executing the handlers " << e.what());
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