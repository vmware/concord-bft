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

#include "state_client.hpp"
#include "state_handler.hpp"
#include "config.hpp"
#include "Logger.hpp"
#include <vector>
#include <thread>
namespace cre {
class ClientReconfigurationEngine {
 public:
  ClientReconfigurationEngine(const config::Config& config,
                              std::shared_ptr<bft::client::Client> bftclient,
                              uint16_t id);
  void registerHandler(std::shared_ptr<state::IStateHandler> handler);
  void registerUpdateStateHandler(std::shared_ptr<state::IStateHandler> handler);
  ~ClientReconfigurationEngine();

 private:
  void main();
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("cre.ClientReconfigurationEngine"));
    return logger_;
  }
  std::shared_ptr<bft::client::Client> bftclient_;
  std::vector<std::shared_ptr<state::IStateHandler>> handlers_;
  std::vector<std::shared_ptr<state::IStateHandler>> updateStateHandlers_;
  std::unique_ptr<state::IStateClient> stateClient_;
  const config::Config& config_;
  std::atomic_bool stopped_{true};
  uint64_t lastKnownBlock_{0};
  std::thread mainThread_;
};
}  // namespace cre