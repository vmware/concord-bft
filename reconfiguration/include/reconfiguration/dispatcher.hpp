// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "ireconfiguration.hpp"
#include "concord.cmf.hpp"
#include "OpenTracing.hpp"
#include "Logger.hpp"

namespace concord::reconfiguration {

// The dispatcher forwards all messages to their appropriate handlers.
// All handled messages are defined in the IReconfigurationHandler interface.
class Dispatcher {
 public:
  Dispatcher(std::shared_ptr<IReconfigurationHandler> handler) { addReconfigurationHandler(handler); }

  // This method is the gate for all reconfiguration actions. It works as
  // follows:
  // 1. Validate the request against the reconfiguration system operator (RSO)
  // public key
  // 2. Direct the request to the relevant handler
  // 3. Wrap the response in the concordResponse message
  //
  // Basically, we would like to write each reconfiguration write command to the
  // blockchain and document it as part of the state. This will be under the
  // responsibility of each handler to write its own commands to the blockchain.
  concord::messages::ReconfigurationResponse dispatch(const concord::messages::ReconfigurationRequest&,
                                                      uint64_t sequence_num);

  void addReconfigurationHandler(std::shared_ptr<IReconfigurationHandler> h) {
    if (h != nullptr) reconfig_handlers_.push_back(h);
  }
  void addPruningHandler(std::shared_ptr<IPruningHandler> h) {
    if (h != nullptr) pruning_handlers_.push_back(h);
  }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.reconfiguration"));
    return logger_;
  }
  std::vector<std::shared_ptr<IReconfigurationHandler>> reconfig_handlers_;
  std::vector<std::shared_ptr<IPruningHandler>> pruning_handlers_;
};

}  // namespace concord::reconfiguration
