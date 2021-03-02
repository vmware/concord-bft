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

#include "reconfiguration/reconfiguration_handler.hpp"
#include "reconfiguration/dispatcher.hpp"
#include "reconfiguration/pruning_handler.hpp"
#include "Replica.hpp"
#pragma once

namespace bftEngine {

class RequestHandler : public IRequestsHandler {
 public:
  RequestHandler(IRequestsHandler *userHdlr) : userRequestsHandler_{userHdlr}, reconfig_dispatcher_{reconfig_handler_} {
    reconfig_handler_ = std::make_shared<concord::reconfiguration::ReconfigurationHandler>();
    pruning_handler_ = std::make_shared<concord::reconfiguration::PruningHandler>();
    reconfig_dispatcher_.addReconfigurationHandler(userHdlr->getReconfigurationHandler());
    reconfig_dispatcher_.addPruningHandler(userHdlr->getPruningHandler());
  }

  void execute(ExecutionRequestsQueue &requests, const std::string &batchCid, concordUtils::SpanWrapper &) override;

  void onFinishExecutingReadWriteRequests() override { userRequestsHandler_->onFinishExecutingReadWriteRequests(); }

 private:
  IRequestsHandler *const userRequestsHandler_ = nullptr;
  concord::reconfiguration::Dispatcher reconfig_dispatcher_;
};

}  // namespace bftEngine
