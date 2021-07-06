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

#include "reconfiguration/reconfiguration_handler.hpp"
#include "reconfiguration/dispatcher.hpp"
#include "IRequestHandler.hpp"

#include <ccron/cron_table_registry.hpp>
#include <optional>

namespace bftEngine {

class RequestHandler : public IRequestsHandler {
 public:
  RequestHandler() {
    reconfig_handler_ = std::make_shared<concord::reconfiguration::ReconfigurationHandler>();
    reconfig_dispatcher_.addReconfigurationHandler(reconfig_handler_);
  }

  void execute(ExecutionRequestsQueue &requests,
               std::optional<Timestamp> timestamp,
               const std::string &batchCid,
               concordUtils::SpanWrapper &) override;

  void setUserRequestHandler(std::shared_ptr<IRequestsHandler> userHdlr) {
    if (userHdlr) {
      userRequestsHandler_ = userHdlr;
      reconfig_dispatcher_.addReconfigurationHandler(userHdlr->getReconfigurationHandler());
    }
  }

  void setReconfigurationHandler(std::shared_ptr<concord::reconfiguration::IReconfigurationHandler> rh,
                                 concord::reconfiguration::ReconfigurationHandlerType type =
                                     concord::reconfiguration::ReconfigurationHandlerType::REGULAR) override {
    reconfig_dispatcher_.addReconfigurationHandler(rh, type);
  }

  void setCronTableRegistry(const std::shared_ptr<concord::cron::CronTableRegistry> &reg) {
    cron_table_registry_ = reg;
  }

  void onFinishExecutingReadWriteRequests() override { userRequestsHandler_->onFinishExecutingReadWriteRequests(); }

 private:
  std::shared_ptr<IRequestsHandler> userRequestsHandler_ = nullptr;
  concord::reconfiguration::Dispatcher reconfig_dispatcher_;
  std::shared_ptr<concord::cron::CronTableRegistry> cron_table_registry_;
};

}  // namespace bftEngine
