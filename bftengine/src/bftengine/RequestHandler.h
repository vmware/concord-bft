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

#include "reconfiguration/dispatcher.hpp"
#include "IRequestHandler.hpp"
#include "Metrics.hpp"
#include "ReplicasInfo.hpp"
#include "Reconfiguration.hpp"
#include "ReplicaConfig.hpp"
#include <ccron/cron_table_registry.hpp>
#include <optional>

namespace bftEngine {

class RequestHandler : public IRequestsHandler {
 public:
  RequestHandler(
      std::shared_ptr<concordMetrics::Aggregator> aggregator_ = std::make_shared<concordMetrics::Aggregator>()) {
    reconfig_handler_.push_back(
        std::make_shared<impl::ReconfigurationHandler>(bftEngine::ReplicaConfig::instance().pathToOperatorPublicKey_,
                                                       bftEngine::ReplicaConfig::instance().operatorMsgSigningAlgo));
    for (const auto &rh : reconfig_handler_) {
      reconfig_dispatcher_.addReconfigurationHandler(rh);
    }
    reconfig_dispatcher_.addReconfigurationHandler(std::make_shared<impl::ClientReconfigurationHandler>());
    reconfig_dispatcher_.setAggregator(aggregator_);
  }

  void execute(ExecutionRequestsQueue &requests,
               std::optional<Timestamp> timestamp,
               const std::string &batchCid,
               concordUtils::SpanWrapper &) override;

  void preExecute(IRequestsHandler::ExecutionRequest &req,
                  std::optional<Timestamp> timestamp,
                  const std::string &batchCid,
                  concordUtils::SpanWrapper &parent_span) override;

  void setUserRequestHandler(std::shared_ptr<IRequestsHandler> userHdlr);

  void setReconfigurationHandler(std::shared_ptr<concord::reconfiguration::IReconfigurationHandler> rh,
                                 concord::reconfiguration::ReconfigurationHandlerType type =
                                     concord::reconfiguration::ReconfigurationHandlerType::REGULAR) override;

  void setCronTableRegistry(const std::shared_ptr<concord::cron::CronTableRegistry> &reg);
  void setPersistentStorage(const std::shared_ptr<bftEngine::impl::PersistentStorage> &persistent_storage) override;
  void onFinishExecutingReadWriteRequests() override { userRequestsHandler_->onFinishExecutingReadWriteRequests(); }
  std::shared_ptr<IRequestsHandler> getUserHandler() { return userRequestsHandler_; }

 private:
  std::shared_ptr<IRequestsHandler> userRequestsHandler_;
  concord::reconfiguration::Dispatcher reconfig_dispatcher_;
  std::shared_ptr<concord::cron::CronTableRegistry> cron_table_registry_;
};

}  // namespace bftEngine
