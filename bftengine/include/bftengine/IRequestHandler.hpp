// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <cstdint>
#include <optional>
#include <string>
#include <functional>
#include <deque>
#include "OpenTracing.hpp"
#include "TimeService.hpp"
#include "ISystemResourceEntity.hpp"
#include "PersistentStorageImp.hpp"

namespace concord::reconfiguration {
class IReconfigurationHandler;
enum ReconfigurationHandlerType : unsigned int;
}  // namespace concord::reconfiguration

namespace concord::cron {
class CronTableRegistry;
}

namespace bftEngine {
class IRequestsHandler {
 public:
  struct ExecutionRequest {
    uint16_t clientId = 0;
    uint64_t executionSequenceNum = 0;
    std::string cid;
    uint64_t flags = 0;  // copy of ClientRequestMsg flags
    uint32_t requestSize = 0;
    const char *request;
    std::string signature;
    uint32_t maxReplySize = 0;
    char *outReply = nullptr;
    uint64_t requestSequenceNum = executionSequenceNum;
    uint32_t outExecutionStatus = 1;  // UNKNOWN
    uint32_t outActualReplySize = 0;
    uint32_t outReplicaSpecificInfoSize = 0;
    uint64_t blockId = 0;
  };

  static std::shared_ptr<IRequestsHandler> createRequestsHandler(
      std::shared_ptr<IRequestsHandler> userReqHandler,
      const std::shared_ptr<concord::cron::CronTableRegistry> &,
      concord::performance::ISystemResourceEntity &);
  typedef std::deque<ExecutionRequest> ExecutionRequestsQueue;

  virtual void execute(ExecutionRequestsQueue &requests,
                       std::optional<Timestamp> timestamp,
                       const std::string &batchCid,
                       concordUtils::SpanWrapper &parent_span) = 0;

  virtual void preExecute(IRequestsHandler::ExecutionRequest &req,
                          std::optional<Timestamp> timestamp,
                          const std::string &batchCid,
                          concordUtils::SpanWrapper &parent_span) = 0;

  virtual void onFinishExecutingReadWriteRequests() {}

  std::vector<std::shared_ptr<concord::reconfiguration::IReconfigurationHandler>> getReconfigurationHandler() const {
    return reconfig_handler_;
  }
  virtual void setReconfigurationHandler(std::shared_ptr<concord::reconfiguration::IReconfigurationHandler> rh,
                                         concord::reconfiguration::ReconfigurationHandlerType type =
                                             static_cast<concord::reconfiguration::ReconfigurationHandlerType>(1)) {
    reconfig_handler_.push_back(rh);
  }
  virtual void setPersistentStorage(const std::shared_ptr<bftEngine::impl::PersistentStorage> &persistent_storage) {}

  virtual ~IRequestsHandler() = default;

 protected:
  std::vector<std::shared_ptr<concord::reconfiguration::IReconfigurationHandler>> reconfig_handler_;
};
}  // namespace bftEngine
