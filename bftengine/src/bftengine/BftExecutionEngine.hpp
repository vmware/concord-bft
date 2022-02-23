// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once
#include "Logger.hpp"
#include "Bitmap.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "ClientsManager.hpp"
#include "ReplicasInfo.hpp"
#include "PersistentStorage.hpp"
#include "TimeServiceManager.hpp"
#include "callback_registry.hpp"
#include "diagnostics.h"
#include "Metrics.hpp"

namespace bftEngine::impl {
class BftExecutionEngineBase {
 public:
  BftExecutionEngineBase(std::shared_ptr<IRequestsHandler> requests_handler,
                         std::shared_ptr<ClientsManager>,
                         std::shared_ptr<ReplicasInfo> reps_info,
                         std::shared_ptr<PersistentStorage> ps,
                         std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager);
  virtual SeqNum addExecutions(const std::vector<PrePrepareMsg*>&) = 0;
  void addPostExecCallBack(std::function<void(PrePrepareMsg*, IRequestsHandler::ExecutionRequestsQueue&)> cb);
  virtual ~BftExecutionEngineBase() = default;
  virtual bool isExecuting() { return false; }
  void setRequestsMap(const Bitmap& requestsMap) { requestsMap_ = requestsMap; }
  virtual void onExecutionComplete(SeqNum) { return; }
  void loadTime(SeqNum);

 private:
  logging::Logger& getLogger() const {
    static logging::Logger logger = logging::getLogger("bftEngine.impl.BasicBftExecutionEngine");
    return logger;
  }

 protected:
  virtual Bitmap filterRequests(const PrePrepareMsg&);
  std::deque<IRequestsHandler::ExecutionRequest> collectRequests(const PrePrepareMsg&);
  virtual void execute(std::deque<IRequestsHandler::ExecutionRequest>&);
  std::shared_ptr<IRequestsHandler> requests_handler_;
  bftEngine::ReplicaConfig& config_;
  std::shared_ptr<ClientsManager> clients_manager_;
  std::shared_ptr<ReplicasInfo> reps_info_;
  std::shared_ptr<PersistentStorage> ps_;
  std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager_;
  concord::util::CallbackRegistry<PrePrepareMsg*, IRequestsHandler::ExecutionRequestsQueue&> post_exec_handlers_;
  Bitmap requestsMap_;
  std::map<SeqNum, Timestamp> timestamps;

 public:
  // Metrics
  struct ExecutionMetrics {
    std::shared_ptr<concord::diagnostics::Recorder> numRequestsInPrePrepareMsg;
    std::shared_ptr<concord::diagnostics::Recorder> executeRequestsInPrePrepareMsg;
    std::shared_ptr<concord::diagnostics::Recorder> executeRequestsAndSendResponses;
    std::shared_ptr<concord::diagnostics::Recorder> executeWriteRequest;
  };
  void setMetrics(const ExecutionMetrics& execution_metrics) { metrics_ = execution_metrics; }

  ExecutionMetrics metrics_;
};

class BftExecutionEngineFactory {
 public:
  enum TYPE { SYNC, ASYNC, SKIP, CONFIG };
  static std::unique_ptr<BftExecutionEngineBase> create(
      std::shared_ptr<IRequestsHandler> requests_handler,
      std::shared_ptr<ClientsManager> client_manager,
      std::shared_ptr<ReplicasInfo> reps_info,
      std::shared_ptr<PersistentStorage> ps,
      std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager,
      TYPE type);
};

}  // namespace bftEngine::impl