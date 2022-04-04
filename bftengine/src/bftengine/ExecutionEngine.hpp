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
#include "bftengine/ReplicaConfig.hpp"
#include "callback_registry.hpp"
#include "diagnostics.h"
#include "Metrics.hpp"
#include "thread_pool.hpp"
#include "TimeServiceManager.hpp"
#include "PerfMetrics.hpp"

#include <iterator>

#ifdef USE_FAKE_CLOCK_IN_TS
#include "FakeClock.hpp"
#define CLOCK_TYPE concord::util::FakeClock
#else
#define CLOCK_TYPE std::chrono::system_clock
#endif
namespace bftEngine::impl {
class Bitmap;
class ClientsManager;
class ReplicasInfo;
class PersistentStorage;

class ExecutionEngine {
 public:
  ExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                  std::shared_ptr<ClientsManager>,
                  std::shared_ptr<ReplicasInfo> reps_info,
                  std::shared_ptr<PersistentStorage> ps,
                  std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager,
                  bool blockAccumulation,
                  bool async);

  virtual SeqNum addExecutions(const std::vector<PrePrepareMsg*>&);
  void addPostExecCallBack(std::function<void(PrePrepareMsg*, IRequestsHandler::ExecutionRequestsQueue&)> cb);
  virtual ~ExecutionEngine() = default;
  bool isExecuting() { return in_execution > 0; }
  void setRequestsMap(const Bitmap& requestsMap) { requestsMap_ = requestsMap; }
  void onExecutionComplete(SeqNum sn) {
    in_execution--;
    timestamps.erase(sn);
  }
  void loadTime(SeqNum);

 private:
  logging::Logger& getLogger() const {
    static logging::Logger logger = logging::getLogger("bftEngine.impl.BasicExecutionEngine");
    return logger;
  }

 protected:
  virtual Bitmap filterRequests(const PrePrepareMsg&);
  std::deque<IRequestsHandler::ExecutionRequest> collectRequests(const PrePrepareMsg&);
  virtual void execute(std::deque<IRequestsHandler::ExecutionRequest>&, Timestamp&);
  std::shared_ptr<IRequestsHandler> requests_handler_;
  bftEngine::ReplicaConfig& config_;
  std::shared_ptr<ClientsManager> clients_manager_;
  std::shared_ptr<ReplicasInfo> reps_info_;
  std::shared_ptr<PersistentStorage> ps_;
  std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager_;
  concord::util::CallbackRegistry<PrePrepareMsg*, IRequestsHandler::ExecutionRequestsQueue&> post_exec_handlers_;
  Bitmap requestsMap_;
  std::map<SeqNum, Timestamp> timestamps;
  std::shared_ptr<concord::util::ThreadPool> thread_pool_;
  std::atomic_uint64_t in_execution{0};
  bool block_accumulation_;

 public:
  // Metrics
  struct ExecutionMetrics {
    std::shared_ptr<concord::diagnostics::Recorder> numRequestsInPrePrepareMsg;
    std::shared_ptr<concord::diagnostics::Recorder> executeRequestsInPrePrepareMsg;
    std::shared_ptr<concord::diagnostics::Recorder> executeRequestsAndSendResponses;
    std::shared_ptr<concord::diagnostics::Recorder> executeWriteRequest;
    std::shared_ptr<PerfMetric<uint64_t>> metric_consensus_end_to_core_exe_duration_;
    std::shared_ptr<PerfMetric<uint64_t>> metric_core_exe_func_duration_;
  };
  void setMetrics(const ExecutionMetrics& execution_metrics) { metrics_ = execution_metrics; }

 protected:
  ExecutionMetrics metrics_;
};

class SkipAndSendExecutionEngine : public ExecutionEngine {
 public:
  SkipAndSendExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                             std::shared_ptr<ClientsManager> client_manager,
                             std::shared_ptr<ReplicasInfo> reps_info,
                             std::shared_ptr<PersistentStorage> ps,
                             std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager)
      : ExecutionEngine{requests_handler, client_manager, reps_info, ps, time_service_manager, false, false} {}
  SeqNum addExecutions(const std::vector<PrePrepareMsg*>& ppMsgs) override;

 private:
  Bitmap filterRequests(const PrePrepareMsg& ppMsg) override;
};

}  // namespace bftEngine::impl