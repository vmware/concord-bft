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

#include "BftExecutionEngineFactory.hpp"
#include "performance_handler.h"
#include "ControlStateManager.hpp"

namespace bftEngine::impl {
using namespace concord::diagnostics;

class BlockAccumulationExecutionEngine : public BftExecutionEngineBase {
 public:
  BlockAccumulationExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                                   std::shared_ptr<ClientsManager> client_manager,
                                   std::shared_ptr<ReplicasInfo> reps_info,
                                   std::shared_ptr<PersistentStorage> ps,
                                   std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager)
      : BftExecutionEngineBase{requests_handler, client_manager, reps_info, ps, time_service_manager} {}
  SeqNum addExecutions(const std::vector<PrePrepareMsg*>& ppMsgs) override {
    SeqNum sn = 0;
    for (auto ppMsg : ppMsgs) {
      sn = ppMsg->seqNumber();
      if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return sn;
      TimeRecorder scoped_timer(*metrics_.executeRequestsInPrePrepareMsg);
      auto requests_for_execution = collectRequests(*ppMsg);
      TimeRecorder scoped_timer1(*metrics_.executeRequestsAndSendResponses);
      loadTime(ppMsg->seqNumber());
      if (!requests_for_execution.empty()) execute(requests_for_execution);
      timestamps.erase(ppMsg->seqNumber());
      post_exec_handlers_.invokeAll(ppMsg, requests_for_execution);
    }
    return sn;
  }

 private:
  logging::Logger& getLogger() const {
    static logging::Logger logger = logging::getLogger("bftEngine.impl.BlockAccumulationExecutionEngine");
    return logger;
  }
};

class SingleRequestExecutionEngine : public BftExecutionEngineBase {
 public:
  SingleRequestExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                               std::shared_ptr<ClientsManager> client_manager,
                               std::shared_ptr<ReplicasInfo> reps_info,
                               std::shared_ptr<PersistentStorage> ps,
                               std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager)
      : BftExecutionEngineBase{requests_handler, client_manager, reps_info, ps, time_service_manager} {}
  SeqNum addExecutions(const std::vector<PrePrepareMsg*>& ppMsgs) override {
    SeqNum sn = 0;
    for (auto ppMsg : ppMsgs) {
      sn = ppMsg->seqNumber();
      if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return sn;
      TimeRecorder scoped_timer(*metrics_.executeRequestsInPrePrepareMsg);
      auto requests_for_execution = collectRequests(*ppMsg);
      std::deque<IRequestsHandler::ExecutionRequest> single_req_queue;
      TimeRecorder scoped_timer1(*metrics_.executeRequestsAndSendResponses);
      loadTime(ppMsg->seqNumber());
      for (auto& req : requests_for_execution) {
        TimeRecorder scoped_timer2(*metrics_.executeWriteRequest);
        single_req_queue.push_back(req);
        execute(single_req_queue);
        req = single_req_queue.back();
        single_req_queue.clear();
      }
      timestamps.erase(ppMsg->seqNumber());
      post_exec_handlers_.invokeAll(ppMsg, requests_for_execution);
    }
    return sn;
  }

 private:
  logging::Logger& getLogger() const {
    static logging::Logger logger = logging::getLogger("bftEngine.impl.SingleRequestExecutionEngine");
    return logger;
  }
};

class AsyncExecutionEngine : public BftExecutionEngineBase {
 public:
  AsyncExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                       std::shared_ptr<ClientsManager> client_manager,
                       std::shared_ptr<ReplicasInfo> reps_info,
                       std::shared_ptr<PersistentStorage> ps,
                       std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager)
      : BftExecutionEngineBase{requests_handler, client_manager, reps_info, ps, time_service_manager} {
    active_ = true;
    worker_ = std::thread([&]() { thread_function(); });
  }
  SeqNum addExecutions(const std::vector<PrePrepareMsg*>& ppMsgs) override {
    SeqNum sn = 0;
    for (auto pp : ppMsgs) {
      sn = pp->seqNumber();
      if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return sn;
      requests_++;
      auto requests_for_execution =
          std::make_shared<std::deque<IRequestsHandler::ExecutionRequest>>(collectRequests(*pp));
      std::unique_lock<std::mutex> lk(lock_);
      data_.emplace_back(pp, requests_for_execution);
      var_.notify_one();
    }
    return sn;
  }
  virtual ~AsyncExecutionEngine() {
    active_ = false;
    var_.notify_one();
    worker_.join();
  }
  bool isExecuting() override { return requests_ > 0; }
  void onExecutionComplete(SeqNum) override { requests_--; }

 private:
  logging::Logger& getLogger() const {
    static logging::Logger logger = logging::getLogger("bftEngine.impl.AsyncExecutionEngine");
    return logger;
  }
  virtual void runExecutions(std::deque<IRequestsHandler::ExecutionRequest>& requests_for_execution) = 0;
  void thread_function() {
    while (active_) {
      std::pair<PrePrepareMsg*, std::shared_ptr<std::deque<IRequestsHandler::ExecutionRequest>>> candidate;
      {
        {
          std::unique_lock<std::mutex> lk(lock_);
          var_.wait(lk, [this]() {
            return !data_.empty() && !bftEngine::ControlStateManager::instance().getPruningProcessStatus();
          });
          if (!active_) return;
          if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) continue;
          candidate = data_.front();
          data_.pop_front();
        }
        auto& requests_for_execution = *(candidate.second);
        TimeRecorder scoped_timer(*metrics_.executeRequestsInPrePrepareMsg);
        loadTime(candidate.first->seqNumber());
        if (!requests_for_execution.empty()) {
          runExecutions(requests_for_execution);
        }
        timestamps.erase(candidate.first->seqNumber());
        post_exec_handlers_.invokeAll(candidate.first, requests_for_execution);
      }
    }
  }
  std::mutex lock_;
  std::condition_variable var_;
  std::thread worker_;
  std::deque<std::pair<PrePrepareMsg*, std::shared_ptr<std::deque<IRequestsHandler::ExecutionRequest>>>> data_;
  std::atomic_bool active_;
  std::atomic_uint32_t requests_{0};
};

class SingleRequestAsyncExecutionEngine : public AsyncExecutionEngine {
 public:
  SingleRequestAsyncExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                                    std::shared_ptr<ClientsManager> client_manager,
                                    std::shared_ptr<ReplicasInfo> reps_info,
                                    std::shared_ptr<PersistentStorage> ps,
                                    std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager)
      : AsyncExecutionEngine{requests_handler, client_manager, reps_info, ps, time_service_manager} {}

 private:
  void runExecutions(std::deque<IRequestsHandler::ExecutionRequest>& requests_for_execution) override {
    std::deque<IRequestsHandler::ExecutionRequest> single_req_queue;
    for (auto& req : requests_for_execution) {
      single_req_queue.push_back(req);
      execute(single_req_queue);
      req = single_req_queue.back();
      single_req_queue.clear();
    }
  }
};

class AccumulatedAsyncExecutionEngine : public AsyncExecutionEngine {
 public:
  AccumulatedAsyncExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                                  std::shared_ptr<ClientsManager> client_manager,
                                  std::shared_ptr<ReplicasInfo> reps_info,
                                  std::shared_ptr<PersistentStorage> ps,
                                  std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager)
      : AsyncExecutionEngine{requests_handler, client_manager, reps_info, ps, time_service_manager} {}

 private:
  void runExecutions(std::deque<IRequestsHandler::ExecutionRequest>& requests_for_execution) override {
    execute(requests_for_execution);
  }
};
class SkipAndSendExecutionEngine : public BftExecutionEngineBase {
 public:
  SkipAndSendExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                             std::shared_ptr<ClientsManager> client_manager,
                             std::shared_ptr<ReplicasInfo> reps_info,
                             std::shared_ptr<PersistentStorage> ps,
                             std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager)
      : BftExecutionEngineBase{requests_handler, client_manager, reps_info, ps, time_service_manager} {}
  SeqNum addExecutions(const std::vector<PrePrepareMsg*>& ppMsgs) override {
    for (auto ppMsg : ppMsgs) {
      auto requests_for_execution = collectRequests(*ppMsg);
      if (requests_for_execution.empty()) continue;
      post_exec_handlers_.invokeAll(ppMsg, requests_for_execution);
    }
    return 0;
  }

 private:
  Bitmap filterRequests(const PrePrepareMsg& ppMsg) override {
    const uint16_t numOfRequests = ppMsg.numberOfRequests();
    Bitmap requestSet(numOfRequests);
    size_t reqIdx = 0;
    RequestsIterator reqIter(&ppMsg);
    char* requestBody = nullptr;
    while (reqIter.getAndGoToNext(requestBody)) {
      ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader*>(requestBody));
      SCOPED_MDC_CID(req.getCid());
      NodeIdType clientId = req.clientProxyId();
      if (clients_manager_->hasReply(clientId, req.requestSeqNum())) {
        requestSet.set(reqIdx++);
      } else {
        reqIdx++;
      }
    }
    return requestSet;
  }
};

std::unique_ptr<BftExecutionEngineBase> BftExecutionEngineFactory::create(
    std::shared_ptr<IRequestsHandler> requests_handler,
    std::shared_ptr<ClientsManager> client_manager,
    std::shared_ptr<ReplicasInfo> reps_info,
    std::shared_ptr<PersistentStorage> ps,
    std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager,
    TYPE type) {
  bool accumulated = bftEngine::ReplicaConfig::instance().blockAccumulation;
  TYPE type_ = type;
  if (type == CONFIG) type_ = bftEngine::ReplicaConfig::instance().enablePostExecutionSeparation ? ASYNC : SYNC;
  switch (type_) {
    case SYNC:
      if (accumulated)
        return std::make_unique<BlockAccumulationExecutionEngine>(
            requests_handler, client_manager, reps_info, ps, time_service_manager);
      return std::make_unique<SingleRequestExecutionEngine>(
          requests_handler, client_manager, reps_info, ps, time_service_manager);
    case ASYNC:
      if (accumulated)
        return std::make_unique<AccumulatedAsyncExecutionEngine>(
            requests_handler, client_manager, reps_info, ps, time_service_manager);
      return std::make_unique<SingleRequestAsyncExecutionEngine>(
          requests_handler, client_manager, reps_info, ps, time_service_manager);
    case SKIP:
      return std::make_unique<SkipAndSendExecutionEngine>(
          requests_handler, client_manager, reps_info, ps, time_service_manager);
    case CONFIG:
      return nullptr;
  }
}
}  // namespace bftEngine::impl
