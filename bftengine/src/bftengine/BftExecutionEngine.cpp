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

#include "Replica.hpp"
#include "SharedTypes.hpp"
#include "ControlStateManager.hpp"
#include "performance_handler.h"
#include <utility>
#include "BftExecutionEngine.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "serialize.hpp"
#include "assertUtils.hpp"
namespace bftEngine::impl {

BftExecutionEngineBase::BftExecutionEngineBase(std::shared_ptr<IRequestsHandler> requests_handler,
                                               std::shared_ptr<ClientsManager> client_manager,
                                               std::shared_ptr<ReplicasInfo> reps_info,
                                               std::shared_ptr<PersistentStorage> ps,
                                               std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager)
    : requests_handler_{requests_handler},
      config_{bftEngine::ReplicaConfig::instance()},
      clients_manager_{client_manager},
      reps_info_{reps_info},
      ps_{ps},
      time_service_manager_{time_service_manager} {}
Bitmap BftExecutionEngineBase::filterRequests(const PrePrepareMsg& ppMsg) {
  if (!requestsMap_.isEmpty()) return requestsMap_;
  const uint16_t numOfRequests = ppMsg.numberOfRequests();
  Bitmap requestSet(numOfRequests);
  size_t reqIdx = 0;
  RequestsIterator reqIter(&ppMsg);
  char* requestBody = nullptr;

  bool seenTimeService = false;
  while (reqIter.getAndGoToNext(requestBody)) {
    ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader*>(requestBody));
    SCOPED_MDC_CID(req.getCid());
    NodeIdType clientId = req.clientProxyId();

    if (config_.timeServiceEnabled && req.flags() & MsgFlag::TIME_SERVICE_FLAG) {
      ConcordAssert(!seenTimeService && "Multiple Time Service messages in PrePrepare");
      seenTimeService = true;
      reqIdx++;
      continue;
    }
    const bool validClient = clients_manager_->isValidClient(clientId) ||
                             ((req.flags() & RECONFIG_FLAG) && reps_info_->isIdOfReplica(clientId));
    if (!validClient) {
      LOG_WARN(getLogger(), "The client is not valid" << KVLOG(clientId));
      reqIdx++;
      continue;
    }
    if (clients_manager_->hasReply(clientId, req.requestSeqNum())) {
      reqIdx++;
      continue;
    }
    requestSet.set(reqIdx++);
  }

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setRequestsMapInSeqNumWindow(ppMsg.seqNumber(), requestSet);
    ps_->setIsExecutedInSeqNumWindow(ppMsg.seqNumber(), true);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }
  return requestSet;
}
std::deque<IRequestsHandler::ExecutionRequest> BftExecutionEngineBase::collectRequests(const PrePrepareMsg& ppMsg) {
  metrics_.numRequestsInPrePrepareMsg->record(ppMsg.numberOfRequests());
  auto requestSet = filterRequests(ppMsg);
  if (!requestsMap_.isEmpty()) requestSet += requestsMap_;
  IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  RequestsIterator reqIter(&ppMsg);
  size_t reqIdx = 0;
  char* requestBody = nullptr;
  while (reqIter.getAndGoToNext(requestBody)) {
    size_t tmp = reqIdx;
    reqIdx++;
    ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader*>(requestBody));

    if (!requestSet.get(tmp) || req.requestLength() == 0) {
      continue;
    }
    if (config_.timeServiceEnabled) {
      if (req.flags() & MsgFlag::TIME_SERVICE_FLAG) {
        timestamps.emplace(ppMsg.seqNumber(), Timestamp());
        timestamps[ppMsg.seqNumber()].time_since_epoch =
            concord::util::deserialize<ConsensusTime>(req.requestBuf(), req.requestBuf() + req.requestLength());
        continue;
      }
    }
    SCOPED_MDC_CID(req.getCid());
    NodeIdType clientId = req.clientProxyId();
    IRequestsHandler::ExecutionRequest execution_request{
        clientId,
        static_cast<uint64_t>(ppMsg.seqNumber()),
        ppMsg.getCid(),
        req.flags(),
        req.requestLength(),
        req.requestBuf(),
        std::string(req.requestSignature(), req.requestSignatureLength()),
        static_cast<uint32_t>(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)),
        (char*)std::malloc(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)),
        req.requestSeqNum(),
        req.result()};
    // Decode the pre-execution block-id for the conflict detection optimization,
    // and pass it to the post-execution.
    if (req.flags() & HAS_PRE_PROCESSED_FLAG) {
      ConcordAssertGT(req.requestLength(), sizeof(uint64_t));
      auto requestSize = req.requestLength() - sizeof(uint64_t);
      auto opt_block_id = *(reinterpret_cast<uint64_t*>(req.requestBuf() + requestSize));
      LOG_DEBUG(GL, "Conflict detection optimization block id is " << opt_block_id);
      execution_request.blockId = opt_block_id;
      execution_request.requestSize = requestSize;
    }
    accumulatedRequests.push_back(execution_request);
  }
  return accumulatedRequests;
}
void BftExecutionEngineBase::addPostExecCallBack(
    std::function<void(PrePrepareMsg*, IRequestsHandler::ExecutionRequestsQueue&)> cb) {
  post_exec_handlers_.add(std::move(cb));
}
void BftExecutionEngineBase::execute(std::deque<IRequestsHandler::ExecutionRequest>& accumulatedRequests,
                                     Timestamp& timestamp) {
  const std::string cid = accumulatedRequests.back().cid;
  const auto sn = accumulatedRequests.front().executionSequenceNum;
  concordUtils::SpanWrapper span_wrapper{};
  LOG_INFO(getLogger(), "Executing all the requests of preprepare message: " << KVLOG(cid, sn));
  concord::diagnostics::TimeRecorder scoped_timer1(*metrics_.executeWriteRequest);
  requests_handler_->execute(accumulatedRequests, timestamp, cid, span_wrapper);
  if (accumulatedRequests.size() == 1) timestamp.request_position++;
}
void BftExecutionEngineBase::loadTime(SeqNum sn) {
  if (config_.timeServiceEnabled) {
    timestamps[sn].time_since_epoch = time_service_manager_->compareAndUpdate(timestamps[sn].time_since_epoch);
    LOG_INFO(getLogger(),
             "Timestamp to be provided to the execution: " << timestamps[sn].time_since_epoch.count() << "ms");
  }
}

using namespace concord::diagnostics;

class BlockAccumulationExecutionEngine : public BftExecutionEngineBase {
 public:
  BlockAccumulationExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                                   std::shared_ptr<ClientsManager> client_manager,
                                   std::shared_ptr<ReplicasInfo> reps_info,
                                   std::shared_ptr<PersistentStorage> ps,
                                   std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager)
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
      if (!requests_for_execution.empty()) execute(requests_for_execution, timestamps[ppMsg->seqNumber()]);
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
                               std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager)
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
        execute(single_req_queue, timestamps[ppMsg->seqNumber()]);
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
                       std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager)
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
      loadTime(pp->seqNumber());
      std::unique_lock<std::mutex> lk(lock_);
      data_.emplace_back(pp, requests_for_execution, timestamps[pp->seqNumber()]);
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
  void onExecutionComplete(SeqNum sn) override {
    requests_--;
    timestamps.erase(sn);
  }

 private:
  logging::Logger& getLogger() const {
    static logging::Logger logger = logging::getLogger("bftEngine.impl.AsyncExecutionEngine");
    return logger;
  }
  virtual void runExecutions(std::deque<IRequestsHandler::ExecutionRequest>& requests_for_execution, Timestamp&) = 0;
  void thread_function() {
    while (active_) {
      std::tuple<PrePrepareMsg*, std::shared_ptr<std::deque<IRequestsHandler::ExecutionRequest>>, Timestamp> candidate;
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
        auto& requests_for_execution = *(std::get<1>(candidate));
        TimeRecorder scoped_timer(*metrics_.executeRequestsInPrePrepareMsg);
        if (!requests_for_execution.empty()) {
          auto timestamp = std::get<2>(candidate);
          runExecutions(requests_for_execution, timestamp);
        }
        post_exec_handlers_.invokeAll(std::get<0>(candidate), requests_for_execution);
      }
    }
  }
  std::mutex lock_;
  std::condition_variable var_;
  std::thread worker_;
  std::deque<std::tuple<PrePrepareMsg*, std::shared_ptr<std::deque<IRequestsHandler::ExecutionRequest>>, Timestamp>>
      data_;
  std::atomic_bool active_;
  std::atomic_uint32_t requests_{0};
};

class SingleRequestAsyncExecutionEngine : public AsyncExecutionEngine {
 public:
  SingleRequestAsyncExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                                    std::shared_ptr<ClientsManager> client_manager,
                                    std::shared_ptr<ReplicasInfo> reps_info,
                                    std::shared_ptr<PersistentStorage> ps,
                                    std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager)
      : AsyncExecutionEngine{requests_handler, client_manager, reps_info, ps, time_service_manager} {}

 private:
  void runExecutions(std::deque<IRequestsHandler::ExecutionRequest>& requests_for_execution,
                     Timestamp& timestamp) override {
    std::deque<IRequestsHandler::ExecutionRequest> single_req_queue;
    for (auto& req : requests_for_execution) {
      single_req_queue.push_back(req);
      execute(single_req_queue, timestamp);
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
                                  std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager)
      : AsyncExecutionEngine{requests_handler, client_manager, reps_info, ps, time_service_manager} {}

 private:
  void runExecutions(std::deque<IRequestsHandler::ExecutionRequest>& requests_for_execution,
                     Timestamp& timestamp) override {
    execute(requests_for_execution, timestamp);
  }
};
class SkipAndSendExecutionEngine : public BftExecutionEngineBase {
 public:
  SkipAndSendExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                             std::shared_ptr<ClientsManager> client_manager,
                             std::shared_ptr<ReplicasInfo> reps_info,
                             std::shared_ptr<PersistentStorage> ps,
                             std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager)
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
    std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager,
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
  return nullptr;
}

}  // namespace bftEngine::impl