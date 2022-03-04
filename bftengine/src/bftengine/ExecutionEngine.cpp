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
#include "ExecutionEngine.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "serialize.hpp"
#include "assertUtils.hpp"
#include "Bitmap.hpp"
#include "ClientsManager.hpp"
#include "ReplicasInfo.hpp"
#include "PersistentStorage.hpp"

namespace bftEngine::impl {

typedef std::iterator<std::input_iterator_tag,
                      PrePrepareMsg*,
                      uint32_t,
                      const IRequestsHandler::ExecutionRequestsQueue*,
                      IRequestsHandler::ExecutionRequestsQueue>
    ExecutionIterator;
class RequestIterator : public ExecutionIterator {
 public:
  explicit RequestIterator(std::deque<IRequestsHandler::ExecutionRequest>& requests) : requests_{requests} {};
  virtual RequestIterator& operator++() {
    position++;
    return *this;
  }
  bool operator==(const RequestIterator& other) const { return position == other.position; }
  bool operator!=(const RequestIterator& other) const { return position != other.position; }
  virtual reference operator*() const {
    std::deque<IRequestsHandler::ExecutionRequest> single_req_queue;
    single_req_queue.push_back(requests_[position]);
    return single_req_queue;
  }
  virtual ~RequestIterator() = default;

 protected:
  std::deque<IRequestsHandler::ExecutionRequest>& requests_;
  uint32_t position = 0;
};

class BlockAccumulationIterator : public RequestIterator {
 public:
  explicit BlockAccumulationIterator(std::deque<IRequestsHandler::ExecutionRequest>& requests)
      : RequestIterator{requests} {};
  BlockAccumulationIterator& operator++() override {
    if (position < requests_.size()) position = requests_.size();
    return *this;
  }
  reference operator*() const override { return requests_; }
};

class RequestsSelector {
 public:
  RequestsSelector(
      const std::deque<IRequestsHandler::ExecutionRequest>& requests,
      const std::function<RequestIterator*(std::deque<IRequestsHandler::ExecutionRequest>&)>& iterator_factory)
      : requests_{requests}, iterator_factory_{iterator_factory} {
    begin_.reset(iterator_factory_(requests_));
    end_.reset(iterator_factory_(requests_));
    for (auto i = 0u; i < requests_.size(); i++) ++(*end_);
  };

  RequestIterator& begin() { return *begin_; }
  RequestIterator& end() { return *end_; }

 private:
  std::deque<IRequestsHandler::ExecutionRequest> requests_;
  std::function<RequestIterator*(std::deque<IRequestsHandler::ExecutionRequest>&)> iterator_factory_;
  std::unique_ptr<RequestIterator> begin_;
  std::unique_ptr<RequestIterator> end_;
};

ExecutionEngine::ExecutionEngine(std::shared_ptr<IRequestsHandler> requests_handler,
                                 std::shared_ptr<ClientsManager> client_manager,
                                 std::shared_ptr<ReplicasInfo> reps_info,
                                 std::shared_ptr<PersistentStorage> ps,
                                 std::shared_ptr<TimeServiceManager<CLOCK_TYPE>> time_service_manager,
                                 bool blockAccumulation,
                                 bool async)
    : requests_handler_{requests_handler},
      config_{bftEngine::ReplicaConfig::instance()},
      clients_manager_{client_manager},
      reps_info_{reps_info},
      ps_{ps},
      time_service_manager_{time_service_manager},
      block_accumulation_{blockAccumulation} {
  if (async) thread_pool_ = std::make_shared<concord::util::ThreadPool>(1);
}
Bitmap ExecutionEngine::filterRequests(const PrePrepareMsg& ppMsg) {
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
  auto sn = ppMsg.seqNumber();
  if (ps_) {
    ps_->beginWriteTran();
    ps_->setRequestsMapInSeqNumWindow(sn, requestSet);
    ps_->setIsExecutedInSeqNumWindow(sn, true);
    ps_->endWriteTran(config_.getsyncOnUpdateOfMetadata());
  }
  return requestSet;
}
std::deque<IRequestsHandler::ExecutionRequest> ExecutionEngine::collectRequests(const PrePrepareMsg& ppMsg) {
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
    auto sn = ppMsg.seqNumber();
    if (config_.timeServiceEnabled) {
      if (req.flags() & MsgFlag::TIME_SERVICE_FLAG) {
        timestamps.emplace(sn, Timestamp());
        timestamps[sn].time_since_epoch =
            concord::util::deserialize<ConsensusTime>(req.requestBuf(), req.requestBuf() + req.requestLength());
        continue;
      }
    }
    SCOPED_MDC_CID(req.getCid());
    NodeIdType clientId = req.clientProxyId();
    IRequestsHandler::ExecutionRequest execution_request{
        clientId,
        static_cast<uint64_t>(sn),
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
void ExecutionEngine::addPostExecCallBack(
    std::function<void(PrePrepareMsg*, IRequestsHandler::ExecutionRequestsQueue&)> cb) {
  post_exec_handlers_.add(std::move(cb));
}
void ExecutionEngine::execute(std::deque<IRequestsHandler::ExecutionRequest>& accumulatedRequests,
                              Timestamp& timestamp) {
  const std::string cid = accumulatedRequests.back().cid;
  const auto sn = accumulatedRequests.front().executionSequenceNum;
  concordUtils::SpanWrapper span_wrapper{};
  LOG_INFO(getLogger(), "Executing all the requests of preprepare message: " << KVLOG(cid, sn));
  RequestsSelector execution_selector(
      accumulatedRequests, [&](std::deque<IRequestsHandler::ExecutionRequest>& requests) -> RequestIterator* {
        if (block_accumulation_) return new BlockAccumulationIterator(requests);
        return new RequestIterator(requests);
      });
  accumulatedRequests.clear();
  for (RequestIterator iter = execution_selector.begin(); iter != execution_selector.end(); ++iter) {
    concord::diagnostics::TimeRecorder scoped_timer1(*metrics_.executeWriteRequest);
    auto data = *iter;
    requests_handler_->execute(data, timestamp, cid, span_wrapper);
    if (data.size() == 1) timestamp.request_position++;
    accumulatedRequests.insert(accumulatedRequests.end(), data.begin(), data.end());
  }
}
void ExecutionEngine::loadTime(SeqNum sn) {
  if (config_.timeServiceEnabled) {
    timestamps[sn].time_since_epoch = time_service_manager_->compareAndUpdate(timestamps[sn].time_since_epoch);
    LOG_INFO(getLogger(),
             "Timestamp to be provided to the execution: " << timestamps[sn].time_since_epoch.count() << "ms");
  }
}
using namespace concord::diagnostics;
SeqNum ExecutionEngine::addExecutions(const vector<PrePrepareMsg*>& ppMsgs) {
  SeqNum sn = 0;
  for (auto* ppMsg : ppMsgs) {
    sn = ppMsg->seqNumber();
    if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return sn;
    TimeRecorder scoped_timer(*metrics_.executeRequestsInPrePrepareMsg);
    auto requests_for_execution = collectRequests(*ppMsg);
    TimeRecorder scoped_timer1(*metrics_.executeRequestsAndSendResponses);
    loadTime(ppMsg->seqNumber());
    auto exec = [&](PrePrepareMsg* ppMsg_,
                    IRequestsHandler::ExecutionRequestsQueue requests_for_execution_,
                    Timestamp timestamp) {
      if (!requests_for_execution_.empty()) execute(requests_for_execution_, timestamp);
      post_exec_handlers_.invokeAll(ppMsg_, requests_for_execution_);
    };
    in_execution++;
    if (thread_pool_) {
      thread_pool_->async(exec, ppMsg, requests_for_execution, timestamps[ppMsg->seqNumber()]);
    } else {
      exec(ppMsg, requests_for_execution, timestamps[ppMsg->seqNumber()]);
      onExecutionComplete(ppMsg->seqNumber());
    }
  }
  return sn;
}

SeqNum SkipAndSendExecutionEngine::addExecutions(const std::vector<PrePrepareMsg*>& ppMsgs) {
  for (auto* ppMsg : ppMsgs) {
    auto requests_for_execution = collectRequests(*ppMsg);
    if (requests_for_execution.empty()) continue;
    post_exec_handlers_.invokeAll(ppMsg, requests_for_execution);
  }
  return 0;
}

Bitmap SkipAndSendExecutionEngine::filterRequests(const PrePrepareMsg& ppMsg) {
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
}  // namespace bftEngine::impl