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
#include "BftExecutionEngineBase.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "serialize.hpp"
#include "assertUtils.hpp"
namespace bftEngine::impl {

BftExecutionEngineBase::BftExecutionEngineBase(
    std::shared_ptr<IRequestsHandler> requests_handler,
    std::shared_ptr<ClientsManager> client_manager,
    std::shared_ptr<ReplicasInfo> reps_info,
    std::shared_ptr<PersistentStorage> ps,
    std::shared_ptr<TimeServiceManager<std::chrono::system_clock>> time_service_manager)
    : requests_handler_{requests_handler},
      config_{bftEngine::ReplicaConfig::instance()},
      clients_manager_{client_manager},
      reps_info_{reps_info},
      ps_{ps},
      time_service_manager_{time_service_manager} {}
Bitmap BftExecutionEngineBase::filterRequests(const PrePrepareMsg &ppMsg) {
  if (!requestsMap_.isEmpty()) return requestsMap_;
  const uint16_t numOfRequests = ppMsg.numberOfRequests();
  Bitmap requestSet(numOfRequests);
  size_t reqIdx = 0;
  RequestsIterator reqIter(&ppMsg);
  char *requestBody = nullptr;

  bool seenTimeService = false;
  while (reqIter.getAndGoToNext(requestBody)) {
    ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader *>(requestBody));
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
std::deque<IRequestsHandler::ExecutionRequest> BftExecutionEngineBase::collectRequests(const PrePrepareMsg &ppMsg) {
  metrics_.numRequestsInPrePrepareMsg->record(ppMsg.numberOfRequests());
  auto requestSet = filterRequests(ppMsg);
  if (!requestsMap_.isEmpty()) requestSet += requestsMap_;
  IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  RequestsIterator reqIter(&ppMsg);
  size_t reqIdx = 0;
  char *requestBody = nullptr;
  while (reqIter.getAndGoToNext(requestBody)) {
    size_t tmp = reqIdx;
    reqIdx++;
    ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader *>(requestBody));
    if (config_.timeServiceEnabled) {
      if (req.flags() & MsgFlag::TIME_SERVICE_FLAG) {
        timestamps.emplace(ppMsg.seqNumber(), Timestamp());
        timestamps[ppMsg.seqNumber()].time_since_epoch =
            concord::util::deserialize<ConsensusTime>(req.requestBuf(), req.requestBuf() + req.requestLength());
        continue;
      }
    }
    if (!requestSet.get(tmp) || req.requestLength() == 0) {
      continue;
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
        (char *)std::malloc(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)),
        req.requestSeqNum(),
        req.result()};
    // Decode the pre-execution block-id for the conflict detection optimization,
    // and pass it to the post-execution.
    if (req.flags() & HAS_PRE_PROCESSED_FLAG) {
      ConcordAssertGT(req.requestLength(), sizeof(uint64_t));
      auto requestSize = req.requestLength() - sizeof(uint64_t);
      auto opt_block_id = *(reinterpret_cast<uint64_t *>(req.requestBuf() + requestSize));
      LOG_DEBUG(GL, "Conflict detection optimization block id is " << opt_block_id);
      execution_request.blockId = opt_block_id;
      execution_request.requestSize = requestSize;
    }
    accumulatedRequests.push_back(execution_request);
  }
  return accumulatedRequests;
}
void BftExecutionEngineBase::addPostExecCallBack(
    std::function<void(PrePrepareMsg *, IRequestsHandler::ExecutionRequestsQueue &)> cb) {
  post_exec_handlers_.add(cb);
}
void BftExecutionEngineBase::execute(std::deque<IRequestsHandler::ExecutionRequest> &accumulatedRequests) {
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;
  const std::string cid = accumulatedRequests.back().cid;
  const auto sn = accumulatedRequests.front().executionSequenceNum;
  concordUtils::SpanWrapper span_wrapper{};
  LOG_INFO(getLogger(), "Executing all the requests of preprepare message: " << KVLOG(cid, sn));
  concord::diagnostics::TimeRecorder scoped_timer1(*metrics_.executeWriteRequest);
  requests_handler_->execute(accumulatedRequests, timestamps[sn], cid, span_wrapper);
  if (accumulatedRequests.size() == 1) timestamps[sn].request_position++;
}
void BftExecutionEngineBase::loadTime(SeqNum sn) {
  if (config_.timeServiceEnabled) {
    timestamps[sn].time_since_epoch = time_service_manager_->compareAndUpdate(timestamps[sn].time_since_epoch);
    LOG_INFO(getLogger(),
             "Timestamp to be provided to the execution: " << timestamps[sn].time_since_epoch.count() << "ms");
  }
}
}  // namespace bftEngine::impl