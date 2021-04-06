// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms.
// Your use of these subcomponents is subject to the terms and conditions of the sub-component's license,
// as noted in the LICENSE file.

#include "RequestsBatchingLogic.hpp"

namespace bftEngine::batchingLogic {

using namespace concordUtil;
using namespace std;
using namespace std::chrono;

RequestsBatchingLogic::RequestsBatchingLogic(InternalReplicaApi &replica,
                                             const ReplicaConfig &config,
                                             concordMetrics::Component &metrics,
                                             concordUtil::Timers &timers)
    : replica_(replica),
      metric_not_enough_client_requests_event_{metrics.RegisterCounter("notEnoughClientRequestsEvent")},
      batchingPolicy_((BatchingPolicy)config.batchingPolicy),
      batchingFactorCoefficient_(config.batchingFactorCoefficient),
      maxInitialBatchSize_(config.maxInitialBatchSize),
      batchFlushPeriodMs_(config.batchFlushPeriod),
      maxNumOfRequestsInBatch_(config.maxNumOfRequestsInBatch),
      initialMaxNumOfRequestsInBatch_(config.maxNumOfRequestsInBatch),
      maxBatchSizeInBytes_(config.maxBatchSizeInBytes),
      adaptiveConsensusSize_(config.adaptiveConsensus),
      timers_(timers) {
  if (batchingPolicy_ == BATCH_BY_REQ_SIZE || batchingPolicy_ == BATCH_BY_REQ_NUM)
    batchFlushTimer_ = timers_.add(milliseconds(batchFlushPeriodMs_),
                                   Timers::Timer::RECURRING,
                                   [this](Timers::Handle h) { onBatchFlushTimer(h); });
}

RequestsBatchingLogic::~RequestsBatchingLogic() {
  if (batchingPolicy_ == BATCH_BY_REQ_SIZE || batchingPolicy_ == BATCH_BY_REQ_NUM) timers_.cancel(batchFlushTimer_);
}

void RequestsBatchingLogic::onBatchFlushTimer(Timers::Handle) {
  if (replica_.isCurrentPrimary()) {
    lock_guard<mutex> lock(batchProcessingLock_);
    if (replica_.tryToSendPrePrepareMsg(false)) {
      LOG_INFO(GL, "Batching flush period expired" << KVLOG(batchFlushPeriodMs_));
      closed_on_flush += 1;
      timers_.reset(batchFlushTimer_, milliseconds(batchFlushPeriodMs_));
    }
  }
}

PrePrepareMsg *RequestsBatchingLogic::batchRequestsSelfAdjustedPolicy(SeqNum primaryLastUsedSeqNum,
                                                                      uint64_t requestsInQueue,
                                                                      SeqNum lastExecutedSeqNum) {
  if (requestsInQueue > maxNumberOfPendingRequestsInRecentHistory_)
    maxNumberOfPendingRequestsInRecentHistory_ = requestsInQueue;

  uint64_t minBatchSize = 1;
  uint64_t concurrentDiff = primaryLastUsedSeqNum + 1 - lastExecutedSeqNum;

  if (concurrentDiff >= 2) {
    minBatchSize = concurrentDiff * batchingFactor_;
    if (minBatchSize > maxInitialBatchSize_) minBatchSize = maxInitialBatchSize_;
  }

  if (requestsInQueue < minBatchSize) {
    LOG_INFO(GL, "Not enough client requests in the queue to fill the batch" << KVLOG(minBatchSize, requestsInQueue));
    metric_not_enough_client_requests_event_.Get().Inc();
    return nullptr;
  }

  // Update batching factor
  if (((primaryLastUsedSeqNum + 1) % kWorkWindowSize) == 0) {
    batchingFactor_ = maxNumberOfPendingRequestsInRecentHistory_ / batchingFactorCoefficient_;
    if (batchingFactor_ < 1) batchingFactor_ = 1;
    maxNumberOfPendingRequestsInRecentHistory_ = 0;
    LOG_DEBUG(GL, "PrePrepare batching factor updated" << KVLOG(batchingFactor_));
  }

  return replica_.buildPrePrepareMessage();
}

void RequestsBatchingLogic::adjustPreprepareSize() {
  auto totalConsensusesCount = closed_on_logic + closed_on_flush;
  start_timer_ = std::chrono::steady_clock::now();
  if (closed_on_flush / totalConsensusesCount > 0.5 && maxNumOfRequestsInBatch_ > initialMaxNumOfRequestsInBatch_)
    maxNumOfRequestsInBatch_ *= 0.8;
  else if (closed_on_flush / totalConsensusesCount > 0.25 &&
           maxNumOfRequestsInBatch_ > initialMaxNumOfRequestsInBatch_) {
    maxNumOfRequestsInBatch_ *= 0.9;
  } else if (maxNumOfRequestsInBatch_ <= 3 * initialMaxNumOfRequestsInBatch_) {
    if (closed_on_logic / totalConsensusesCount >= 0.95) {
      maxNumOfRequestsInBatch_ += maxNumOfRequestsInBatch_ * 0.15;
    } else if (closed_on_logic / totalConsensusesCount >= 0.9) {
      maxNumOfRequestsInBatch_ += maxNumOfRequestsInBatch_ * 0.1;
    } else if (closed_on_logic / totalConsensusesCount >= 0.75) {
      maxNumOfRequestsInBatch_ += maxNumOfRequestsInBatch_ * 0.05;
    }
  }
  closed_on_flush = 0;
  closed_on_logic = 0;
  LOG_INFO(GL, "increasing maxBatchSize to:" << maxNumOfRequestsInBatch_);
}

PrePrepareMsg *RequestsBatchingLogic::batchRequests() {
  const auto requestsInQueue = replica_.getRequestsInQueue();
  if (requestsInQueue == 0) return nullptr;

  PrePrepareMsg *prePrepareMsg = nullptr;
  switch (batchingPolicy_) {
    case BATCH_SELF_ADJUSTED:
      prePrepareMsg = batchRequestsSelfAdjustedPolicy(
          replica_.getPrimaryLastUsedSeqNum(), requestsInQueue, replica_.getLastExecutedSeqNum());
      break;
    case BATCH_BY_REQ_NUM: {
      lock_guard<mutex> lock(batchProcessingLock_);
      prePrepareMsg = replica_.buildPrePrepareMsgBatchByRequestsNum(maxNumOfRequestsInBatch_);
      if (prePrepareMsg) {
        timers_.reset(batchFlushTimer_, milliseconds(batchFlushPeriodMs_));
        auto period =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_timer_)
                .count();
        closed_on_logic += 1;
        if (adaptiveConsensusSize_ && period > batchFlushPeriodMs_ * 20) adjustPreprepareSize();
      }
    } break;
    case BATCH_BY_REQ_SIZE: {
      lock_guard<mutex> lock(batchProcessingLock_);
      prePrepareMsg = replica_.buildPrePrepareMsgBatchByOverallSize(maxBatchSizeInBytes_);
      if (prePrepareMsg) timers_.reset(batchFlushTimer_, milliseconds(batchFlushPeriodMs_));
    } break;
  }
  return prePrepareMsg;
}

}  // namespace bftEngine::batchingLogic
