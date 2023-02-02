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
      increaseRate_(stod(config.adaptiveBatchingIncFactor)),
      decreaseCondition_(stod(config.adaptiveBatchingDecCond)),
      maxIncreaseCondition_(stod(config.adaptiveBatchingMaxIncCond)),
      midIncreaseCondition_(stod(config.adaptiveBatchingMidIncCond)),
      minIncreaseCondition_(stod(config.adaptiveBatchingMinIncCond)),
      initialBatchSize_(config.maxNumOfRequestsInBatch),
      maxBatchSizeInBytes_(config.maxBatchSizeInBytes),
      timers_(timers) {
  if (batchingPolicy_ != BATCH_SELF_ADJUSTED)
    batchFlushTimer_ = timers_.add(milliseconds(batchFlushPeriodMs_),
                                   Timers::Timer::RECURRING,
                                   [this](Timers::Handle h) { onBatchFlushTimer(h); });
}

RequestsBatchingLogic::~RequestsBatchingLogic() {
  if (batchingPolicy_ != BATCH_SELF_ADJUSTED) timers_.cancel(batchFlushTimer_);
}

void RequestsBatchingLogic::onBatchFlushTimer(Timers::Handle) {
  if (replica_.isCurrentPrimary()) {
    lock_guard<mutex> lock(batchProcessingLock_);
    if (replica_.tryToSendPrePrepareMsg(false)) {
      LOG_INFO(GL, "Batching flush period expired" << KVLOG(batchFlushPeriodMs_));
      closedOnFlush_ += 1;
      timers_.reset(batchFlushTimer_, milliseconds(batchFlushPeriodMs_));
    }
  }
}

PrePrepareMsgCreationResult RequestsBatchingLogic::batchRequestsSelfAdjustedPolicy(SeqNum primaryLastUsedSeqNum,
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
    metric_not_enough_client_requests_event_++;
    return std::make_pair(nullptr, false);
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
  auto totalConsensusesCount = closedOnLogic_ + closedOnFlush_;
  auto perClosedOnFlush = (double)closedOnFlush_ / totalConsensusesCount;
  auto perClosedOnLogic = (double)closedOnLogic_ / totalConsensusesCount;
  start_timer_ = std::chrono::steady_clock::now();
  if (perClosedOnFlush > decreaseCondition_ && maxNumOfRequestsInBatch_ > initialBatchSize_)
    maxNumOfRequestsInBatch_ *= (1 - 2 * increaseRate_);
  else if (perClosedOnFlush > (decreaseCondition_ / 2) && maxNumOfRequestsInBatch_ > initialBatchSize_) {
    maxNumOfRequestsInBatch_ *= (1 - increaseRate_);
  } else if (maxNumOfRequestsInBatch_ <= 3 * initialBatchSize_) {
    if (perClosedOnLogic >= maxIncreaseCondition_) {
      maxNumOfRequestsInBatch_ += maxNumOfRequestsInBatch_ * (increaseRate_ * 1.5);
    } else if (perClosedOnLogic >= midIncreaseCondition_) {
      maxNumOfRequestsInBatch_ += maxNumOfRequestsInBatch_ * increaseRate_;
    } else if (perClosedOnLogic >= minIncreaseCondition_) {
      maxNumOfRequestsInBatch_ += maxNumOfRequestsInBatch_ * (increaseRate_ / 3);
    }
  }
  closedOnFlush_ = 0;
  closedOnLogic_ = 0;
  LOG_INFO(GL, "increasing maxBatchSize to:" << maxNumOfRequestsInBatch_);
}

PrePrepareMsgCreationResult RequestsBatchingLogic::batchRequests() {
  const auto requestsInQueue = replica_.getRequestsInQueue();
  if (requestsInQueue == 0) return std::make_pair(nullptr, false);

  PrePrepareMsgCreationResult prePrepareMsgWithResult{nullptr, false};
  switch (batchingPolicy_) {
    case BATCH_SELF_ADJUSTED:
      prePrepareMsgWithResult = batchRequestsSelfAdjustedPolicy(
          replica_.getPrimaryLastUsedSeqNum(), requestsInQueue, replica_.getLastExecutedSeqNum());
      break;
    case BATCH_BY_REQ_NUM: {
      bool resetTimer = false;
      {
        lock_guard<mutex> lock(batchProcessingLock_);
        prePrepareMsgWithResult = replica_.buildPrePrepareMsgBatchByRequestsNum(maxNumOfRequestsInBatch_);
        resetTimer = prePrepareMsgWithResult.second;
      }
      if (resetTimer) timers_.reset(batchFlushTimer_, milliseconds(batchFlushPeriodMs_));
    } break;
    case BATCH_BY_REQ_SIZE: {
      bool resetTimer = false;
      {
        lock_guard<mutex> lock(batchProcessingLock_);
        prePrepareMsgWithResult = replica_.buildPrePrepareMsgBatchByOverallSize(maxBatchSizeInBytes_);
        resetTimer = prePrepareMsgWithResult.second;
      }
      if (resetTimer) timers_.reset(batchFlushTimer_, milliseconds(batchFlushPeriodMs_));
    } break;
    case BATCH_ADAPTIVE: {
      bool resetTimer = false;
      {
        lock_guard<mutex> lock(batchProcessingLock_);
        prePrepareMsgWithResult = replica_.buildPrePrepareMsgBatchByRequestsNum(maxNumOfRequestsInBatch_);
        resetTimer = prePrepareMsgWithResult.second;
        if (resetTimer) {
          auto period =
              std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_timer_)
                  .count();
          closedOnLogic_ += 1;
          if (period > batchFlushPeriodMs_ * 20) adjustPreprepareSize();
        }
      }
      if (resetTimer) {
        timers_.reset(batchFlushTimer_, milliseconds(batchFlushPeriodMs_));
      }
    } break;
  }
  return prePrepareMsgWithResult;
}

}  // namespace bftEngine::batchingLogic
