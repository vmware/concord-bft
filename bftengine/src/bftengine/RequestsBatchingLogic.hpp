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

#pragma once

#include <utility>

#include "Metrics.hpp"
#include "ReplicaConfig.hpp"
#include "InternalReplicaApi.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "Timers.hpp"
#include "performance_handler.h"

namespace bftEngine::batchingLogic {

class RequestsBatchingLogic {
 public:
  RequestsBatchingLogic(InternalReplicaApi &replica,
                        const ReplicaConfig &config,
                        concordMetrics::Component &metrics,
                        concordUtil::Timers &timers);
  virtual ~RequestsBatchingLogic();

  uint32_t getMaxNumberOfPendingRequestsInRecentHistory() const { return maxNumberOfPendingRequestsInRecentHistory_; }
  uint32_t getBatchingFactor() const { return batchingFactor_; }

  PrePrepareMsgCreationResult batchRequests();

 private:
  void onBatchFlushTimer(concordUtil::Timers::Handle timer);
  PrePrepareMsgCreationResult batchRequestsSelfAdjustedPolicy(SeqNum primaryLastUsedSeqNum,
                                                              uint64_t requestsInQueue,
                                                              SeqNum lastExecutedSeqNum);
  void adjustPreprepareSize();

 private:
  InternalReplicaApi &replica_;
  concordMetrics::CounterHandle metric_not_enough_client_requests_event_;
  BatchingPolicy batchingPolicy_;
  // Variables used to heuristically compute the 'optimal' batch size
  uint32_t maxNumberOfPendingRequestsInRecentHistory_ = 0;
  uint32_t batchingFactor_ = 1;
  std::chrono::steady_clock::time_point start_timer_ = std::chrono::steady_clock::now();
  const uint32_t batchingFactorCoefficient_;
  const uint32_t maxInitialBatchSize_;
  const uint32_t batchFlushPeriodMs_;
  uint16_t closedOnLogic_ = 0;
  uint16_t closedOnFlush_ = 0;
  uint32_t maxNumOfRequestsInBatch_;
  const double increaseRate_;
  const double decreaseCondition_;
  const double maxIncreaseCondition_;
  const double midIncreaseCondition_;
  const double minIncreaseCondition_;
  const uint32_t initialBatchSize_;
  const uint32_t maxBatchSizeInBytes_;
  concordUtil::Timers &timers_;
  concordUtil::Timers::Handle batchFlushTimer_;
  std::mutex batchProcessingLock_;

  static constexpr uint64_t MAX_VALUE_MICROSECONDS = 1000lu * 1000 * 60;
};

}  // namespace bftEngine::batchingLogic
