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

RequestsBatchingLogic::RequestsBatchingLogic(InternalReplicaApi &replica,
                                             const ReplicaConfig &config,
                                             concordMetrics::Component &metrics)
    : replica_(replica),
      metric_not_enough_client_requests_event_{metrics.RegisterCounter("notEnoughClientRequestsEvent")},
      batchingPolicy_((BatchingPolicy)config.batchingPolicy),
      batchingFactorCoefficient_(config.batchingFactorCoefficient),
      maxInitialBatchSize_(config.maxInitialBatchSize) {}

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

PrePrepareMsg *RequestsBatchingLogic::batchRequests() {
  const auto requestsInQueue = replica_.getRequestsInQueue();
  if (requestsInQueue == 0) return nullptr;

  PrePrepareMsg *prePrepareMsg = nullptr;
  switch (batchingPolicy_) {
    case BATCH_SELF_ADJUSTED:
      prePrepareMsg = batchRequestsSelfAdjustedPolicy(
          replica_.getPrimaryLastUsedSeqNum(), requestsInQueue, replica_.getLastExecutedSeqNum());
      break;
    case BATCH_BY_REQ_SIZE:
    case BATCH_BY_REQ_NUM:
      LOG_ERROR(GL, "Unsupported batching policy" << KVLOG(batchingPolicy_));
      return nullptr;
  }
  return prePrepareMsg;
}

}  // namespace bftEngine::batchingLogic
