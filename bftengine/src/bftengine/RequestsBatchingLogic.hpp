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

#include "Metrics.hpp"
#include "ReplicaConfig.hpp"
#include "InternalReplicaApi.hpp"
#include "messages/PrePrepareMsg.hpp"

namespace bftEngine::batchingLogic {

class RequestsBatchingLogic {
 public:
  RequestsBatchingLogic(InternalReplicaApi &replica, const ReplicaConfig &config, concordMetrics::Component &metrics);
  virtual ~RequestsBatchingLogic() = default;

  uint32_t getMaxNumberOfPendingRequestsInRecentHistory() const { return maxNumberOfPendingRequestsInRecentHistory_; }
  uint32_t getBatchingFactor() const { return batchingFactor_; }

  PrePrepareMsg *batchRequests();

 private:
  PrePrepareMsg *batchRequestsSelfAdjustedPolicy(SeqNum primaryLastUsedSeqNum,
                                                 uint64_t requestsInQueue,
                                                 SeqNum lastExecutedSeqNum);

 private:
  InternalReplicaApi &replica_;
  concordMetrics::CounterHandle metric_not_enough_client_requests_event_;
  BatchingPolicy batchingPolicy_;
  // Variables used to heuristically compute the 'optimal' batch size
  uint32_t maxNumberOfPendingRequestsInRecentHistory_ = 0;
  uint32_t batchingFactor_ = 1;
  const uint32_t batchingFactorCoefficient_;
  const uint32_t maxInitialBatchSize_;
};

}  // namespace bftEngine::batchingLogic
