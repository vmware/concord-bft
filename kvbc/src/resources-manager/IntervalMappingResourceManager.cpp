// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "IntervalMappingResourceManager.hpp"

using namespace concord::performance;

// IntervalMappingResourceManager::IntervalMappingResourceManager(ISystemResourceEntity &replicaResources,
//                                  std::vector<std::pair<uint64_t, uint64_t>> &&intervalMapping)
//       : replica_resources_(replicaResources), intervalMapping_(std::move(intervalMapping)) {
//         std::ostringstream intervals;
//         for(const auto& p:intervalMapping_){
//           intervals << "{" << p.first << "," << p.second << "},";
//         }
//         LOG_INFO(ADPTV_PRUNING,"Constructing with the following intervals " << intervals.str());
// }

const std::vector<std::pair<uint64_t, uint64_t>> IntervalMappingResourceManager::default_mapping = {
    {20, 35}, {100, 21}, {300, 14}, {500, 7}};

IntervalMappingResourceManager::IntervalMappingResourceManager(
    ISystemResourceEntity &replicaResources,
    std::vector<std::pair<uint64_t, uint64_t>> &&intervalMapping,
    const IntervalMappingResourceManagerConfiguration &configuration)
    : replicaResources_(replicaResources), intervalMapping_(std::move(intervalMapping)), configuration(configuration) {
  std::ostringstream intervals;
  for (const auto &[rate, blocks] : intervalMapping_) {
    intervals << "{" << rate << "," << blocks << "},";
  }
  LOG_INFO(ADPTV_PRUNING, "Constructing with the following intervals {rate,blocks}" << intervals.str());
}

PruneInfo IntervalMappingResourceManager::getPruneInfo() {
  auto duration = getDurationFromLastCallSec();
  if (duration == 0 || (++period_) % periodicInterval_ == 0) {
    LOG_DEBUG(ADPTV_PRUNING, "Resetting measurements");
    period_ = 0;
    lastTPS_ = 0;
    replicaResources_.reset();
    return PruneInfo{};
  }

  // Get measurements and emit stats log
  auto transactions = replicaResources_.getMeasurement(ISystemResourceEntity::type::transactions_accumulated);
  auto postExecUtilization = replicaResources_.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
  auto pruningUtilization = replicaResources_.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
  auto pruningAvgTimeMicro = replicaResources_.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
  auto tps = transactions / duration;
  // in order to give an up to date result in the next invocation
  replicaResources_.reset();
  auto it = std::upper_bound(intervalMapping_.begin(), intervalMapping_.end(), std::make_pair(tps, (u_int64_t)0));

  PruneInfo ret;
  if (it != intervalMapping_.end()) {
    // 0.9 reflects maximum allowed 10% drop in perfomance.
    long double adjust = 1;
    if (lastTPS_ * 0.9 > tps && pruningUtilization > configuration.limitMaximumPruningTimeUtilizationPercentage) {
      adjust = (100 - (long double)pruningUtilization) / 100.0;
      LOG_WARN(ADPTV_PRUNING, "Pruning consumes too much resources at the cost of TPS. Adjusting by " << adjust);

      // pruningTimeUlizationTPSInterferenceLimit of percentage where it can be assumed that tps fall is not caused by
      // pruning
    } else if (pruningUtilization < configuration.pruningTimeUlizationTPSInterferenceLimitPercentage ||
               lastTPS_ < tps) {
      lastTPS_ = tps;
    }
    ret.blocksPerSecond = static_cast<long double>(it->second) * adjust;
    ret.batchSize = 1;
    ret.postExecUtilization = postExecUtilization;
    ret.pruningUtilization = pruningUtilization;
    ret.pruningAvgTimeMicro = pruningAvgTimeMicro;
    ret.transactionsPerSecond = tps;
  }

  LOG_INFO(ADPTV_PRUNING,
           "calculated [" << tps << "] transactions per second, mapped to pruning [" << ret.blocksPerSecond
                          << "] blocks per second, post execution utilization is [" << postExecUtilization
                          << "], pruning utilization is [" << pruningUtilization << "], avg time to prune a block ["
                          << pruningAvgTimeMicro << "] micro seconds");

  return ret;
}

uint64_t IntervalMappingResourceManager::getDurationFromLastCallSec() {
  if (lastInvocationTime_ == 0) {
    lastInvocationTime_ =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    return 0;
  }
  std::uint64_t currTime =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
          .count();

  uint64_t dur = (currTime - lastInvocationTime_) / 1000;
  // Since we use integral types, lower than 1 will be rounded to 0, we'll set the minimum dur to be 1 sec.
  if (dur == 0) dur = 1;
  lastInvocationTime_ = currTime;
  return dur;
}