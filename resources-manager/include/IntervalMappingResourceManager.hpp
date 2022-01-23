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

#pragma once

#include "IResourceManager.hpp"
#include "ISystemResourceEntity.hpp"

#include <memory>
#include <vector>
#include <algorithm>

namespace concord::performance {
class IntervalMappingResourceManager : public IResourceManager {
 public:
  virtual ~IntervalMappingResourceManager() = default;
  /*
   IntervalMappingResourceManager implementation takes a vector of sorted uint64_t pairs,
   which maps the consensus engine load to prune blocks per second. E.g. {{100, 200}, {400, 100}, {600, 10}}.
   Traffic up to 100 tps gvies 200 bps to prune, Up to 400 tps 100 bps ... TPS above the max is always 0 bps.
  */
  virtual uint64_t getPruneBlocksPerSecond() const override {
    // In order to calcualte to rate the IResourceManager should sample periodically and calculate the tps from the
    // resolt, as the ISystemResourceEntity does not run in a thread and has only the accumulative counter.
    auto consensus_rate = replicaResources->getMeasurement(ISystemResourceEntity::type::consensus_accumulated);
    return std::upper_bound(
               intervalMapping.begin(), intervalMapping.end(), std::make_pair(consensus_rate, (u_int64_t)0))
        ->second;
  }
  static std::unique_ptr<IntervalMappingResourceManager> createIntervalMappingResourceManager(
      const std::shared_ptr<ISystemResourceEntity> &replicaResources,
      std::vector<std::pair<uint64_t, uint64_t>> &&intervalMapping) {
    intervalMapping.push_back(std::make_pair(UINT64_MAX, 0));
    return std::unique_ptr<IntervalMappingResourceManager>(
        new IntervalMappingResourceManager(replicaResources, std::move(intervalMapping)));
  }

 protected:
  IntervalMappingResourceManager(const std::shared_ptr<ISystemResourceEntity> &replicaResources,
                                 std::vector<std::pair<uint64_t, uint64_t>> &&intervalMapping)
      : replicaResources(replicaResources), intervalMapping(std::move(intervalMapping)) {}

 private:
  const std::shared_ptr<ISystemResourceEntity> replicaResources;
  const std::vector<std::pair<uint64_t, uint64_t>> intervalMapping;
};
}  // namespace concord::performance
