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

struct IntervalMappingResourceManagerConfiguration {
  uint16_t limitMaximumPruningTimeUtilizationPercentage{60};
  uint16_t pruningTimeUlizationTPSInterferenceLimitPercentage{20};
};

namespace concord::performance {
class IntervalMappingResourceManager : public IResourceManager {
 public:
  static const std::vector<std::pair<uint64_t, uint64_t>> default_mapping;
  virtual ~IntervalMappingResourceManager() = default;
  /*
   IntervalMappingResourceManager implementation takes a vector of sorted uint64_t pairs,
   which maps the consensus engine load to prune blocks per second. E.g. {{100, 200}, {400, 100}, {600, 10}}.
   Traffic up to 100 tps gvies 200 bps to prune, Up to 400 tps 100 bps ... TPS above the max is always 0 bps.
  */
  virtual PruneInfo getPruneInfo() override;

  static std::unique_ptr<IntervalMappingResourceManager> createIntervalMappingResourceManager(
      ISystemResourceEntity &replicaResources,
      std::vector<std::pair<uint64_t, uint64_t>> &&intervalMapping,
      const IntervalMappingResourceManagerConfiguration &configuration =
          IntervalMappingResourceManagerConfiguration{}) {
    sort(intervalMapping.begin(), intervalMapping.end());
    intervalMapping.push_back(std::make_pair(UINT64_MAX, 0));
    return std::unique_ptr<IntervalMappingResourceManager>(
        new IntervalMappingResourceManager(replicaResources, std::move(intervalMapping), configuration));
  }

  std::uint64_t getDurationFromLastCallSec();
  virtual void setPeriod(std::uint64_t interval) override { periodicInterval_ = interval; }

 protected:
  IntervalMappingResourceManager(ISystemResourceEntity &replicaResources,
                                 std::vector<std::pair<uint64_t, uint64_t>> &&intervalMapping,
                                 const IntervalMappingResourceManagerConfiguration &configuration);

 private:
  ISystemResourceEntity &replicaResources_;
  const std::vector<std::pair<uint64_t, uint64_t>> intervalMapping_;
  std::uint64_t lastInvocationTime_{0};
  std::uint64_t period_{0};
  std::uint64_t periodicInterval_{20};  // config
  std::uint64_t lastTPS_{0};
  IntervalMappingResourceManagerConfiguration configuration;
};
}  // namespace concord::performance
