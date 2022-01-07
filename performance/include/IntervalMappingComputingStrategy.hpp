// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "IResourceComputingStrategy.hpp"

#include <vector>
#include <utility>
#include <algorithm>

namespace concord::performance {
class IntervalMappingComputingStrategy : public IResourceComputingStrategy {
 public:
  IntervalMappingComputingStrategy(std::vector<std::pair<uint64_t, uint64_t>> &&intervalMappingSorted)
      : intervalMapping(std::move(intervalMappingSorted)) {
    intervalMapping.push_back(std::make_pair(UINT64_MAX, 0));
  };
  virtual ~IntervalMappingComputingStrategy() = default;
  virtual std::uint64_t computeAvailableResources(const ISystemResourceEntity &consensusMonitor,
                                                  const ISystemResourceEntity &storageMonitor) override {
    return std::upper_bound(intervalMapping.begin(),
                            intervalMapping.end(),
                            std::make_pair((u_int64_t)consensusMonitor.getAvailableResources(), (u_int64_t)0))
        ->second;
  };

 private:
  std::vector<std::pair<uint64_t, uint64_t>> &&intervalMapping;
};
}  // namespace concord::performance