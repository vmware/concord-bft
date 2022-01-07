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

#include <cstdint>

namespace concord::performance {

class SubstractFromMaxComputingStrategy : public IResourceComputingStrategy {
 public:
  SubstractFromMaxComputingStrategy(const uint64_t maxSystemCapacity) : maxSystemCapacity(maxSystemCapacity){};
  virtual std::uint64_t computeAvailableResources(const ISystemResourceEntity &consensusMonitor,
                                                  const ISystemResourceEntity &storageMonitor) {
    return maxSystemCapacity - consensusMonitor.getAvailableResources() - storageMonitor.getAvailableResources();
  };

 private:
  const uint64_t maxSystemCapacity;
};

}  // namespace concord::performance