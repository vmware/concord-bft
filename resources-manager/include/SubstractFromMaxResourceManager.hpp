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

#include "IResourceManager.hpp"
#include "ISystemResourceEntity.hpp"

#include <cstdint>
#include <vector>
#include <memory>

namespace concord::performance {

class SubstractFromMaxResourceManager : public IResourceManager {
 public:
  SubstractFromMaxResourceManager(const uint64_t maxSystemCapacity,
                                  std::vector<std::shared_ptr<ISystemResourceEntity>> &&resourceEntities)
      : maxSystemCapacity(maxSystemCapacity), resourceEntities(std::move(resourceEntities)) {}
  virtual ~SubstractFromMaxResourceManager() = default;
  /*
    SubstractFromMaxResourceManager implementation sums all given measurements and substract it from configured maximum
    system capacity.
  */
  virtual std::uint64_t getPruneBlocksPerSecond() const override {
    std::uint64_t sum = 0;
    for (const auto &entity : resourceEntities) {
      sum += entity->getMeasurements();
    }
    return maxSystemCapacity - sum;
  }

 private:
  const uint64_t maxSystemCapacity;
  std::vector<std::shared_ptr<ISystemResourceEntity>> &&resourceEntities;
};

}  // namespace concord::performance
