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

#include <vector>
#include <memory>
#include <algorithm>

namespace concord::performance {

class SumsResourceEntitiesAvailabilityManager : public IResourceManager {
 public:
  SumsResourceEntitiesAvailabilityManager(std::vector<std::shared_ptr<ISystemResourceEntity>> &&resourceEntities)
      : resourceEntities(std::move(resourceEntities)) {}
  /*
    SumsResourceEntitiesAvailabilityManager implementation sums all given avialabile resources and outputs as blocks per
    second to be pruned.
  */
  virtual uint64_t getPruneBlocksPerSecond() const override {
    int64_t sum = 0;
    for (const auto &entity : resourceEntities) {
      sum += entity->getAvailableResources();
    }

    return (uint64_t)std::max(sum, (int64_t)0);
  }

 private:
  const std::vector<std::shared_ptr<ISystemResourceEntity>> resourceEntities;
};
}  // namespace concord::performance
