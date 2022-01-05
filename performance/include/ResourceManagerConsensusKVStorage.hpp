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
#include "IResourceManager.hpp"
#include "ISystemResourceEntity.hpp"

#include <memory>

namespace concord::performance {
class ResourceManagerConsensusKVStorage : public IResourceManager {
 public:
  ResourceManagerConsensusKVStorage(const std::shared_ptr<ISystemResourceEntity> &consensusEngine,
                                    const std::shared_ptr<ISystemResourceEntity> &kvStorage,
                                    const std::shared_ptr<IResourceComputingStrategy> &resourceComputingStrategy)
      : consensusEngine(consensusEngine), kvStorage(kvStorage), resourceComputingStrategy(resourceComputingStrategy) {}
  virtual std::uint64_t getAvailableResources() const override {
    return resourceComputingStrategy->computeAvailableResources(*consensusEngine, *kvStorage);
  }

 private:
  const std::shared_ptr<ISystemResourceEntity> consensusEngine;
  const std::shared_ptr<ISystemResourceEntity> kvStorage;
  const std::shared_ptr<IResourceComputingStrategy> resourceComputingStrategy;
};
}  // namespace concord::performance
