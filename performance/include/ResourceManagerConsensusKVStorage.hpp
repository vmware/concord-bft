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
