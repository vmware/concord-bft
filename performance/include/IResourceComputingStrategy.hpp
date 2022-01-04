#pragma once

#include <cstdint>
#include "ISystemResourceEntity.hpp"

namespace concord::performance {

class IResourceComputingStrategy {
 public:
  virtual std::uint64_t computeAvailableResources(const ISystemResourceEntity &consensusMonitor,
                                                  const ISystemResourceEntity &storageMonitor) = 0;
};

}  // namespace concord::performance