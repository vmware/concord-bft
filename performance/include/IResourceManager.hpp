#pragma once

#include <cstdint>

namespace concord::performance {
class IResourceManager {
 public:
  virtual ~IResourceManager() = default;
  virtual uint64_t getAvailableResources() const = 0;
};

}  // namespace concord::performance
