#pragma once

#include <cstdint>
#include <string>

namespace concord::performance {
class ISystemResourceEntity {
 public:
  virtual int64_t getAvailableResources() const = 0;
  virtual uint64_t getMeasurements() const = 0;
  virtual const std::string& getResourceName() const = 0;
};
}  // namespace concord::performance