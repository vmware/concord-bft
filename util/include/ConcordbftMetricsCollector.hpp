//
// Created by ybuchnik on 12/23/19.
//

#pragma once

#include <string>

namespace concordMetrics {

class IMetricsCollector {
 public:
  IMetricsCollector() = default;
  virtual ~IMetricsCollector(){};

  virtual void increment(const std::string& counterPath) = 0;
  virtual void set(const std::string& gaugePath, uint64_t val) = 0;
  virtual void update(const std::string& statusPath, const std::string& val) = 0;
};
}  // namespace concordMetrics