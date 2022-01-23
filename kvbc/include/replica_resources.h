// Copyright 2022 VMware, all rights reserved

#pragma once

#include "ISystemResourceEntity.hpp"

class ReplicaResourceEntity : public concord::performance::ISystemResourceEntity {
 public:
  virtual void addMeasurement(const concord::performance::ISystemResourceEntity::measurement& m);

  virtual uint64_t getMeasurement(const ISystemResourceEntity::type type) const;

  virtual const std::string getResourceName() const { return "Replica resource entity"; }

  virtual void reset();

  // Not meaningful, should consider removing from interface
  virtual int64_t getAvailableResources() const { return 0; }

  virtual void stop() { is_stopped = true; }
  virtual void start() { is_stopped = false; }

 private:
  mutable std::mutex mutex_;
  std::atomic_uint64_t pruned_blocks{0};
  std::atomic_uint64_t pruning_accumulated_time{0};
  concordUtil::utilization pruning_utilization;
  std::atomic_uint64_t post_exec_ops{0};
  std::atomic_uint64_t post_exec_accumulated_time{0};
  concordUtil::utilization post_exec_utilization;
  std::atomic_uint64_t num_of_consensus{0};
  std::atomic_uint64_t num_of_blocks{0};
  std::atomic_bool is_stopped{false};
};
