// Copyright 2022 VMware, all rights reserved

#pragma once

#include "ISystemResourceEntity.hpp"
// This class collects several measurements that help to evaluate the load on the system.
// The current usage is for deciding how many blocks can be pruned.
// IMPORTANT !!! - the measurements are added from the post-execution-thread and collected from another thread.
// in order not to lock the hot path, few assumptions are made, and it's important to keep them.
// - single writer adds measurements, single reader gets measurements and also can reset them
// - the frequency of the writes is high
// - using stop before getting metrics and start when finishing ensures mutual exclusion.
// - delaying the reading thread is ok, as its priority is low.
class ReplicaResourceEntity : public concord::performance::ISystemResourceEntity {
 public:
  // From the post-execution thread
  virtual void addMeasurement(const concord::performance::ISystemResourceEntity::measurement& m);

  //////////The reading thread, must use stop before calling and start on finish////
  virtual uint64_t getMeasurement(const ISystemResourceEntity::type type) const;
  virtual void reset();
  /////////////////////////////////////////////////////////////////////////////////

  virtual const std::string getResourceName() const { return "Replica resource entity"; }

  // Not meaningful, should consider removing from interface
  virtual int64_t getAvailableResources() const { return 0; }

  // stops the addition of metrics
  virtual void stop();
  // starts the addition of metrics
  virtual void start();

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
  std::atomic_bool is_busy{true};
};
