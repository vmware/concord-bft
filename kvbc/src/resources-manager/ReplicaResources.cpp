// Copyright 2022 VMware, all rights reserved

#include "ReplicaResources.h"
#include "Logger.hpp"
#include <thread>

using namespace concord::performance;
using namespace std::chrono_literals;

void ReplicaResourceEntity::addMeasurement(const ISystemResourceEntity::measurement &m) {
  if (is_stopped) {
    return;
  }
  // don't wait for the lock if not available, it's ok to loose small amount of measurements
  if (!mutex_.try_lock()) return;
  switch (m.type) {
    // Pruning average time to add a block
    case ISystemResourceEntity::type::pruning_avg_time_micro:
      ConcordAssertGE(m.end, m.start);
      pruning_accumulated_time += (m.end - m.start);
      ++pruned_blocks;
      break;
    // Pruning utilization
    case ISystemResourceEntity::type::pruning_utilization:
      pruning_utilization.addDuration({m.start, m.end});
      break;
    // Poset-exec utilization
    case ISystemResourceEntity::type::post_execution_utilization:
      post_exec_utilization.addDuration({m.start, m.end});
    // no break so calculation of post_execution_avg_time_micro will be performed as well
    case ISystemResourceEntity::type::post_execution_avg_time_micro:
      post_exec_accumulated_time += (m.end - m.start);
      ++post_exec_ops;
      break;
    // Count of reached consensus
    case ISystemResourceEntity::type::transactions_accumulated:
      num_of_consensus += m.count;
      break;
    // Count of block addition
    case ISystemResourceEntity::type::add_blocks_accumulated:
      num_of_blocks += m.count;
      break;
    default:
      break;
  }
  mutex_.unlock();
}

uint64_t ReplicaResourceEntity::getMeasurement(const ISystemResourceEntity::type type) const {
  mutex_.lock();
  uint64_t ret = 0;
  switch (type) {
    // Pruning average time to add a block
    case ISystemResourceEntity::type::pruning_avg_time_micro:
      ret = (pruned_blocks != 0) ? (pruning_accumulated_time / pruned_blocks) : 0;
      break;
    // Pruning utilization
    case ISystemResourceEntity::type::pruning_utilization:
      ret = pruning_utilization.getUtilization();
      break;
    // Poset-exec utilization
    case ISystemResourceEntity::type::post_execution_utilization:
      ret = post_exec_utilization.getUtilization();
      break;
    // Poset-exec avg
    case ISystemResourceEntity::type::post_execution_avg_time_micro:
      ret = (post_exec_ops != 0) ? (post_exec_accumulated_time / post_exec_ops) : 0;
      break;
    // Count of reached consesnus
    case ISystemResourceEntity::type::transactions_accumulated:
      ret = num_of_consensus;
      break;
    // Count of block addition
    case ISystemResourceEntity::type::add_blocks_accumulated:
      ret = num_of_blocks;
      break;
    default:
      break;
  }
  mutex_.unlock();
  return ret;
}

void ReplicaResourceEntity::stop() { is_stopped = true; }

void ReplicaResourceEntity::start() { is_stopped = false; }

void ReplicaResourceEntity::reset() {
  mutex_.lock();
  pruning_utilization.restart();
  pruning_utilization.addMarker();
  post_exec_utilization.restart();
  post_exec_utilization.addMarker();
  pruned_blocks = 0;
  pruning_accumulated_time = 0;
  num_of_consensus = 0;
  num_of_blocks = 0;
  post_exec_ops = 0;
  post_exec_accumulated_time = 0;
  mutex_.unlock();
}