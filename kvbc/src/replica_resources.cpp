// Copyright 2022 VMware, all rights reserved

#include "replica_resources.h"
#include "Logger.hpp"
#include <thread>

using namespace concord::performance;
using namespace std::chrono_literals;

void ReplicaResourceEntity::addMeasurement(const ISystemResourceEntity::measurement &m) {
  // if the reading thread sets to stop, the writing thread signal that it saw it,
  // by setting the is_busy flag to false.
  if (is_stopped) {
    is_busy = false;
    return;
  } else {
    is_busy = true;
  }
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
    case ISystemResourceEntity::type::consensus_accumulated:
      num_of_consensus += m.count;
      break;
    // Count of block addition
    case ISystemResourceEntity::type::add_blocks_accumulated:
      num_of_blocks += m.count;
      break;
    default:
      break;
  }
}

uint64_t ReplicaResourceEntity::getMeasurement(const ISystemResourceEntity::type type) const {
  switch (type) {
    // Pruning average time to add a block
    case ISystemResourceEntity::type::pruning_avg_time_micro:
      return (pruned_blocks != 0) ? (pruning_accumulated_time / pruned_blocks) : 0;
    // Pruning utilization
    case ISystemResourceEntity::type::pruning_utilization:
      return pruning_utilization.getUtilization();
    // Poset-exec utilization
    case ISystemResourceEntity::type::post_execution_utilization:
      return post_exec_utilization.getUtilization();
    // Poset-exec avg
    case ISystemResourceEntity::type::post_execution_avg_time_micro:
      return (post_exec_ops != 0) ? (post_exec_accumulated_time / post_exec_ops) : 0;
    // Count of reached consesnus
    case ISystemResourceEntity::type::consensus_accumulated:
      return num_of_consensus;
    // Count of block addition
    case ISystemResourceEntity::type::add_blocks_accumulated:
      return num_of_blocks;
    default:
      break;
  }
  return 0;
}

// only when is_busy is set to the desired state we can be sure that the writing thread
// is not on the critical path.
void ReplicaResourceEntity::stop() {
  is_stopped = true;
  while (is_busy) {
    std::this_thread::sleep_for(100us);
  }
}

void ReplicaResourceEntity::start() {
  is_stopped = false;
  while (!is_busy) {
    std::this_thread::sleep_for(100us);
  }
}

void ReplicaResourceEntity::reset() {
  pruning_utilization.restart();
  post_exec_utilization.restart();
  pruned_blocks = 0;
  pruning_accumulated_time = 0;
  num_of_consensus = 0;
  num_of_blocks = 0;
  post_exec_ops = 0;
  post_exec_accumulated_time = 0;
}
