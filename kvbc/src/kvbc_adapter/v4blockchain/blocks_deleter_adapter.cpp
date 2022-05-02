// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "kvbc_adapter/v4blockchain/blocks_deleter_adapter.hpp"
#include "assertUtils.hpp"
#include "ReplicaResources.h"

using concord::performance::ISystemResourceEntity;

namespace concord::kvbc::adapter::v4blockchain {

BlocksDeleterAdapter::BlocksDeleterAdapter(std::shared_ptr<concord::kvbc::v4blockchain::KeyValueBlockchain> &kvbc,
                                           const std::optional<aux::AdapterAuxTypes> &aux_types)
    : kvbc_{kvbc.get()} {
  if (aux_types.has_value()) {
    replica_resources_.reset(&(aux_types->resource_entity_));
  } else {
    replica_resources_ = std::make_shared<ReplicaResourceEntity>();
  }
  ConcordAssertNE(kvbc_, nullptr);
  ConcordAssertEQ(!replica_resources_, false);
}

BlockId BlocksDeleterAdapter::deleteBlocksUntil(BlockId until) {
  const auto start = std::chrono::steady_clock::now();
  ISystemResourceEntity::scopedDurMeasurment mes(*replica_resources_,
                                                 ISystemResourceEntity::type::pruning_avg_time_micro);
  auto upTo = kvbc_->deleteBlocksUntil(until);

  auto jobDuration =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
  histograms_.delete_batch_blocks_duration->recordAtomic(jobDuration);

  return upTo;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}  // namespace concord::kvbc::adapter::v4blockchain
