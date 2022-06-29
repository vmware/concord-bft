// Concord
//
// Copyright (c) 2019-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// Interface for replica state synchronization.

#pragma once

#include "storage/db_types.h"
#include "Logger.hpp"
#include "db_adapter_interface.h"
#include "kvbc_adapter/replica_adapter.hpp"
#include "PersistentStorage.hpp"

#include <memory>

namespace concord::kvbc {

class IBlockMetadata;

class ReplicaStateSync {
 public:
  virtual ~ReplicaStateSync() = default;

  // Synchronizes replica state and returns a number of deleted blocks.
  virtual uint64_t execute(logging::Logger& logger,
                           adapter::ReplicaBlockchain& blockchain,
                           const std::shared_ptr<bftEngine::impl::PersistentStorage>& metadata,
                           uint64_t lastExecutedSeqNum,
                           uint32_t maxNumOfBlocksToDelete) = 0;
};

}  // namespace concord::kvbc
