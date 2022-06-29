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

#pragma once

#include "replica_state_sync.h"
#include "db_adapter_interface.h"

namespace concord {
namespace kvbc {

class ReplicaStateSyncImp : public ReplicaStateSync {
 public:
  ReplicaStateSyncImp(IBlockMetadata* blockMetadata);
  ~ReplicaStateSyncImp() override = default;

  uint64_t execute(logging::Logger& logger,
                   adapter::ReplicaBlockchain& blockchain,
                   const std::shared_ptr<bftEngine::impl::PersistentStorage>& metadata,
                   uint64_t lastExecutedSeqNum,
                   uint32_t maxNumOfBlocksToDelete) override;

  uint64_t executeBasedOnBftSeqNum(logging::Logger& logger,
                                   adapter::ReplicaBlockchain& blockchain,
                                   uint64_t lastExecutedSeqNum,
                                   uint32_t maxNumOfBlocksToDelete);

  uint64_t executeBasedOnBlockId(logging::Logger& logger,
                                 adapter::ReplicaBlockchain& blockchain,
                                 const std::shared_ptr<bftEngine::impl::PersistentStorage>& metadata,
                                 uint32_t maxNumOfBlocksToDelete);

 protected:
  std::unique_ptr<IBlockMetadata> blockMetadata_;
};

}  // namespace kvbc
}  // namespace concord
