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

#include "replica_state_sync_imp.hpp"
#include "bftengine/DbMetadataStorage.hpp"
#include "block_metadata.hpp"
#include "kvstream.h"

using concord::kvbc::Key;

namespace concord::kvbc {

ReplicaStateSyncImp::ReplicaStateSyncImp(IBlockMetadata* blockMetadata) : blockMetadata_(blockMetadata) {}

uint64_t ReplicaStateSyncImp::execute(logging::Logger& logger,
                                      IDbAdapter& bcDBAdapter,
                                      BlockId lastReachableBlockId,
                                      uint64_t lastExecutedSeqNum) {
  if (!lastExecutedSeqNum) {
    LOG_INFO(logger, "Replica's metadata is empty => skip blocks removal");
    return 0;
  }
  uint64_t removedBlocksNum = 0;
  const auto genesisBlockId = bcDBAdapter.getGenesisBlockId();
  const auto blockMetadataKey = blockMetadata_->getKey();
  uint64_t lastBlockSeqNum = 0;
  while (lastReachableBlockId && genesisBlockId <= lastReachableBlockId) {
    // Get execution sequence number stored in the current last block.
    // After a last block deletion blockSeqNum gets a new value.
    lastBlockSeqNum = blockMetadata_->getLastBlockSequenceNum(blockMetadataKey);
    LOG_INFO(logger, KVLOG(lastExecutedSeqNum, lastBlockSeqNum, lastReachableBlockId));
    if (lastBlockSeqNum <= lastExecutedSeqNum) {
      LOG_INFO(logger, "Replica state is in sync " << KVLOG(removedBlocksNum, lastBlockSeqNum, lastReachableBlockId));
      return removedBlocksNum;
    }
    // SBFT State Metadata is not in sync with the Blockchain State.
    // Remove blocks which sequence number is greater than lastExecutedSeqNum.
    if (removedBlocksNum == 1) {
      std::string error = " Detected more than one block needs to be deleted from the blockchain - unsupported";
      LOG_FATAL(logger, error);
      throw std::runtime_error(__PRETTY_FUNCTION__ + error);
    }
    bcDBAdapter.deleteLastReachableBlock();
    --lastReachableBlockId;
    ++removedBlocksNum;
  }
  LOG_INFO(logger,
           "Inconsistent blockchain block deleted, if found"
               << KVLOG(removedBlocksNum, lastExecutedSeqNum, lastBlockSeqNum, lastReachableBlockId));
  return removedBlocksNum;
}

}  // namespace concord::kvbc
