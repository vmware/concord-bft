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

#include "assertUtils.hpp"
#include "replica_state_sync_imp.hpp"
#include "bftengine/DbMetadataStorage.hpp"
#include "block_metadata.hpp"
#include "kvstream.h"
#include "metadata_block_id.h"

#include <stdexcept>

using concord::kvbc::Key;

namespace concord::kvbc {

ReplicaStateSyncImp::ReplicaStateSyncImp(IBlockMetadata* blockMetadata) : blockMetadata_(blockMetadata) {}

uint64_t ReplicaStateSyncImp::execute(logging::Logger& logger,
                                      adapter::ReplicaBlockchain& blockchain,
                                      const std::shared_ptr<bftEngine::impl::PersistentStorage>& metadata,
                                      uint64_t lastExecutedSeqNum,
                                      uint32_t maxNumOfBlocksToDelete) {
  // If we don't have the last reachable block ID in metadata, assume this is the first system startup after upgrade to
  // block ID based sync. Run the existing BFT sequence number based sync first. This is needed, because the system
  // might be out of sync before the upgrade.
  //
  // After the BFT sequence number based sync completes (possibly syncing bad state before the upgrade),
  // persist the last block ID in metadata for the block ID based sync to operate properly onwards.
  //
  // Additionally, we **assume** that the first block after software upgrade is successfully added to both KVBC and
  // metadata by the replica. If that is not the case, we don't have a way of knowing how many blocks to delete and,
  // therefore, we don't delete any as the last block ID in metadata would just be persisted with the last reachable
  // block ID from KVBC, making them equal.
  if (!getLastBlockIdFromMetadata(metadata)) {
    const auto deletedBasedOnBftSeqNum =
        executeBasedOnBftSeqNum(logger, blockchain, lastExecutedSeqNum, maxNumOfBlocksToDelete);

    constexpr auto in_transaction = false;
    persistLastBlockIdInMetadata<in_transaction>(blockchain, metadata);

    return deletedBasedOnBftSeqNum;
  }

  // Run the block ID based sync.
  return executeBasedOnBlockId(logger, blockchain, metadata, maxNumOfBlocksToDelete);
}

uint64_t ReplicaStateSyncImp::executeBasedOnBftSeqNum(logging::Logger& logger,
                                                      adapter::ReplicaBlockchain& blockchain,
                                                      uint64_t lastExecutedSeqNum,
                                                      uint32_t maxNumOfBlocksToDelete) {
  if (!lastExecutedSeqNum) {
    LOG_INFO(logger, "Replica's metadata is empty => skip blocks removal");
    return 0;
  }
  uint64_t removedBlocksNum = 0;
  const auto genesisBlockId = blockchain.getGenesisBlockId();
  BlockId lastReachableBlockId = blockchain.getLastBlockId();
  uint64_t lastBlockSeqNum = 0;
  while (lastReachableBlockId && genesisBlockId <= lastReachableBlockId) {
    // Get execution sequence number stored in the current last block.
    // After a last block deletion blockSeqNum gets a new value.
    lastBlockSeqNum = blockMetadata_->getLastBlockSequenceNum();
    LOG_INFO(logger, KVLOG(lastExecutedSeqNum, lastBlockSeqNum, lastReachableBlockId));
    if (lastBlockSeqNum <= lastExecutedSeqNum) {
      LOG_INFO(logger,
               "Replica state is in sync (based on BFT seq num) "
                   << KVLOG(removedBlocksNum, lastBlockSeqNum, lastReachableBlockId));
      return removedBlocksNum;
    }
    // SBFT State Metadata is not in sync with the Blockchain State.
    // Remove blocks which sequence number is greater than lastExecutedSeqNum.
    if (removedBlocksNum >= maxNumOfBlocksToDelete) {
      std::string error = " Detected too many blocks to be deleted from the blockchain";
      LOG_FATAL(logger, error);
      throw std::runtime_error(__PRETTY_FUNCTION__ + error);
    }
    blockchain.deleteLastReachableBlock();
    lastReachableBlockId = blockchain.getLastBlockId();
    ++removedBlocksNum;
  }
  LOG_INFO(logger,
           "Inconsistent blockchain block deleted "
               << KVLOG(removedBlocksNum, lastExecutedSeqNum, lastBlockSeqNum, blockchain.getLastBlockId()));
  blockchain.onFinishDeleteLastReachable();
  return removedBlocksNum;
}

uint64_t ReplicaStateSyncImp::executeBasedOnBlockId(logging::Logger& logger,
                                                    adapter::ReplicaBlockchain& blockchain,
                                                    const std::shared_ptr<bftEngine::impl::PersistentStorage>& metadata,
                                                    uint32_t maxNumOfBlocksToDelete) {
  if (0 == maxNumOfBlocksToDelete) {
    LOG_WARN(logger,
             "Maximum number of blocks to delete is 0, replica state sync based on block ID will not run "
                 << KVLOG(maxNumOfBlocksToDelete));
    return 0;
  }
  const auto lastMtdBlockId = getLastBlockIdFromMetadata(metadata);
  ConcordAssert(lastMtdBlockId.has_value());
  const auto genesisBlockId = blockchain.getGenesisBlockId();
  auto lastReachableKvbcBlockId = blockchain.getLastBlockId();
  auto deletedBlocks = uint64_t{0};
  // Even though the KVBC implementation at the time of writing cannot delete the only block left (the genesis block in
  // the loop below can be the only one left), we still write the code here so that it attempts to delete, even though
  // it throws. Rationale is that we cannot leave the system in an unknown state. Instead, we throw, effectively
  // stopping the system and await manual intervention as the next startup would lead to the same situation.
  while (lastReachableKvbcBlockId > lastMtdBlockId && genesisBlockId <= lastReachableKvbcBlockId) {
    LOG_INFO(logger,
             "Deleting last reachable " << lastReachableKvbcBlockId << " metadata block " << *lastMtdBlockId
                                        << " block deleted " << deletedBlocks << " max num blocks to delete "
                                        << maxNumOfBlocksToDelete);
    blockchain.deleteLastReachableBlock();
    lastReachableKvbcBlockId = blockchain.getLastBlockId();
    ++deletedBlocks;

    if (deletedBlocks == maxNumOfBlocksToDelete) {
      const auto msg = std::string{"Detected too many blocks to be deleted from the blockchain"};
      LOG_FATAL(logger, msg);
      throw std::runtime_error{msg};
    }
  }
  if (deletedBlocks == 0) {
    LOG_INFO(logger,
             "Replica state is in sync (based on last reachable KVBC block ID) " << KVLOG(lastReachableKvbcBlockId));
  } else {
    LOG_INFO(logger,
             "Replica state sync based on last reachable KVBC block ID deleted blocks "
                 << KVLOG(deletedBlocks, lastReachableKvbcBlockId));
  }
  blockchain.onFinishDeleteLastReachable();
  return deletedBlocks;
}

}  // namespace concord::kvbc
