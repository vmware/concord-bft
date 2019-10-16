// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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
#include "storage/db_metadata_storage.h"
#include "block_metadata.hpp"

using concord::storage::blockchain::DBAdapter;
using concord::storage::blockchain::BlockId;
using concord::storage::DBMetadataStorage;
using concord::storage::blockchain::ILocalKeyValueStorageReadOnly;
using concordUtils::Key;

namespace SimpleKVBC {

uint64_t ReplicaStateSyncImp::execute(concordlogger::Logger &logger,
                                      DBAdapter &bcDBAdapter,
                                      ILocalKeyValueStorageReadOnly &kvs,
                                      BlockId lastReachableBlockId,
                                      uint64_t lastExecutedSeqNum) {
  BlockMetadata metadata(kvs);
  BlockId blockId = lastReachableBlockId;
  uint64_t blockSeqNum = 0;
  uint64_t removedBlocksNum = 0;
  Key key = metadata.Key();
  do {
    blockSeqNum = metadata.Get(key);
    LOG_INFO(logger, "Block Metadata key = " << key << ", blockId = " << blockId << ", blockSeqNum = " << blockSeqNum << ", lastExecutedSeqNum = " << lastExecutedSeqNum );
    if (blockSeqNum <= lastExecutedSeqNum) {
      LOG_INFO(logger, "Replica state is in sync; removedBlocksNum is " << removedBlocksNum);
      return removedBlocksNum;
    }
    // SBFT State Metadata is not in sync with SBFT State.
    // Remove blocks which sequence number is greater than lastExecutedSeqNum.
    bcDBAdapter.deleteBlockAndItsKeys(blockId--);
    ++removedBlocksNum;
  } while (blockId);
  return removedBlocksNum;
}

}  // namespace SimpleKVBC
