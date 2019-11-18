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
// Interface for replica state synchronisation.

#pragma once

#include "Logger.hpp"
#include "blockchain/db_adapter.h"
#include "blockchain/db_types.h"

namespace concord {
namespace kvbc {

class ReplicaStateSync {
 public:
  virtual ~ReplicaStateSync() = default;

  // Synchronises replica state and returns a number of deleted blocks.
  virtual uint64_t execute(concordlogger::Logger &logger,
                           concord::storage::blockchain::DBAdapter &bcDBAdapter,
                           concord::storage::blockchain::ILocalKeyValueStorageReadOnly &kvs,
                           concord::storage::blockchain::BlockId lastReachableBlockId,
                           uint64_t lastExecutedSeqNum) = 0;
};

}
}
