// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string>

#include "state_snapshot_interface.hpp"
#include "categorization/kv_blockchain.h"

namespace concord::kvbc::adapter::categorization {
class DbCheckpointImpl : public IDBCheckpoint {
 public:
  explicit DbCheckpointImpl(std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain>& kvbc);

  /////////////////////////////////IDBCheckpoint////////////////////////////////////////////////////////////////////////
  void trimBlocksFromCheckpoint(BlockId block_id_at_checkpoint) override final {
    return kvbc_->trimBlocksFromSnapshot(block_id_at_checkpoint);
  }

  void checkpointInProcess(bool, kvbc::BlockId) override final {}

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  virtual ~DbCheckpointImpl() { kvbc_ = nullptr; }

 private:
  concord::kvbc::categorization::KeyValueBlockchain* kvbc_{nullptr};
};
}  // namespace concord::kvbc::adapter::categorization
