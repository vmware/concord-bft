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
#include <type_traits>

#include "state_snapshot_interface.hpp"
#include "v4blockchain/v4_blockchain.h"

namespace concord::kvbc::adapter::v4blockchain {
class BlocksDbCheckpointAdapter : public IDBCheckpoint {
 public:
  explicit BlocksDbCheckpointAdapter(std::shared_ptr<concord::kvbc::v4blockchain::KeyValueBlockchain>& v4_kvbc)
      : v4_kvbc_(v4_kvbc.get()) {}

  /////////////////////////////////IDBCheckpoint////////////////////////////////////////////////////////////////////////
  void trimBlocksFromCheckpoint(BlockId block_id_at_checkpoint) override final {
    return v4_kvbc_->trimBlocksFromSnapshot(block_id_at_checkpoint);
  }

  // Do nothing, its a NOOP for categorized blockchain
  void checkpointInProcess(bool flag, kvbc::BlockId bid) override final { v4_kvbc_->checkpointInProcess(flag, bid); }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  virtual ~BlocksDbCheckpointAdapter() { v4_kvbc_ = nullptr; }

 private:
  concord::kvbc::v4blockchain::KeyValueBlockchain* v4_kvbc_{nullptr};
};
}  // namespace concord::kvbc::adapter::v4blockchain
