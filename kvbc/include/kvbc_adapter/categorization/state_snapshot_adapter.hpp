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

#include "categorization/base_types.h"
#include "categorization/updates.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "state_snapshot_interface.hpp"
#include "categorization/kv_blockchain.h"
#include "rocksdb/native_client.h"
#include "ReplicaConfig.hpp"

using concord::storage::rocksdb::NativeClient;
using concord::kvbc::IKVBCStateSnapshot;

namespace concord::kvbc::adapter::categorization::statesnapshot {
class KVBCStateSnapshot : public IKVBCStateSnapshot, public IDBCheckpoint {
 public:
  explicit KVBCStateSnapshot(std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain>& kvbc);

  ////////////////////////////IKVBCStateSnapshot////////////////////////////////////////////////////////////////////////
  void computeAndPersistPublicStateHash(
      BlockId checkpoint_block_id,
      const Converter& value_converter = [](std::string&& s) -> std::string { return std::move(s); }) override final {
    return kvbc_->computeAndPersistPublicStateHash(checkpoint_block_id, value_converter);
  }

  std::optional<PublicStateKeys> getPublicStateKeys() const override final { return kvbc_->getPublicStateKeys(); }

  void iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f) const override final {
    return kvbc_->iteratePublicStateKeyValues(f);
  }

  bool iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f,
                                   const std::string& after_key) const override final {
    return kvbc_->iteratePublicStateKeyValues(f, after_key);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////IDBCheckpoint////////////////////////////////////////////////////////////////////////
  void trimBlocksFromCheckpoint(BlockId block_id_at_checkpoint) override final {
    return kvbc_->trimBlocksFromSnapshot(block_id_at_checkpoint);
  }
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  virtual ~KVBCStateSnapshot() { kvbc_ = nullptr; }

 private:
  concord::kvbc::categorization::KeyValueBlockchain* kvbc_{nullptr};
};
}  // end of namespace concord::kvbc::adapter::categorization::statesnapshot
