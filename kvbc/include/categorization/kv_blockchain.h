// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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

#include "updates.h"
#include "rocksdb/native_client.h"
#include <memory>
#include "blocks.h"
#include "blockchain.h"

#include "kv_types.hpp"

namespace concord::kvbc::categorization {

class KeyValueBlockchain {
 public:
  KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client)
      : native_client_{native_client}, block_chain_{native_client_}, state_transfer_block_chain_{native_client_} {}

  /////////////////////// Add Block ///////////////////////

  BlockId addBlock(Updates&& updates);

  /////////////////////// Delete block ///////////////////////

  bool deleteBlock(const BlockId& blockId);
  void deleteLastReachableBlock();

  /////////////////////// Info ///////////////////////
  inline BlockId getGenesisBlockId() { return block_chain_.getGenesisBlockId(); }
  inline BlockId getLastReachableBlockId() { return block_chain_.getLastReachableBlockId(); }

 private:
  void deleteStateTransferBlock(const BlockId block_id);
  void deleteGenesisBlock();

  // Delete per category
  void deleteGenesisBlock(BlockId block_id,
                          const std::string& category_id,
                          const ImmutableUpdatesInfo& updates_info,
                          storage::rocksdb::NativeWriteBatch&);

  void deleteGenesisBlock(BlockId block_id,
                          const std::string& category_id,
                          const KeyValueUpdatesInfo& updates_info,
                          storage::rocksdb::NativeWriteBatch&);

  void deleteGenesisBlock(BlockId block_id,
                          const std::string& category_id,
                          const MerkleUpdatesInfo& updates_info,
                          storage::rocksdb::NativeWriteBatch&);

  void deleteLastReachableBlock(BlockId block_id,
                                const std::string& category_id,
                                const ImmutableUpdatesInfo& updates_info,
                                storage::rocksdb::NativeWriteBatch&);

  void deleteLastReachableBlock(BlockId block_id,
                                const std::string& category_id,
                                const KeyValueUpdatesInfo& updates_info,
                                storage::rocksdb::NativeWriteBatch&);

  void deleteLastReachableBlock(BlockId block_id,
                                const std::string& category_id,
                                const MerkleUpdatesInfo& updates_info,
                                storage::rocksdb::NativeWriteBatch&);

  // Update per category
  MerkleUpdatesInfo handleCategoryUpdates(BlockId block_id,
                                          const std::string& category_id,
                                          MerkleUpdatesData&& updates,
                                          concord::storage::rocksdb::NativeWriteBatch& write_batch);

  KeyValueUpdatesInfo handleCategoryUpdates(BlockId block_id,
                                            const std::string& category_id,
                                            KeyValueUpdatesData&& updates,
                                            concord::storage::rocksdb::NativeWriteBatch& write_batch);
  ImmutableUpdatesInfo handleCategoryUpdates(BlockId block_id,
                                             const std::string& category_id,
                                             ImmutableUpdatesData&& updates,
                                             concord::storage::rocksdb::NativeWriteBatch& write_batch);

  // Members
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  detail::Blockchain block_chain_;
  detail::Blockchain::StateTransfer state_transfer_block_chain_;
};

}  // namespace concord::kvbc::categorization
