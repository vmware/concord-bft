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

#include "kv_types.hpp"

namespace concord::kvbc::categorization {

class KeyValueBlockchain {
 public:
  KeyValueBlockchain()
      : native_client_(concord::storage::rocksdb::NativeClient::newClient(
            "/tmp", false, concord::storage::rocksdb::NativeClient::DefaultOptions{})) {}
  KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client)
      : native_client_(native_client) {}
  // 1) Defines a new block
  // 2) calls per cateogry with its updates
  // 3) inserts the updates KV to the DB updates set per column family
  // 4) add the category block data into the new block
  BlockId addBlock(Updates&& updates) {
    // Use new client batch and column families
    auto write_batch = native_client_->getBatch();
    Block new_block{getLastReachableBlockId() + 1};
    // auto parentBlockDigestFuture = computeParentBlockDigest(new_block.ID( ));
    // Per category updates
    for (auto&& [category_id, update] : updates.category_updates_) {
      // https://stackoverflow.com/questions/46114214/lambda-implicit-capture-fails-with-variable-declared-from-structured-binding
      std::visit(
          [&new_block, category_id = category_id, &write_batch, this](auto& update) {
            auto block_updates = handleCategoryUpdates(new_block.id(), category_id, std::move(update), write_batch);
            new_block.add(category_id, std::move(block_updates));
          },
          update);
    }
    if (updates.shared_update_.has_value()) {
      auto block_updates =
          handleCategoryUpdates(new_block.id(), std::move(updates.shared_update_.value()), write_batch);
      new_block.add(std::move(block_updates));
    }
    // newBlock.parentDigest = parentBlockDigestFuture.get();
    // // E.L blocks in new column Family
    write_batch.put(detail::BLOCKS_CF, Block::generateKey(new_block.id()), Block::serialize(new_block));
    native_client_->write(std::move(write_batch));
    return lastReachableBlockId_ = new_block.id();
  }

 private:
  MerkleUpdatesInfo handleCategoryUpdates(BlockId block_id,
                                          const std::string& category_id,
                                          MerkleUpdatesData&& updates,
                                          concord::storage::rocksdb::NativeWriteBatch& write_batch) {
    MerkleUpdatesInfo mui;
    for (auto& [k, v] : updates.kv) {
      (void)v;
      mui.keys[k] = MerkleKeyFlag{false};
    }
    for (auto& k : updates.deletes) {
      mui.keys[k] = MerkleKeyFlag{true};
    }
    return mui;
  }

  KeyValueUpdatesInfo handleCategoryUpdates(BlockId block_id,
                                            const std::string& category_id,
                                            KeyValueUpdatesData&& updates,
                                            concord::storage::rocksdb::NativeWriteBatch& write_batch) {
    KeyValueUpdatesInfo kvui;
    for (auto& [k, v] : updates.kv) {
      (void)v;
      kvui.keys[k] = KVKeyFlag{false, v.stale_on_update};
    }
    for (auto& k : updates.deletes) {
      kvui.keys[k] = KVKeyFlag{true, false};
    }
    return kvui;
  }

  SharedKeyValueUpdatesInfo handleCategoryUpdates(BlockId block_id,
                                                  SharedKeyValueUpdatesData&& updates,
                                                  concord::storage::rocksdb::NativeWriteBatch& write_batch) {
    SharedKeyValueUpdatesInfo skvui;
    for (auto& [k, v] : updates.kv) {
      (void)v;
      skvui.keys[k] = SharedKeyData{v.category_ids};
    }
    return skvui;
  }

  BlockId getLastReachableBlockId() { return lastReachableBlockId_; }
  // Members
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  BlockId lastReachableBlockId_{0};
};

}  // namespace concord::kvbc::categorization
