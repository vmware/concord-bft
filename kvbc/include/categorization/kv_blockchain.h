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

#include "kv_types.hpp"

using namespace concord::storage::rocksdb;

namespace concord::kvbc::categorization {

struct Block {
  void add(const std::string& category_id, std::variant<MerkleUpdatesInfo, KeyValueUpdatesInfo> updates_info) {}
  void add(SharedKeyValueUpdatesInfo updates_info) {}
};
class KeyValueBlockchain {
 public:
  KeyValueBlockchain() : native_client_(NativeClient::newClient("/tmp", false, NativeClient::DefaultOptions{})) {}
  // 1) Defines a new block
  // 2) calls per cateogry with its updates
  // 3) inserts the updates KV to the DB updates set per column family
  // 4) add the category block data into the new block
  BlockId addBlock(Updates& updates) {
    // Use new client batch and column families
    auto write_batch = native_client_->getBatch();
    // const auto addedBlockId = getLastReachableBlockId() + 1;
    // auto parentBlockDigestFuture = computeParentBlockDigest(blockId);
    Block new_block;
    // Per category updates
    for (auto& [category_id, update] : updates.getCategoryUpdate()) {
      // https://stackoverflow.com/questions/46114214/lambda-implicit-capture-fails-with-variable-declared-from-structured-binding
      std::visit(
          [&new_block, category_id = category_id, write_batch, this](auto& update) {
            auto block_updates = handleCategoryUpdates(category_id, update, write_batch);
            new_block.add(category_id, block_updates);
          },
          update);
    }
    if (updates.getSharedUpdates().has_value()) {
      auto block_updates = handleCategoryUpdates(updates.getSharedUpdates().value(), write_batch);
      new_block.add(block_updates);
    }
    return BlockId{8};
    // newBlock.parentDigest = parentBlockDigestFuture.get();
    // // E.L blocks in new column Family
    // write_batch.put("blocks",DBKeyManipulator::genBlockDbKey(blockId),serizilize(newBlock));
    // native_client_->write(write_batch);
  }

 private:
  std::variant<MerkleUpdatesInfo, KeyValueUpdatesInfo> handleCategoryUpdates(
      const std::string& category_id,
      std::variant<MerkleUpdatesData, KeyValueUpdatesData> updates_info,
      const WriteBatch& write_batch) {
    return MerkleUpdatesInfo{};
  }

  SharedKeyValueUpdatesInfo handleCategoryUpdates(SharedUpdatesData& updates_info, const WriteBatch& write_batch) {
    return SharedKeyValueUpdatesInfo{};
  }

  std::shared_ptr<NativeClient> native_client_;
};

}  // namespace concord::kvbc::categorization
