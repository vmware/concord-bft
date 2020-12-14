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

#include "categorization/kv_blockchain.h"

namespace concord::kvbc::categorization {

// 1) Defines a new block
// 2) calls per cateogry with its updates
// 3) inserts the updates KV to the DB updates set per column family
// 4) add the category block data into the new block
BlockId KeyValueBlockchain::addBlock(Updates&& updates) {
  // Use new client batch and column families
  auto write_batch = native_client_->getBatch();
  Block new_block{block_chain_.getLastReachableBlockId() + 1};
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
    auto block_updates = handleCategoryUpdates(new_block.id(), std::move(updates.shared_update_.value()), write_batch);
    new_block.add(std::move(block_updates));
  }
  // newBlock.parentDigest = parentBlockDigestFuture.get();
  block_chain_.addBlock(new_block, write_batch);
  write_batch.put(detail::BLOCKS_CF, Block::generateKey(new_block.id()), Block::serialize(new_block));
  native_client_->write(std::move(write_batch));
  block_chain_.setAddedBlockId(new_block.id());
  return new_block.id();
}

/////////////////////// Delete block ///////////////////////
bool KeyValueBlockchain::deleteBlock(const BlockId& block_id) {
  // Deleting blocks that don't exist is not an error.
  if (block_id == 0 || block_id < block_chain_.getGenesisBlockId()) {
    // E.L log
    return false;
  }

  // If block id is bigger than what we have
  const auto latest_block_id = block_chain_.getLatestBlockId(state_transfer_block_chain_);
  if (latest_block_id == 0 || block_id > latest_block_id) {
    return false;
  }

  const auto last_reachable_block_id = block_chain_.getLastReachableBlockId();

  // Block id belongs to the ST chain
  if (block_id > last_reachable_block_id) {
    deleteStateTransferBlock(block_id);
    return true;
  }

  const auto genesis_block_id = block_chain_.getGenesisBlockId();
  if (block_id == last_reachable_block_id && block_id == genesis_block_id) {
    throw std::logic_error{"Deleting the only block in the system is not supported"};
  } else if (block_id == last_reachable_block_id) {
    deleteLastReachableBlock();
    return true;
  } else if (block_id == genesis_block_id) {
    deleteGenesisBlock();
    return true;
  } else {
    throw std::invalid_argument{"Cannot delete blocks in the middle of the blockchain"};
  }
}

void KeyValueBlockchain::deleteStateTransferBlock(const BlockId block_id) {
  auto write_batch = native_client_->getBatch();
  state_transfer_block_chain_.deleteBlock(block_id, write_batch);
  native_client_->write(std::move(write_batch));
  if (!state_transfer_block_chain_.getLastBlockId()) {
    state_transfer_block_chain_.loadLastBlockId();
  }
}

// 1 - Get genesis block form DB.
// 2 - iterate over the update_info and calls the corresponding deleteGenesisBlock
// 3 - perform the delete
// 4 - increment the genesis block id.
void KeyValueBlockchain::deleteGenesisBlock() {
  // We assume there are blocks in the system.
  auto genesis_id = block_chain_.getGenesisBlockId();
  ConcordAssertGE(genesis_id, INITIAL_GENESIS_BLOCK_ID);
  // And we assume this is not the only block in the blockchain. That excludes ST temporary blocks as they are not yet
  // part of the blockchain.
  ConcordAssertNE(genesis_id, block_chain_.getLastReachableBlockId());
  auto write_batch = native_client_->getBatch();

  // Get block node from storage
  auto block = block_chain_.getBlock(genesis_id);
  if (!block) {
    const auto msg = "Failed to get block node for block ID = " + std::to_string(genesis_id);
    throw std::runtime_error{msg};
  }

  block_chain_.deleteBlock(genesis_id, write_batch);

  // Iterate over groups and call corresponding deleteGenesisBlock,
  // Each group is responsible to fill its deltetes to the batch
  for (auto&& [category_id, update_info] : (*block).data.categories_updates_info) {
    std::visit([&genesis_id, category_id = category_id, &write_batch, this](
                   const auto& update_info) { deleteGenesisBlock(genesis_id, category_id, update_info, write_batch); },
               update_info);
  }
  if ((*block).data.shared_updates_info.has_value()) {
    deleteGenesisBlock(genesis_id, (*block).data.shared_updates_info.value(), write_batch);
  }

  native_client_->write(std::move(write_batch));
  // Increment the genesis block ID cache.
  block_chain_.setGenesisBlockId(genesis_id + 1);
}

// 1 - Get last id block form DB.
// 2 - iterate over the update_info and calls the corresponding deleteLastReachableBlock
// 3 - perform the delete
// 4 - increment the genesis block id.
void KeyValueBlockchain::deleteLastReachableBlock() {
  auto last_id = block_chain_.getLastReachableBlockId();
  if (last_id == 0) return;

  auto write_batch = native_client_->getBatch();
  // Get block node from storage
  auto block = block_chain_.getBlock(last_id);
  if (!block) {
    const auto msg = "Failed to get block node for block ID = " + std::to_string(last_id);
    throw std::runtime_error{msg};
  }

  block_chain_.deleteBlock(last_id, write_batch);

  // Iterate over groups and call corresponding deleteGenesisBlock,
  // Each group is responsible to fill its deltetes to the batch
  for (auto&& [category_id, update_info] : (*block).data.categories_updates_info) {
    std::visit(
        [&last_id, category_id = category_id, &write_batch, this](const auto& update_info) {
          deleteLastReachableBlock(last_id, category_id, update_info, write_batch);
        },
        update_info);
  }
  if ((*block).data.shared_updates_info.has_value()) {
    deleteLastReachableBlock(last_id, (*block).data.shared_updates_info.value(), write_batch);
  }

  native_client_->write(std::move(write_batch));

  // Since we allow deletion of the only block left as last reachable (due to replica state sync), reflect that in the
  // genesis block ID cache.
  auto genesis_id = block_chain_.getGenesisBlockId();
  if (last_id == genesis_id) {
    block_chain_.setGenesisBlockId(genesis_id - 1);
  }

  // Decrement the last reachable block ID cache.
  block_chain_.setLastReachableBlockId(last_id - 1);
}

// Deletes per category
void KeyValueBlockchain::deleteGenesisBlock(BlockId block_id,
                                            const SharedKeyValueUpdatesInfo& updates_info,
                                            storage::rocksdb::NativeWriteBatch&) {}

void KeyValueBlockchain::deleteGenesisBlock(BlockId block_id,
                                            const std::string& category_id,
                                            const KeyValueUpdatesInfo& updates_info,
                                            storage::rocksdb::NativeWriteBatch&) {}

void KeyValueBlockchain::deleteGenesisBlock(BlockId block_id,
                                            const std::string& category_id,
                                            const MerkleUpdatesInfo& updates_info,
                                            storage::rocksdb::NativeWriteBatch&) {}

void KeyValueBlockchain::deleteLastReachableBlock(BlockId block_id,
                                                  const SharedKeyValueUpdatesInfo& updates_info,
                                                  storage::rocksdb::NativeWriteBatch&) {}

void KeyValueBlockchain::deleteLastReachableBlock(BlockId block_id,
                                                  const std::string& category_id,
                                                  const KeyValueUpdatesInfo& updates_info,
                                                  storage::rocksdb::NativeWriteBatch&) {}

void KeyValueBlockchain::deleteLastReachableBlock(BlockId block_id,
                                                  const std::string& category_id,
                                                  const MerkleUpdatesInfo& updates_info,
                                                  storage::rocksdb::NativeWriteBatch&) {}

// Updates per category

MerkleUpdatesInfo KeyValueBlockchain::handleCategoryUpdates(BlockId block_id,
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

KeyValueUpdatesInfo KeyValueBlockchain::handleCategoryUpdates(
    BlockId block_id,
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

SharedKeyValueUpdatesInfo KeyValueBlockchain::handleCategoryUpdates(
    BlockId block_id, SharedKeyValueUpdatesData&& updates, concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  SharedKeyValueUpdatesInfo skvui;
  for (auto& [k, v] : updates.kv) {
    (void)v;
    skvui.keys[k] = SharedKeyData{v.category_ids};
  }
  return skvui;
}

}  // namespace concord::kvbc::categorization
