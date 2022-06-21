// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "v4blockchain/detail/blockchain.h"
#include "Logger.hpp"
#include <algorithm>

namespace concord::kvbc::v4blockchain::detail {

std::atomic<BlockId> Blockchain::global_genesis_block_id = INVALID_BLOCK_ID;

Blockchain::Blockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client)
    : native_client_{native_client} {
  if (native_client->createColumnFamilyIfNotExisting(v4blockchain::detail::BLOCKS_CF)) {
    LOG_INFO(V4_BLOCK_LOG, "Created [" << v4blockchain::detail::BLOCKS_CF << "] column family for the main blockchain");
  }
  auto last_reachable_block_id = loadLastReachableBlockId();
  if (last_reachable_block_id) {
    last_reachable_block_id_ = last_reachable_block_id.value();
    LOG_INFO(V4_BLOCK_LOG, "Last reachable block was loaded from storage " << last_reachable_block_id_);
  }
  auto genesis_blockId = loadGenesisBlockId();
  if (genesis_blockId) {
    setGenesisBlockId(genesis_blockId.value());
    LOG_INFO(V4_BLOCK_LOG, "Genesis block was loaded from storage " << genesis_block_id_);
  }
}

/*
1 - define the id of the new block
2 - calculate the digest of the previous block
3 - create the block,  add the updates and digest to it.
4 - put it in the write batch.
5 - save the block buffer for the next block digest calculation.
Note - the increment for the  last_reachable_block_id_ is done after the write batch was written to storage.
*/
BlockId Blockchain::addBlock(const concord::kvbc::categorization::Updates& category_updates,
                             storage::rocksdb::NativeWriteBatch& wb) {
  v4blockchain::detail::Block block;
  block.addUpdates(category_updates);
  return addBlock(block, wb);
}

BlockId Blockchain::addBlock(v4blockchain::detail::Block& block, storage::rocksdb::NativeWriteBatch& wb) {
  BlockId id = last_reachable_block_id_ + 1;
  // If future from the previous add exist get its value
  concord::util::digest::BlockDigest digest;
  if (future_digest_) {
    ++from_future;
    digest = future_digest_->get();
  } else {
    ++from_storage;
    digest = calculateBlockDigest(last_reachable_block_id_);
  }
  auto blockKey = generateKey(id);
  block.addDigest(digest);
  wb.put(v4blockchain::detail::BLOCKS_CF, blockKey, block.getBuffer());
  future_digest_ = thread_pool_.async(
      [](BlockId id, v4blockchain::detail::Block&& block) { return block.calculateDigest(id); }, id, std::move(block));
  return id;
}

// Delete up to until not including until,
// returns the last block id that was deleted.
BlockId Blockchain::deleteBlocksUntil(BlockId until) {
  ConcordAssertGT(genesis_block_id_, INVALID_BLOCK_ID);
  ConcordAssertLT(genesis_block_id_, until);
  // We have a single block on the chain
  if (last_reachable_block_id_ == genesis_block_id_) {
    LOG_WARN(V4_BLOCK_LOG, "Deleting the last block in the blockchain is not supported " << last_reachable_block_id_);
    return genesis_block_id_ - 1;
  }
  // We don't want to erase all the blockchain
  const auto last_deleted_block = std::min(last_reachable_block_id_.load() - 1, until - 1);
  auto write_batch = native_client_->getBatch();

  // Removes the database entries in the range ["begin_key", "end_key"), i.e.,
  // including "begin_key" and excluding "end_key" --> this is why end is (last_deleted_block + 1)
  write_batch.delRange(
      v4blockchain::detail::BLOCKS_CF, generateKey(genesis_block_id_), generateKey(last_deleted_block + 1));
  native_client_->write(std::move(write_batch));
  auto blocks_deleted = (last_deleted_block - genesis_block_id_) + 1;
  setGenesisBlockId(last_deleted_block + 1);

  LOG_INFO(V4_BLOCK_LOG, "Deleted " << blocks_deleted << " blocks, new genesis is " << genesis_block_id_);
  return last_deleted_block;
}

void Blockchain::deleteGenesisBlock() {
  ConcordAssertGT(genesis_block_id_, INVALID_BLOCK_ID);
  ConcordAssertLT(genesis_block_id_, last_reachable_block_id_);
  auto write_batch = native_client_->getBatch();
  deleteBlock(genesis_block_id_, write_batch);
  native_client_->write(std::move(write_batch));
  setGenesisBlockId(genesis_block_id_ + 1);
  LOG_INFO(V4_BLOCK_LOG, "Deleted genesis, new genesis is " << genesis_block_id_);
}

void Blockchain::deleteLastReachableBlock(storage::rocksdb::NativeWriteBatch& write_batch) {
  ConcordAssertGT(last_reachable_block_id_, INVALID_BLOCK_ID);
  ConcordAssertLT(genesis_block_id_, last_reachable_block_id_);
  deleteBlock(last_reachable_block_id_, write_batch);
}

concord::util::digest::BlockDigest Blockchain::calculateBlockDigest(concord::kvbc::BlockId id) const {
  if (id < concord::kvbc::INITIAL_GENESIS_BLOCK_ID) {
    concord::util::digest::BlockDigest empty_digest;
    empty_digest.fill(0);
    return empty_digest;
  }
  auto block_str = getBlockData(id);
  ConcordAssert(block_str.has_value());
  return v4blockchain::detail::Block::calculateDigest(id, block_str->c_str(), block_str->size());
}

concord::util::digest::BlockDigest Blockchain::getBlockParentDigest(concord::kvbc::BlockId id) const {
  auto block_str = getBlockData(id);
  ConcordAssert(block_str.has_value());
  return v4blockchain::detail::Block{*block_str}.parentDigest();
}

std::optional<std::string> Blockchain::getBlockData(concord::kvbc::BlockId id) const {
  auto blockKey = generateKey(id);
  return native_client_->get(v4blockchain::detail::BLOCKS_CF, blockKey);
}

std::optional<categorization::Updates> Blockchain::getBlockUpdates(BlockId id) const {
  auto block_buffer = getBlockData(id);
  if (!block_buffer) return std::nullopt;
  auto block = v4blockchain::detail::Block(*block_buffer);
  return block.getUpdates();
}

void Blockchain::multiGetBlockData(const std::vector<BlockId>& block_ids,
                                   std::unordered_map<BlockId, std::optional<std::string>>& values) const {
  values.clear();
  std::vector<std::string> block_keys(block_ids.size());
  std::transform(block_ids.cbegin(), block_ids.cend(), block_keys.begin(), [](BlockId bid) {
    return Blockchain::generateKey(bid);
  });
  std::vector<::rocksdb::PinnableSlice> slices(block_ids.size());
  std::vector<::rocksdb::Status> statuses(block_ids.size());
  native_client_->multiGet(v4blockchain::detail::BLOCKS_CF, block_keys, slices, statuses);
  for (auto i = 0ull; i < slices.size(); ++i) {
    const auto& status = statuses[i];
    const auto& slice = slices[i];
    const auto& block_id = block_ids[i];
    if (status.ok()) {
      if (!values.try_emplace(block_id, slice.ToString()).second) {
        throw std::logic_error{std::string("Duplicate block ids should not be sent: ") + block_keys[i]};
      }
    } else if (status.IsNotFound()) {
      if (!values.try_emplace(block_id, std::nullopt).second) {
        throw std::logic_error{std::string("Duplicate block ids should not be sent: ") + block_keys[i]};
      }
    } else {
      // Should never happen.
      throw std::runtime_error{"BLOCK_CF multiGet() failure: " + status.ToString()};
    }
  }
}

void Blockchain::multiGetBlockUpdates(
    std::vector<BlockId> block_ids, std::unordered_map<BlockId, std::optional<categorization::Updates>>& values) const {
  auto uqid = std::unique(block_ids.begin(), block_ids.end());
  block_ids.resize(std::distance(block_ids.begin(), uqid));
  std::unordered_map<BlockId, std::optional<std::string>> updates;
  multiGetBlockData(block_ids, updates);
  ConcordAssertEQ(block_ids.size(), updates.size());
  for (const auto& block_buffer : updates) {
    if (block_buffer.second) {
      auto block = v4blockchain::detail::Block(*(block_buffer.second));
      values.emplace(block_buffer.first, block.getUpdates());
    } else {
      values.emplace(block_buffer.first, std::nullopt);
    }
  }
}

// get the closest key to MAX_BLOCK_ID
std::optional<BlockId> Blockchain::loadLastReachableBlockId() {
  auto itr = native_client_->getIterator(v4blockchain::detail::BLOCKS_CF);
  itr.seekAtMost(generateKey(MAX_BLOCK_ID));
  if (!itr) {
    return std::optional<BlockId>{};
  }
  return concordUtils::fromBigEndianBuffer<BlockId>(itr.keyView().data());
}

// get the closest key to INITIAL_GENESIS_BLOCK_ID
std::optional<BlockId> Blockchain::loadGenesisBlockId() {
  auto itr = native_client_->getIterator(detail::BLOCKS_CF);
  itr.seekAtLeast(generateKey(concord::kvbc::INITIAL_GENESIS_BLOCK_ID));
  if (!itr) {
    return std::optional<BlockId>{};
  }
  return concordUtils::fromBigEndianBuffer<BlockId>(itr.keyView().data());
}

void Blockchain::setBlockId(BlockId id) {
  setLastReachable(id);
  if (genesis_block_id_ == INVALID_BLOCK_ID) setGenesisBlockId(id);
}

bool Blockchain::hasBlock(BlockId block_id) const {
  if (block_id > last_reachable_block_id_) return false;
  return native_client_->getSlice(v4blockchain::detail::BLOCKS_CF, generateKey(block_id)).has_value();
}

}  // namespace concord::kvbc::v4blockchain::detail
