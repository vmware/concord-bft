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

#include <variant>

#include "v4blockchain/v4_blockchain.h"
#include "v4blockchain/detail/detail.h"
#include "categorization/base_types.h"
#include "categorization/db_categories.h"
#include "kvbc_key_types.hpp"
#include "block_metadata.hpp"
#include "throughput.hpp"

namespace concord::kvbc::v4blockchain {

using namespace concord::kvbc;

KeyValueBlockchain::KeyValueBlockchain(
    const std::shared_ptr<concord::storage::rocksdb::NativeClient> &native_client,
    bool link_st_chain,
    const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>> &category_types)
    : native_client_{native_client},
      block_chain_{native_client_},
      state_transfer_chain_{native_client_},
      latest_keys_{native_client_, category_types, [&]() { return block_chain_.getGenesisBlockId(); }} {
  if (!link_st_chain || state_transfer_chain_.getLastBlockId() == 0) return;
  // Make sure that if linkSTChainFrom() has been interrupted (e.g. a crash or an abnormal shutdown), all DBAdapter
  // methods will return the correct values. For example, if state transfer had completed and linkSTChainFrom() was
  // interrupted, getLatestBlockId() should be equal to getLastReachableBlockId() on the next startup. Another example
  // is getValue() that returns keys from the blockchain only and ignores keys in the temporary state
  // transfer chain.
  LOG_INFO(V4_BLOCK_LOG, "Try to link ST temporary chain, this might take some time...");
  auto old_last_reachable_block_id = getLastReachableBlockId();
  linkSTChain();
  auto new_last_reachable_block_id = getLastReachableBlockId();
  if (new_last_reachable_block_id > old_last_reachable_block_id) {
    LOG_INFO(V4_BLOCK_LOG,
             "Done linking ST temporary chain:" << KVLOG(old_last_reachable_block_id, new_last_reachable_block_id));
  }
}

//////////////////////////// ADDER////////////////////////////////////////////
/*
  1 - check if we can perform GC on latest CF
  2 - add the block to the blocks CF
  3 - add the keys to the latest CF
  4 - atomic write to storage
  5 - increment last reachable block.
*/
BlockId KeyValueBlockchain::add(categorization::Updates &&updates) {
  auto scoped = v4blockchain::detail::ScopedDuration{"Add block"};
  // Should be performed before we add the block with the current Updates.
  auto sequence_number = markHistoryForGarbageCollectionIfNeeded(updates);
  auto write_batch = native_client_->getBatch();
  // addGenesisBlockKey(updates);
  auto block_id = add(updates, write_batch);

  native_client_->write(std::move(write_batch));
  block_chain_.setBlockId(block_id);
  if (sequence_number > 0) setLastBlockSequenceNumber(sequence_number);
  return block_id;
}

BlockId KeyValueBlockchain::add(const categorization::Updates &updates,
                                storage::rocksdb::NativeWriteBatch &write_batch) {
  BlockId block_id{};
  {
    auto scoped2 = v4blockchain::detail::ScopedDuration{"Add block to blockchain"};
    block_id = block_chain_.addBlock(updates, write_batch);
  }
  {
    auto scoped3 = v4blockchain::detail::ScopedDuration{"Add block to latest"};
    latest_keys_.addBlockKeys(updates, block_id, write_batch);
  }
  return block_id;
}

//////////////////////////// DELETER////////////////////////////////////////////

BlockId KeyValueBlockchain::deleteBlocksUntil(BlockId until) {
  auto scoped = v4blockchain::detail::ScopedDuration{"deleteBlocksUntil"};
  return block_chain_.deleteBlocksUntil(until);
}

void KeyValueBlockchain::deleteGenesisBlock() {
  auto scoped = v4blockchain::detail::ScopedDuration{"deleteGenesisBlock"};
  block_chain_.deleteGenesisBlock();
}

void KeyValueBlockchain::deleteLastReachableBlock() {
  // validate conditions
  auto last_reachable_id = block_chain_.getLastReachable();
  auto genesis_id = block_chain_.getGenesisBlockId();
  ConcordAssertLT(genesis_id, last_reachable_id);
  // get block updates
  auto write_batch = native_client_->getBatch();
  auto updates = block_chain_.getBlockUpdates(last_reachable_id);
  ConcordAssert(updates.has_value());
  // revert from latest keys
  latest_keys_.revertLastBlockKeys(*updates, last_reachable_id, write_batch);
  // delete from blockchain
  block_chain_.deleteLastReachableBlock(write_batch);
  // atomically commit changes
  native_client_->write(std::move(write_batch));
  block_chain_.setBlockId(--last_reachable_id);
  LOG_INFO(V4_BLOCK_LOG, "Deleted last reachable, new value is " << last_reachable_id);
}

/*
  If the bft sequence number for this updates is bigger than the last, it means that the last sequence number
  was commited, and it's safe to mark the version of the last block that corresponds to that sequence number as safe
  for rocksdb garbage collection.
  https://github.com/facebook/rocksdb/wiki/User-defined-Timestamp-(Experimental)#compaction-and-garbage-collection-gc
  - if last_block_sn_ is nullopt we're probably on first start.
  - we don't want to trim when checkpoint is in process.
*/
uint64_t KeyValueBlockchain::markHistoryForGarbageCollectionIfNeeded(const categorization::Updates &updates) {
  if (checkpointInProcess_) return 0;
  auto sequence_number = getBlockSequenceNumber(updates);
  // Optional not set yet or same sn
  if (!last_block_sn_ || *last_block_sn_ == sequence_number) return sequence_number;
  // This is abnormal
  if (sequence_number < *last_block_sn_) {
    LOG_WARN(
        V4_BLOCK_LOG,
        "Sequence number for trim history " << sequence_number << " is lower than previous value " << *last_block_sn_);
    return 0;
  }
  ++gc_counter;
  auto block_id = block_chain_.getLastReachable();
  latest_keys_.trimHistoryUntil(block_id);
  if (sequence_number % 100 == 0) {
    LOG_INFO(V4_BLOCK_LOG, "History was marked for trim up to block " << block_id);
  }
  return sequence_number;
}

uint64_t KeyValueBlockchain::getBlockSequenceNumber(const categorization::Updates &updates) const {
  static std::string key = std::string(1, IBlockMetadata::kBlockMetadataKey);
  const auto &input = updates.categoryUpdates();
  if (input.kv.count(categorization::kConcordInternalCategoryId) == 0) {
    return 0;
  }
  const auto &ver_input =
      std::get<categorization::VersionedInput>(input.kv.at(categorization::kConcordInternalCategoryId));
  if (ver_input.kv.count(key) == 0) {
    return 0;
  }
  return BlockMetadata::getSequenceNum(ver_input.kv.at(key).data);
}

void KeyValueBlockchain::addGenesisBlockKey(categorization::Updates &updates) const {
  const auto stale_on_update = true;
  updates.addCategoryIfNotExisting<categorization::VersionedInput>(categorization::kConcordInternalCategoryId);
  updates.appendKeyValue<categorization::VersionedUpdates>(
      categorization::kConcordInternalCategoryId,
      std::string{keyTypes::genesis_block_key},
      categorization::VersionedUpdates::ValueType{
          concordUtils::toBigEndianStringBuffer(block_chain_.getGenesisBlockId()), stale_on_update});
}

///////////// STATE TRANSFER////////////////////////
bool KeyValueBlockchain::hasBlock(BlockId block_id) const {
  const auto last_reachable_block = block_chain_.getLastReachable();
  if (block_id > last_reachable_block) {
    return state_transfer_chain_.hasBlock(block_id);
  }
  return block_chain_.hasBlock(block_id);
}

std::optional<std::string> KeyValueBlockchain::getBlockData(const BlockId &block_id) const {
  const auto last_reachable_block = getLastReachableBlockId();
  // Try to take it from the ST chain
  if (block_id > last_reachable_block) {
    return state_transfer_chain_.getBlockData(block_id);
  }
  // Try from the blockchain itself
  return block_chain_.getBlockData(block_id);
}

std::optional<BlockId> KeyValueBlockchain::getLastStatetransferBlockId() const {
  if (state_transfer_chain_.getLastBlockId() == 0) return std::nullopt;
  return state_transfer_chain_.getLastBlockId();
}

concord::util::digest::BlockDigest KeyValueBlockchain::parentDigest(BlockId block_id) const {
  const auto last_reachable_block = getLastReachableBlockId();
  if (block_id > last_reachable_block) {
    return state_transfer_chain_.getBlockParentDigest(block_id);
  }
  if (block_id < getGenesisBlockId()) {
    LOG_ERROR(V4_BLOCK_LOG,
              "Trying to get digest from block " << block_id << " while genesis is " << getGenesisBlockId());
    concord::util::digest::BlockDigest empty_digest;
    empty_digest.fill(0);
    return empty_digest;
  }
  return block_chain_.getBlockParentDigest(block_id);
}

void KeyValueBlockchain::addBlockToSTChain(const BlockId &block_id,
                                           const char *block,
                                           const uint32_t block_size,
                                           bool last_block) {
  const auto last_reachable_block = getLastReachableBlockId();
  if (block_id <= last_reachable_block) {
    const auto msg = "Cannot add an existing block ID " + std::to_string(block_id);
    throw std::invalid_argument{msg};
  }

  if (state_transfer_chain_.hasBlock(block_id)) {
    auto existing_block = state_transfer_chain_.getBlockData(block_id);
    ConcordAssert(existing_block.has_value());
    auto view = std::string_view{block, block_size};
    if (view != *existing_block) {
      LOG_ERROR(V4_BLOCK_LOG,
                "found existing (and different) block ID[" << block_id << "] when receiving from state transfer");

      // E.L I think it's dangerous to delete the block and there is no value in doing so
      // kvbc_->deleteBlock(blockId);
      throw std::runtime_error(
          __PRETTY_FUNCTION__ +
          std::string("found existing (and different) block when receiving state transfer, block ID: ") +
          std::to_string(block_id));
    }
    return;
  }
  LOG_DEBUG(V4_BLOCK_LOG, "Adding ST block " << block_id);
  state_transfer_chain_.addBlock(block_id, block, block_size);
  if (last_block) {
    try {
      LOG_INFO(V4_BLOCK_LOG, "ST last block added " << block_id << " linking to blockchain");
      linkSTChain();
    } catch (const std::exception &e) {
      LOG_FATAL(V4_BLOCK_LOG,
                "Aborting due to failure to link chains after block has been added, reason: " << e.what());
      std::terminate();
    } catch (...) {
      LOG_FATAL(V4_BLOCK_LOG, "Aborting due to failure to link chains after block has been added");
      std::terminate();
    }
  }
}

void KeyValueBlockchain::linkSTChain() {
  BlockId block_id = getLastReachableBlockId() + 1;
  const auto last_block_id = state_transfer_chain_.getLastBlockId();
  if (last_block_id == 0) return;

  for (auto i = block_id; i <= last_block_id; ++i) {
    auto block = state_transfer_chain_.getBlock(i);
    if (!block) {
      LOG_INFO(V4_BLOCK_LOG, "Block " << i << " wasn't found, started from block " << block_id);
      return;
    }
    auto updates = block->getUpdates();
    writeSTLinkTransaction(i, updates);
  }
  // Linking has fully completed and we should not have any more ST temporary blocks left. Therefore, make sure we don't
  // have any value for the latest ST temporary block ID cache.
  LOG_INFO(V4_BLOCK_LOG, "Fully Linked ST from " << block_id << " to " << last_block_id);
  state_transfer_chain_.resetChain();
}

void KeyValueBlockchain::pruneOnSTLink(const categorization::Updates &updates) {
  auto cat_it = updates.categoryUpdates().kv.find(categorization::kConcordInternalCategoryId);
  if (cat_it == updates.categoryUpdates().kv.cend()) {
    return;
  }
  const auto &internal_kvs = std::get<categorization::VersionedInput>(cat_it->second).kv;
  auto key_it = internal_kvs.find(keyTypes::genesis_block_key);
  if (key_it != internal_kvs.cend()) {
    const auto block_genesis_id = concordUtils::fromBigEndianBuffer<BlockId>(key_it->second.data.data());
    while (getGenesisBlockId() >= INITIAL_GENESIS_BLOCK_ID && getGenesisBlockId() < getLastReachableBlockId() &&
           block_genesis_id > getGenesisBlockId()) {
      deleteGenesisBlock();
    }
  }
}

// Atomic delete from state transfer and add to blockchain
void KeyValueBlockchain::writeSTLinkTransaction(const BlockId block_id, const categorization::Updates &updates) {
  auto sequence_number = markHistoryForGarbageCollectionIfNeeded(updates);
  auto write_batch = native_client_->getBatch();
  state_transfer_chain_.deleteBlock(block_id, write_batch);
  auto new_block_id = add(updates, write_batch);
  native_client_->write(std::move(write_batch));
  block_chain_.setBlockId(new_block_id);
  pruneOnSTLink(updates);
  if (sequence_number > 0) setLastBlockSequenceNumber(sequence_number);
}

size_t KeyValueBlockchain::linkUntilBlockId(BlockId until_block_id) {
  const auto from_block_id = getLastReachableBlockId() + 1;
  ConcordAssertLE(from_block_id, until_block_id);

  static constexpr uint64_t report_thresh{1000};
  static uint64_t report_counter{};

  concord::util::DurationTracker<std::chrono::milliseconds> link_duration("link_duration", true);
  BlockId last_added = 0;
  for (auto i = from_block_id; i <= until_block_id; ++i) {
    auto block = state_transfer_chain_.getBlock(i);

    if (!block) {
      break;
    }
    last_added = i;
    // First prune and then link the block to the chain. Rationale is that this will preserve the same order of block
    // deletes relative to block adds on source and destination replicas.
    auto updates = block->getUpdates();
    writeSTLinkTransaction(i, updates);
    if ((++report_counter % report_thresh) == 0) {
      auto elapsed_time_ms = link_duration.totalDuration();
      uint64_t blocks_linked_per_sec{};
      uint64_t blocks_left_to_link{};
      uint64_t estimated_time_left_sec{};
      if (elapsed_time_ms > 0) {
        blocks_linked_per_sec = (((i - from_block_id + 1) * 1000) / (elapsed_time_ms));
        blocks_left_to_link = until_block_id - i;
        estimated_time_left_sec = blocks_left_to_link / blocks_linked_per_sec;
      }
      LOG_INFO(CAT_BLOCK_LOG,
               "Last block ID connected: " << i << ","
                                           << KVLOG(from_block_id,
                                                    until_block_id,
                                                    elapsed_time_ms,
                                                    blocks_linked_per_sec,
                                                    blocks_left_to_link,
                                                    estimated_time_left_sec));
    }
  }

  LOG_INFO(V4_BLOCK_LOG, "Linked st range from " << from_block_id << " until " << last_added);
  return (last_added - from_block_id) + 1;
}

std::optional<categorization::Value> KeyValueBlockchain::getValueFromUpdate(
    BlockId block_id, const std::string &key, const categorization::BlockMerkleInput &category_input) const {
  const auto valit = category_input.kv.find(key);
  if (valit == category_input.kv.cend()) {
    return std::nullopt;
  }
  return categorization::MerkleValue{{block_id, valit->second}};
}
std::optional<categorization::Value> KeyValueBlockchain::getValueFromUpdate(
    BlockId block_id, const std::string &key, const categorization::VersionedInput &category_input) const {
  const auto valit = category_input.kv.find(key);
  if (valit == category_input.kv.cend()) {
    return std::nullopt;
  }
  return categorization::VersionedValue{{block_id, (valit->second).data}};
}
std::optional<categorization::Value> KeyValueBlockchain::getValueFromUpdate(
    BlockId block_id, const std::string &key, const categorization::ImmutableInput &category_input) const {
  const auto valit = category_input.kv.find(key);
  if (valit == category_input.kv.cend()) {
    return std::nullopt;
  }
  return categorization::ImmutableValue{{block_id, (valit->second).data}};
}

std::optional<categorization::Value> KeyValueBlockchain::get(const std::string &category_id,
                                                             const std::string &key,
                                                             BlockId block_id) const {
  auto updates_in_block = block_chain_.getBlockUpdates(block_id);
  if (!updates_in_block) {
    return std::nullopt;
  }
  const auto &kv_updates = updates_in_block->categoryUpdates(category_id);
  if (!kv_updates) {
    return std::nullopt;
  }
  std::optional<categorization::Value> ret;
  std::visit(
      [this, block_id, &key, &ret](const auto &specific_cat_updates) {
        ret = this->getValueFromUpdate(block_id, key, specific_cat_updates);
      },
      (*kv_updates).get());
  return ret;
}

std::optional<categorization::Value> KeyValueBlockchain::getLatest(const std::string &category_id,
                                                                   const std::string &key) const {
  BlockId latest_block_id = block_chain_.getLastReachable();
  return latest_keys_.getValue(category_id, detail::Blockchain::generateKey(latest_block_id), key);
}

void KeyValueBlockchain::multiGet(const std::string &category_id,
                                  const std::vector<std::string> &keys,
                                  const std::vector<BlockId> &versions,
                                  std::vector<std::optional<categorization::Value>> &values) const {
  ConcordAssertEQ(keys.size(), versions.size());
  ConcordAssertEQ(keys.size(), versions.size());
  values.clear();
  values.reserve(keys.size());
  std::unordered_map<BlockId, std::optional<categorization::Updates>> unique_block_updates;
  block_chain_.multiGetBlockUpdates(versions, unique_block_updates);
  for (size_t i = 0; i < keys.size(); ++i) {
    const auto block_id = versions[i];
    const auto &key = keys[i];
    auto updates_in_block_it = unique_block_updates.find(block_id);
    bool value_added_is_good = false;
    if (updates_in_block_it != unique_block_updates.end()) {
      if (updates_in_block_it->second) {
        const auto &kv_updates = updates_in_block_it->second->categoryUpdates(category_id);
        if (kv_updates) {
          std::optional<categorization::Value> val;
          std::visit(
              [this, block_id, &key, &val](const auto &specific_cat_updates) {
                val = getValueFromUpdate(block_id, key, specific_cat_updates);
              },
              (*kv_updates).get());
          values.push_back(val);
          value_added_is_good = true;
        }
      }
    }
    if (!value_added_is_good) {
      values.push_back(std::nullopt);
    }
  }
}

void KeyValueBlockchain::multiGetLatest(const std::string &category_id,
                                        const std::vector<std::string> &keys,
                                        std::vector<std::optional<categorization::Value>> &values) const {
  BlockId latest_block_id = block_chain_.getLastReachable();
  return latest_keys_.multiGetValue(category_id, detail::Blockchain::generateKey(latest_block_id), keys, values);
}

std::optional<categorization::TaggedVersion> KeyValueBlockchain::getLatestVersion(const std::string &category_id,
                                                                                  const std::string &key) const {
  return latest_keys_.getLatestVersion(
      category_id, detail::Blockchain::generateKey(block_chain_.getLastReachable()), key);
}

void KeyValueBlockchain::multiGetLatestVersion(
    const std::string &category_id,
    const std::vector<std::string> &keys,
    std::vector<std::optional<categorization::TaggedVersion>> &versions) const {
  return latest_keys_.multiGetLatestVersion(
      category_id, detail::Blockchain::generateKey(block_chain_.getLastReachable()), keys, versions);
}

void KeyValueBlockchain::trimBlocksFromSnapshot(BlockId block_id_at_checkpoint) {
  ConcordAssertNE(block_id_at_checkpoint, detail::Blockchain::INVALID_BLOCK_ID);
  ConcordAssertLE(block_id_at_checkpoint, getLastReachableBlockId());
  while (block_id_at_checkpoint < getLastReachableBlockId()) {
    LOG_INFO(V4_BLOCK_LOG,
             "Deleting last reachable block = " << getLastReachableBlockId()
                                                << ", DB checkpoint = " << native_client_->path());
    deleteLastReachableBlock();
  }
}

}  // namespace concord::kvbc::v4blockchain
