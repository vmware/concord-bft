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

#include "v4blockchain/v4_blockchain.h"
#include "v4blockchain/detail/detail.h"
#include "categorization/base_types.h"
#include "categorization/db_categories.h"
#include "kvbc_key_types.hpp"
#include "block_metadata.hpp"

namespace concord::kvbc::v4blockchain {

using namespace concord::kvbc;

KeyValueBlockchain::KeyValueBlockchain(
    const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
    bool link_st_chain,
    const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>>& category_types)
    : native_client_{native_client},
      block_chain_{native_client_},
      state_transfer_chain_{native_client_},
      latest_keys_{native_client_, category_types} {}

/*
  1 - check if we can perform GC on latest CF
  2 - add the block to the blocks CF
  3 - add the keys to the latest CF
  4 - atomic write to storage
  5 - increment last reachable block.
*/
BlockId KeyValueBlockchain::add(categorization::Updates&& updates) {
  auto scoped = v4blockchain::detail::ScopedDuration{"Add block"};
  // Should be performed before we add the block with the current Updates.
  auto sequence_number = markHistoryForGarbageCollectionIfNeeded(updates);

  auto write_batch = native_client_->getBatch();
  // addGenesisBlockKey(updates);
  BlockId block_id{};
  {
    auto scoped2 = v4blockchain::detail::ScopedDuration{"Add block to blockchain"};
    block_id = block_chain_.addBlock(updates, write_batch);
  }

  {
    auto scoped3 = v4blockchain::detail::ScopedDuration{"Add block to latest"};
    latest_keys_.addBlockKeys(updates, block_id, write_batch);
  }
  native_client_->write(std::move(write_batch));
  block_chain_.setLastReachable(block_id);
  if (sequence_number > 0) setLastBlockSequenceNumber(sequence_number);
  return block_id;
}

/*
  If the bft sequence number for this updates is bigger than the last, it means that the last sequence number
  was commited, and it's safe to mark the version of the last block that corresponds to that sequence number as safe for
  rocksdb garbage collection.
  https://github.com/facebook/rocksdb/wiki/User-defined-Timestamp-(Experimental)#compaction-and-garbage-collection-gc
  - if last_block_sn_ is nullopt we're probably on first start.
  - we don't want to trim when checkpoint is in process.
*/
uint64_t KeyValueBlockchain::markHistoryForGarbageCollectionIfNeeded(const categorization::Updates& updates) {
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

uint64_t KeyValueBlockchain::getBlockSequenceNumber(const categorization::Updates& updates) const {
  static std::string key = std::string(1, IBlockMetadata::kBlockMetadataKey);
  const auto& input = updates.categoryUpdates();
  if (input.kv.count(categorization::kConcordInternalCategoryId) == 0) {
    return 0;
  }
  const auto& ver_input =
      std::get<categorization::VersionedInput>(input.kv.at(categorization::kConcordInternalCategoryId));
  if (ver_input.kv.count(key) == 0) {
    return 0;
  }
  return BlockMetadata::getSequenceNum(ver_input.kv.at(key).data);
}

void KeyValueBlockchain::addGenesisBlockKey(categorization::Updates& updates) const {
  const auto stale_on_update = true;
  updates.addCategoryIfNotExisting<categorization::VersionedInput>(categorization::kConcordInternalCategoryId);
  updates.appendKeyValue<categorization::VersionedUpdates>(
      categorization::kConcordInternalCategoryId,
      std::string{keyTypes::genesis_block_key},
      categorization::VersionedUpdates::ValueType{
          concordUtils::toBigEndianStringBuffer(block_chain_.getGenesisBlockId()), stale_on_update});
}
}  // namespace concord::kvbc::v4blockchain
