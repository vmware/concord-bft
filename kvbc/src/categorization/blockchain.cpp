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

#include "categorization/blockchain.h"
#include "Logger.hpp"

namespace concord::kvbc::categorization::detail {

Blockchain::Blockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client)
    : native_client_{native_client} {
  if (detail::createColumnFamilyIfNotExisting(detail::BLOCKS_CF, *native_client_.get())) {
    LOG_INFO(CAT_BLOCK_LOG, "Created [" << detail::BLOCKS_CF << "] column family for the main blockchain");
  }
  auto last_reachable_block_id = loadLastReachableBlockId();
  if (last_reachable_block_id) {
    last_reachable_block_id_ = last_reachable_block_id.value();
    LOG_INFO(CAT_BLOCK_LOG, "Last reachable block as loaded from storage " << last_reachable_block_id_);
  }
  auto genesis_blockId = loadGenesisBlockId();
  if (genesis_blockId) {
    genesis_block_id_ = genesis_blockId.value();
    LOG_INFO(CAT_BLOCK_LOG, "Genesis block as loaded from storage " << genesis_block_id_);
  }
}

/////////////////////// Last and genesis block IDs operations ///////////////////////

// Last reachable
std::optional<BlockId> Blockchain::loadLastReachableBlockId() {
  auto itr = native_client_->getIterator(detail::BLOCKS_CF);
  itr.seekAtMost(Block::generateKey(MAX_BLOCK_ID));
  if (!itr) {
    return std::optional<BlockId>{};
  }
  BlockKey key{};
  detail::deserialize(itr.keyView(), key);
  return key.block_id;
}

// Genesis
std::optional<BlockId> Blockchain::loadGenesisBlockId() {
  auto itr = native_client_->getIterator(detail::BLOCKS_CF);
  itr.seekAtLeast(Block::generateKey(INITIAL_GENESIS_BLOCK_ID));
  if (!itr) {
    return std::optional<BlockId>{};
  }
  BlockKey key{};
  detail::deserialize(itr.keyView(), key);
  return key.block_id;
}

}  // namespace concord::kvbc::categorization::detail
