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

#include "v4blockchain/detail/st_chain.h"
#include "v4blockchain/detail/column_families.h"
#include "v4blockchain/detail/blockchain.h"
#include "Logger.hpp"

namespace concord::kvbc::v4blockchain::detail {

StChain::StChain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client)
    : native_client_{native_client} {
  if (native_client_->createColumnFamilyIfNotExisting(v4blockchain::detail::ST_CHAIN_CF)) {
    LOG_INFO(V4_BLOCK_LOG,
             "Created [" << v4blockchain::detail::ST_CHAIN_CF << "] column family for the state transfer blockchain");
  }
  loadLastBlockId();
  if (last_block_id_ > 0) {
    LOG_INFO(CAT_BLOCK_LOG, "State transfer last block id: " << last_block_id_);
  }
}

/////////// BLOCKS/////////////////////////
bool StChain::hasBlock(BlockId block_id) const {
  if (last_block_id_ < block_id) return false;
  return native_client_
      ->getSlice(v4blockchain::detail::ST_CHAIN_CF, v4blockchain::detail::Blockchain::generateKey(block_id))
      .has_value();
}

void StChain::addBlock(const BlockId id, const char* block, const uint32_t blockSize) {
  LOG_DEBUG(CAT_BLOCK_LOG, "Adding ST block " << id);
  auto write_batch = native_client_->getBatch();
  write_batch.put(v4blockchain::detail::ST_CHAIN_CF,
                  v4blockchain::detail::Blockchain::generateKey(id),
                  ::rocksdb::Slice(block, blockSize));
  native_client_->write(std::move(write_batch));
  updateLastIdIfBigger(id);
}

void StChain::deleteBlock(const BlockId id, storage::rocksdb::NativeWriteBatch& wb) {
  wb.del(v4blockchain::detail::ST_CHAIN_CF, v4blockchain::detail::Blockchain::generateKey(id));
}
/////////////// ST LAST BLOCK ID ////////////
void StChain::loadLastBlockId() {
  auto itr = native_client_->getIterator(v4blockchain::detail::ST_CHAIN_CF);
  auto max_db_key = v4blockchain::detail::Blockchain::generateKey(v4blockchain::detail::Blockchain::MAX_BLOCK_ID);
  itr.seekAtMost(max_db_key);
  if (!itr) {
    last_block_id_ = 0;
    return;
  }
  last_block_id_ = concordUtils::fromBigEndianBuffer<BlockId>(itr.keyView().data());
}

void StChain::updateLastIdAfterDeletion(const BlockId id) {
  if (last_block_id_ == 0 || last_block_id_ != id) {
    return;
  }
  last_block_id_ = 0;
  loadLastBlockId();
  return;
}

void StChain::updateLastIdIfBigger(const BlockId id) {
  if (last_block_id_ >= id) {
    return;
  }
  last_block_id_ = id;
}

std::optional<v4blockchain::detail::Block> StChain::getBlock(kvbc::BlockId id) const {
  auto key = v4blockchain::detail::Blockchain::generateKey(id);
  auto opt_block_str = native_client_->get(v4blockchain::detail::ST_CHAIN_CF, key);
  if (!opt_block_str) return std::nullopt;
  return v4blockchain::detail::Block(*opt_block_str);
}

concord::util::digest::BlockDigest StChain::getBlockParentDigest(concord::kvbc::BlockId id) const {
  auto block = getBlock(id);
  ConcordAssert(block.has_value());
  return block->parentDigest();
}

std::optional<std::string> StChain::getBlockData(concord::kvbc::BlockId id) const {
  auto key = v4blockchain::detail::Blockchain::generateKey(id);
  return native_client_->get(v4blockchain::detail::ST_CHAIN_CF, key);
}

}  // namespace concord::kvbc::v4blockchain::detail
