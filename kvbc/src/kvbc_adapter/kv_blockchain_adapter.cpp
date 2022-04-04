// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "kvbc_adapter/kv_blockchain_adapter.hpp"
#include "assertUtils.hpp"

namespace concord::kvbc::adapter {

const categorization::Converter KeyValueBlockchain::kNoopConverter = [](std::string&& v) -> std::string {
  return std::move(v);
};

BlockId KeyValueBlockchain::addBlock(categorization::Updates&& updates) {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->addBlock(std::move(updates));
  }
  ConcordAssert(false);
}

bool KeyValueBlockchain::deleteBlock(const BlockId& blockId) {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->deleteBlock(blockId);
  }
  ConcordAssert(false);
}
void KeyValueBlockchain::deleteLastReachableBlock() {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    std::get<CatBCimpl>(kvbc_)->deleteLastReachableBlock();
    return;
  }
  ConcordAssert(false);
}

void KeyValueBlockchain::addRawBlock(const categorization::RawBlock& block, const BlockId& block_id, bool lastBlock) {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    std::get<CatBCimpl>(kvbc_)->addRawBlock(block, block_id, lastBlock);
    return;
  }
  ConcordAssert(false);
}
std::optional<categorization::RawBlock> KeyValueBlockchain::getRawBlock(const BlockId& block_id) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getRawBlock(block_id);
  }
  ConcordAssert(false);
}

BlockId KeyValueBlockchain::getGenesisBlockId() const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getGenesisBlockId();
  }
  ConcordAssert(false);
}
BlockId KeyValueBlockchain::getLastReachableBlockId() const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getLastReachableBlockId();
  }
  ConcordAssert(false);
}
std::optional<BlockId> KeyValueBlockchain::getLastStatetransferBlockId() const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getLastStatetransferBlockId();
  }
  ConcordAssert(false);
}

std::optional<categorization::Hash> KeyValueBlockchain::parentDigest(BlockId block_id) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->parentDigest(block_id);
  }
  ConcordAssert(false);
}
bool KeyValueBlockchain::hasBlock(BlockId block_id) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->hasBlock(block_id);
  }
  ConcordAssert(false);
}

std::vector<std::string> KeyValueBlockchain::getStaleKeys(BlockId block_id,
                                                          const std::string& category_id,
                                                          const categorization::ImmutableOutput& updates_info) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getStaleKeys(block_id, category_id, updates_info);
  }
  ConcordAssert(false);
}

std::vector<std::string> KeyValueBlockchain::getStaleKeys(BlockId block_id,
                                                          const std::string& category_id,
                                                          const categorization::VersionedOutput& updates_info) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getStaleKeys(block_id, category_id, updates_info);
  }
  ConcordAssert(false);
}

std::vector<std::string> KeyValueBlockchain::getStaleKeys(BlockId block_id,
                                                          const std::string& category_id,
                                                          const categorization::BlockMerkleOutput& updates_info) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getStaleKeys(block_id, category_id, updates_info);
  }
  ConcordAssert(false);
}

std::optional<categorization::Value> KeyValueBlockchain::get(const std::string& category_id,
                                                             const std::string& key,
                                                             BlockId block_id) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->get(category_id, key, block_id);
  }
  ConcordAssert(false);
}

std::optional<categorization::Value> KeyValueBlockchain::getLatest(const std::string& category_id,
                                                                   const std::string& key) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getLatest(category_id, key);
  }
  ConcordAssert(false);
}

void KeyValueBlockchain::multiGet(const std::string& category_id,
                                  const std::vector<std::string>& keys,
                                  const std::vector<BlockId>& versions,
                                  std::vector<std::optional<categorization::Value>>& values) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    std::get<CatBCimpl>(kvbc_)->multiGet(category_id, keys, versions, values);
    return;
  }
  ConcordAssert(false);
}

void KeyValueBlockchain::multiGetLatest(const std::string& category_id,
                                        const std::vector<std::string>& keys,
                                        std::vector<std::optional<categorization::Value>>& values) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    std::get<CatBCimpl>(kvbc_)->multiGetLatest(category_id, keys, values);
    return;
  }
  ConcordAssert(false);
}

std::optional<categorization::TaggedVersion> KeyValueBlockchain::getLatestVersion(const std::string& category_id,
                                                                                  const std::string& key) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getLatestVersion(category_id, key);
  }
  ConcordAssert(false);
}

void KeyValueBlockchain::multiGetLatestVersion(
    const std::string& category_id,
    const std::vector<std::string>& keys,
    std::vector<std::optional<categorization::TaggedVersion>>& versions) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    std::get<CatBCimpl>(kvbc_)->multiGetLatestVersion(category_id, keys, versions);
    return;
  }
  ConcordAssert(false);
}

// Get the updates that were used to create `block_id`.
std::optional<categorization::Updates> KeyValueBlockchain::getBlockUpdates(BlockId block_id) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getBlockUpdates(block_id);
  }
  ConcordAssert(false);
}

// Get a map of category_id and stale keys for `block_id`
std::map<std::string, std::vector<std::string>> KeyValueBlockchain::getBlockStaleKeys(BlockId block_id) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getBlockStaleKeys(block_id);
  }
  ConcordAssert(false);
}

// Get a map from category ID -> type for all known categories in the blockchain.
const std::map<std::string, categorization::CATEGORY_TYPE>& KeyValueBlockchain::blockchainCategories() const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->blockchainCategories();
  }
  ConcordAssert(false);
}

// from_block_id is given as a hint, as supposed to be the next block after the last reachable block.
// until_block_id is the maximum block ID to be linked. If there are blocks with a block ID larger than
// until_block_id, they can be linked on future calls.
// returns the number of blocks connected
size_t KeyValueBlockchain::linkUntilBlockId(BlockId from_block_id, BlockId until_block_id) {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->linkUntilBlockId(from_block_id, until_block_id);
  }
  ConcordAssert(false);
}

std::shared_ptr<concord::storage::rocksdb::NativeClient> KeyValueBlockchain::db() {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->db();
  }
  ConcordAssert(false);
}
std::shared_ptr<const concord::storage::rocksdb::NativeClient> KeyValueBlockchain::db() const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->db();
  }
  ConcordAssert(false);
}

std::string KeyValueBlockchain::getPruningStatus() {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getPruningStatus();
  }
  ConcordAssert(false);
}

void KeyValueBlockchain::setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    std::get<CatBCimpl>(kvbc_)->setAggregator(aggregator);
    return;
  }
  ConcordAssert(false);
}

// The key used in the default column family for persisting the current public state hash.
// TODO: ARC: Remove this static function
std::string KeyValueBlockchain::publicStateHashKey() {
  return concord::kvbc::categorization::KeyValueBlockchain::publicStateHashKey();
}

}  // namespace concord::kvbc::adapter