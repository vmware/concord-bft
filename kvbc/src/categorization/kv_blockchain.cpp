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
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"

#include <stdexcept>

namespace concord::kvbc::categorization {

using ::bftEngine::bcst::computeBlockDigest;

template <typename T>
void nullopts(std::vector<std::optional<T>>& vec, std::size_t count) {
  vec.resize(count, std::nullopt);
}

KeyValueBlockchain::KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
                                       bool link_st_chain)
    : native_client_{native_client}, block_chain_{native_client_}, state_transfer_block_chain_{native_client_} {
  if (detail::createColumnFamilyIfNotExisting(detail::CAT_ID_TYPE_CF, *native_client_.get())) {
    LOG_INFO(CAT_BLOCK_LOG, "Created [" << detail::CAT_ID_TYPE_CF << "] column family for the category types");
  }
  instantiateCategories();
  if (!link_st_chain) return;
  // Make sure that if linkSTChainFrom() has been interrupted (e.g. a crash or an abnormal shutdown), all DBAdapter
  // methods will return the correct values. For example, if state transfer had completed and linkSTChainFrom() was
  // interrupted, getLatestBlockId() should be equal to getLastReachableBlockId() on the next startup. Another example
  // is getValue() that returns keys from the blockchain only and ignores keys in the temporary state
  // transfer chain.
  linkSTChainFrom(getLastReachableBlockId() + 1);
}

void KeyValueBlockchain::instantiateCategories() {
  auto itr = native_client_->getIterator(detail::CAT_ID_TYPE_CF);
  itr.first();
  while (itr) {
    if (itr.valueView().size() != 1) {
      LOG_FATAL(CAT_BLOCK_LOG, "Category type value of [" << itr.key() << "] is invalid (bigger than one).");
      ConcordAssertEQ(itr.valueView().size(), 1);
    }
    auto cat_type = static_cast<CATEGORY_TYPE>(itr.valueView()[0]);
    switch (cat_type) {
      case CATEGORY_TYPE::block_merkle:
        categorires_.emplace(itr.key(), detail::BlockMerkleCategory{native_client_});
        categorires_types_[itr.key()] = CATEGORY_TYPE::block_merkle;
        LOG_INFO(CAT_BLOCK_LOG, "Created category [" << itr.key() << "] as type BlockMerkleCategory");
        break;
      case CATEGORY_TYPE::immutable:
        categorires_.emplace(itr.key(), detail::ImmutableKeyValueCategory{itr.key(), native_client_});
        categorires_types_[itr.key()] = CATEGORY_TYPE::immutable;
        LOG_INFO(CAT_BLOCK_LOG, "Created category [" << itr.key() << "] as type ImmutableKeyValueCategory");
        break;
      case CATEGORY_TYPE::versioned_kv:
        categorires_.emplace(itr.key(), detail::VersionedKeyValueCategory{itr.key(), native_client_});
        categorires_types_[itr.key()] = CATEGORY_TYPE::versioned_kv;
        LOG_INFO(CAT_BLOCK_LOG, "Created category [" << itr.key() << "] as type VersionedKeyValueCategory");
        break;
      default:
        throw std::runtime_error("couldn't find the type of " + itr.key());
        break;
    }
    itr.next();
  }
}

// 1) Defines a new block
// 2) calls per cateogry with its updates
// 3) inserts the updates KV to the DB updates set per column family
// 4) add the category block data into the new block
BlockId KeyValueBlockchain::addBlock(Updates&& updates) {
  // Use new client batch and column families
  auto write_batch = native_client_->getBatch();
  auto block_id = addBlock(std::move(updates.category_updates_), write_batch);
  block_chain_.setAddedBlockId(block_id);
  native_client_->write(std::move(write_batch));
  return block_id;
}

BlockId KeyValueBlockchain::addBlock(CategoryInput&& category_updates,
                                     concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  // Use new client batch and column families
  Block new_block{block_chain_.getLastReachableBlockId() + 1};
  auto parent_digest_future = computeParentBlockDigest(new_block.id(), std::move(last_raw_block_));
  // initialize the raw block for the next call to computeParentBlockDigest
  auto& last_raw_block = last_raw_block_.second.emplace();
  last_raw_block_.first = new_block.id();
  last_raw_block.updates = category_updates;
  // Per category updates
  for (auto&& [category_id, update] : category_updates.kv) {
    std::visit(
        [&new_block, category_id = category_id, &write_batch, this](auto&& update) {
          auto block_updates =
              handleCategoryUpdates(new_block.id(), category_id, std::forward<decltype(update)>(update), write_batch);
          new_block.add(category_id, std::move(block_updates));
        },
        std::move(update));
  }
  new_block.data.parent_digest = parent_digest_future.get();
  last_raw_block.parent_digest = new_block.data.parent_digest;
  block_chain_.addBlock(new_block, write_batch);
  LOG_DEBUG(CAT_BLOCK_LOG, "Writing block [" << new_block.id() << "] to the blocks cf");
  write_batch.put(detail::BLOCKS_CF, Block::generateKey(new_block.id()), Block::serialize(new_block));
  return new_block.id();
}

std::future<BlockDigest> KeyValueBlockchain::computeParentBlockDigest(const BlockId block_id,
                                                                      VersionedRawBlock&& cached_raw_block) {
  auto parent_block_id = block_id - 1;
  // if we have a cached raw block and it matches the parent_block_id then use it.
  if (cached_raw_block.second && cached_raw_block.first == parent_block_id) {
    LOG_DEBUG(CAT_BLOCK_LOG, "Using cached raw block for computing parent digest");
  }
  // cached raw block is unusable, get the raw block from storage
  else if (block_id > INITIAL_GENESIS_BLOCK_ID) {
    auto parent_raw_block = getRawBlock(parent_block_id);
    ConcordAssert(parent_raw_block.has_value());
    cached_raw_block.second = std::move(parent_raw_block->data);
  }
  // it's the first block, we don't have a parent
  else {
    cached_raw_block.second.reset();
  }
  // pass the versioned raw block by value (thread safe), for the async operation to calculate its digest
  return thread_pool_.async(
      [parent_block_id](VersionedRawBlock cached_raw_block) {
        // Make sure the digest is zero-initialized by using {} initialization.
        auto parent_block_digest = BlockDigest{};
        if (cached_raw_block.second) {
          // histograms.dba_hashed_parent_block_size->recordAtomic(parentBlock->length());
          // static constexpr bool is_atomic = true;
          // TimeRecorder<is_atomic> scoped(*histograms.dba_hash_parent_block);

          const auto& raw_buffer = detail::serialize(cached_raw_block.second.value());
          parent_block_digest =
              computeBlockDigest(parent_block_id, reinterpret_cast<const char*>(raw_buffer.data()), raw_buffer.size());
        }
        return parent_block_digest;
      },
      std::move(cached_raw_block));
}

/////////////////////// Readers ///////////////////////

const Category& KeyValueBlockchain::getCategory(const std::string& cat_id) const {
  auto it = categorires_.find(cat_id);
  if (it == categorires_.cend()) {
    throw std::runtime_error{"Category does not exist = " + cat_id};
  }
  return it->second;
}

Category& KeyValueBlockchain::getCategory(const std::string& cat_id) {
  auto it = categorires_.find(cat_id);
  if (it == categorires_.end()) {
    throw std::runtime_error{"Category does not exist = " + cat_id};
  }
  return it->second;
}

bool KeyValueBlockchain::hasCategory(const std::string& cat_id) const { return (categorires_.count(cat_id) != 0); }

std::optional<Value> KeyValueBlockchain::get(const std::string& category_id,
                                             const std::string& key,
                                             BlockId block_id) const {
  if (!hasCategory(category_id)) {
    return std::nullopt;
  }
  std::optional<Value> ret;
  std::visit([&key, &block_id, &ret](const auto& category) { ret = category.get(key, block_id); },
             getCategory(category_id));
  return ret;
}

std::optional<Value> KeyValueBlockchain::getLatest(const std::string& category_id, const std::string& key) const {
  if (!hasCategory(category_id)) {
    return std::nullopt;
  }
  std::optional<Value> ret;
  std::visit([&key, &ret](const auto& category) { ret = category.getLatest(key); }, getCategory(category_id));
  return ret;
}

void KeyValueBlockchain::multiGet(const std::string& category_id,
                                  const std::vector<std::string>& keys,
                                  const std::vector<BlockId>& versions,
                                  std::vector<std::optional<Value>>& values) const {
  if (!hasCategory(category_id)) {
    nullopts(values, keys.size());
    return;
  }
  std::visit([&keys, &versions, &values](const auto& category) { category.multiGet(keys, versions, values); },
             getCategory(category_id));
}

void KeyValueBlockchain::multiGetLatest(const std::string& category_id,
                                        const std::vector<std::string>& keys,
                                        std::vector<std::optional<Value>>& values) const {
  if (!hasCategory(category_id)) {
    nullopts(values, keys.size());
    return;
  }
  std::visit([&keys, &values](const auto& category) { category.multiGetLatest(keys, values); },
             getCategory(category_id));
}

std::optional<categorization::TaggedVersion> KeyValueBlockchain::getLatestVersion(const std::string& category_id,
                                                                                  const std::string& key) const {
  if (!hasCategory(category_id)) {
    return std::nullopt;
  }
  std::optional<categorization::TaggedVersion> ret;
  std::visit([&key, &ret](const auto& category) { ret = category.getLatestVersion(key); }, getCategory(category_id));
  return ret;
}

void KeyValueBlockchain::multiGetLatestVersion(
    const std::string& category_id,
    const std::vector<std::string>& keys,
    std::vector<std::optional<categorization::TaggedVersion>>& versions) const {
  if (!hasCategory(category_id)) {
    nullopts(versions, keys.size());
    return;
  }
  std::visit([&keys, &versions](const auto& catagory) { catagory.multiGetLatestVersion(keys, versions); },
             getCategory(category_id));
}

std::optional<Updates> KeyValueBlockchain::getBlockUpdates(BlockId block_id) const {
  auto raw = getRawBlock(block_id);
  if (!raw) {
    return std::nullopt;
  }
  return Updates{std::move(raw->data.updates)};
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
  state_transfer_block_chain_.updateLastIdAfterDeletion(block_id);
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

  native_client_->write(std::move(write_batch));
  // Increment the genesis block ID cache.
  block_chain_.setGenesisBlockId(genesis_id + 1);
}

// 1 - Get last id block from DB.
// 2 - Iterate over the update_info and calls the corresponding deleteLastReachableBlock
// 3 - Perform the delete
// 4 - Increment the genesis block id.
void KeyValueBlockchain::deleteLastReachableBlock() {
  const auto last_id = block_chain_.getLastReachableBlockId();
  if (last_id == detail::Blockchain::INVALID_BLOCK_ID) {
    throw std::logic_error{"Blockchain empty, cannot delete last reachable block"};
  } else if (last_id == block_chain_.getGenesisBlockId()) {
    throw std::logic_error{"Cannot delete only block as a last reachable one"};
  }

  auto write_batch = native_client_->getBatch();
  // Get block node from storage
  auto block = block_chain_.getBlock(last_id);
  if (!block) {
    const auto msg = "Failed to get block node for block ID = " + std::to_string(last_id);
    throw std::runtime_error{msg};
  }

  block_chain_.deleteBlock(last_id, write_batch);

  // Iterate over groups and call corresponding deleteGenesisBlock,
  // Each group is responsible to put its deletes into the batch
  for (auto&& [category_id, update_info] : block.value().data.categories_updates_info) {
    std::visit(
        [&last_id, category_id = category_id, &write_batch, this](const auto& update_info) {
          deleteLastReachableBlock(last_id, category_id, update_info, write_batch);
        },
        update_info);
  }

  native_client_->write(std::move(write_batch));

  // Since we allow deletion of the only block left as last reachable (due to replica state sync), set both genesis and
  // last reachable cache variables to 0. Otherise, only decrement the last reachable block ID cache.
  auto genesis_id = block_chain_.getGenesisBlockId();
  if (last_id == genesis_id) {
    block_chain_.setGenesisBlockId(0);
    block_chain_.setLastReachableBlockId(0);
  } else {
    // Decrement the last reachable block ID cache.
    block_chain_.setLastReachableBlockId(last_id - 1);
  }
}

// Deletes per category
void KeyValueBlockchain::deleteGenesisBlock(BlockId block_id,
                                            const std::string& category_id,
                                            const ImmutableOutput& updates_info,
                                            storage::rocksdb::NativeWriteBatch& batch) {
  std::get<detail::ImmutableKeyValueCategory>(getCategory(category_id))
      .deleteGenesisBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteGenesisBlock(BlockId block_id,
                                            const std::string& category_id,
                                            const VersionedOutput& updates_info,
                                            storage::rocksdb::NativeWriteBatch& batch) {
  std::get<detail::VersionedKeyValueCategory>(getCategory(category_id))
      .deleteGenesisBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteGenesisBlock(BlockId block_id,
                                            const std::string& category_id,
                                            const BlockMerkleOutput& updates_info,
                                            storage::rocksdb::NativeWriteBatch& batch) {
  std::get<detail::BlockMerkleCategory>(getCategory(category_id)).deleteGenesisBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteLastReachableBlock(BlockId block_id,
                                                  const std::string& category_id,
                                                  const ImmutableOutput& updates_info,
                                                  storage::rocksdb::NativeWriteBatch& batch) {
  std::get<detail::ImmutableKeyValueCategory>(getCategory(category_id))
      .deleteLastReachableBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteLastReachableBlock(BlockId block_id,
                                                  const std::string& category_id,
                                                  const VersionedOutput& updates_info,
                                                  storage::rocksdb::NativeWriteBatch& batch) {
  std::get<detail::VersionedKeyValueCategory>(getCategory(category_id))
      .deleteLastReachableBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteLastReachableBlock(BlockId block_id,
                                                  const std::string& category_id,
                                                  const BlockMerkleOutput& updates_info,
                                                  storage::rocksdb::NativeWriteBatch& batch) {
  std::get<detail::BlockMerkleCategory>(getCategory(category_id))
      .deleteLastReachableBlock(block_id, updates_info, batch);
}

// Updates per category

bool KeyValueBlockchain::insertCategoryMapping(const std::string& cat_id,
                                               const CATEGORY_TYPE type,
                                               concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  // check if we know this category type already
  if (categorires_types_.count(cat_id) == 1) {
    return false;
  }
  // new category
  // cache the type in mem and store it in db.
  if (const auto [itr, inserted] = categorires_types_.try_emplace(cat_id, type); !inserted) {
    (void)itr;
    throw std::runtime_error{"Failed to insert new category"};
  }
  write_batch.put(detail::CAT_ID_TYPE_CF, cat_id, std::string(1, static_cast<char>(type)));
  return true;
}

BlockMerkleOutput KeyValueBlockchain::handleCategoryUpdates(BlockId block_id,
                                                            const std::string& category_id,
                                                            BlockMerkleInput&& updates,
                                                            concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  // if true means that category is new and we should create an instance of it.
  if (insertCategoryMapping(category_id, CATEGORY_TYPE::block_merkle, write_batch)) {
    if (const auto [_, inserted] = categorires_.try_emplace(category_id, detail::BlockMerkleCategory{native_client_});
        !inserted) {
      (void)_;
      LOG_FATAL(CAT_BLOCK_LOG, "Category already exists = " << category_id);
      ConcordAssert(false);
    }
  }

  auto itr = categorires_.find(category_id);
  if (itr == categorires_.end()) {
    throw std::runtime_error{"Category is not present in memory = " + category_id};
  }
  LOG_DEBUG(CAT_BLOCK_LOG, "Adding updates of block [" << block_id << "] to the BlockMerkleCategory");
  return std::get<detail::BlockMerkleCategory>(itr->second).add(block_id, std::move(updates), write_batch);
}

VersionedOutput KeyValueBlockchain::handleCategoryUpdates(BlockId block_id,
                                                          const std::string& category_id,
                                                          VersionedInput&& updates,
                                                          concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  // if true means that category is new and we should create an instance of it.
  if (insertCategoryMapping(category_id, CATEGORY_TYPE::versioned_kv, write_batch)) {
    if (const auto [_, inserted] =
            categorires_.try_emplace(category_id, detail::VersionedKeyValueCategory{category_id, native_client_});
        !inserted) {
      (void)_;
      LOG_FATAL(CAT_BLOCK_LOG, "Category already exists = " << category_id);
      ConcordAssert(false);
    }
  }

  auto itr = categorires_.find(category_id);
  if (itr == categorires_.end()) {
    throw std::runtime_error{"Category is not present in memory = " + category_id};
  }
  LOG_DEBUG(CAT_BLOCK_LOG, "Adding updates of block [" << block_id << "] to the VersionedKeyValueCategory");
  return std::get<detail::VersionedKeyValueCategory>(itr->second).add(block_id, std::move(updates), write_batch);
}

ImmutableOutput KeyValueBlockchain::handleCategoryUpdates(BlockId block_id,
                                                          const std::string& category_id,
                                                          ImmutableInput&& updates,
                                                          concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  // if true means that category is new and we should create an instance of it.
  if (insertCategoryMapping(category_id, CATEGORY_TYPE::immutable, write_batch)) {
    if (const auto [_, inserted] =
            categorires_.try_emplace(category_id, detail::ImmutableKeyValueCategory{category_id, native_client_});
        !inserted) {
      (void)_;
      LOG_FATAL(CAT_BLOCK_LOG, "Category already exists = " << category_id);
      ConcordAssert(false);
    }
  }
  auto itr = categorires_.find(category_id);
  if (itr == categorires_.end()) {
    throw std::runtime_error{"Category is not present in memory = " + category_id};
  }
  LOG_DEBUG(CAT_BLOCK_LOG, "Adding updates of block [" << block_id << "] to the ImmutableKeyValueCategory");
  return std::get<detail::ImmutableKeyValueCategory>(itr->second).add(block_id, std::move(updates), write_batch);
}

/////////////////////// state transfer blockchain ///////////////////////
void KeyValueBlockchain::addRawBlock(const RawBlock& block, const BlockId& block_id) {
  const auto last_reachable_block = getLastReachableBlockId();
  if (block_id <= last_reachable_block) {
    const auto msg = "Cannot add an existing block ID " + std::to_string(block_id);
    throw std::invalid_argument{msg};
  }

  auto write_batch = native_client_->getBatch();
  state_transfer_block_chain_.addBlock(block_id, block, write_batch);
  native_client_->write(std::move(write_batch));
  // Update the cached latest ST temporary block ID if we have received and persisted such a block.
  state_transfer_block_chain_.updateLastId(block_id);

  try {
    linkSTChainFrom(last_reachable_block + 1);
  } catch (const std::exception& e) {
    // LOG_FATAL(logger_, "Aborting due to failure to link chains after block has been added, reason: "s + e.what());
    std::terminate();
  } catch (...) {
    // LOG_FATAL(logger_, "Aborting due to failure to link chains after block has been added");
    std::terminate();
  }
}

std::optional<RawBlock> KeyValueBlockchain::getRawBlock(const BlockId& block_id) const {
  const auto last_reachable_block = getLastReachableBlockId();

  // Try to take it from the ST chain
  if (block_id > last_reachable_block) {
    return state_transfer_block_chain_.getRawBlock(block_id);
  }
  // Try from the blockchain itself
  return block_chain_.getRawBlock(block_id, categorires_);
}

std::optional<Hash> KeyValueBlockchain::parentDigest(BlockId block_id) const {
  const auto last_reachable_block = getLastReachableBlockId();
  if (block_id > last_reachable_block) {
    return state_transfer_block_chain_.parentDigest(block_id);
  }
  return block_chain_.parentDigest(block_id);
}

bool KeyValueBlockchain::hasBlock(BlockId block_id) const {
  const auto last_reachable_block = getLastReachableBlockId();
  if (block_id > last_reachable_block) {
    return state_transfer_block_chain_.hasBlock(block_id);
  }
  return block_chain_.hasBlock(block_id);
}

// tries to remove blocks form the state transfer chain to the blockchain
void KeyValueBlockchain::linkSTChainFrom(BlockId block_id) {
  const auto last_block_id = state_transfer_block_chain_.getLastBlockId();
  if (!last_block_id) return;

  for (auto i = block_id; i <= last_block_id.value(); ++i) {
    auto raw_block = state_transfer_block_chain_.getRawBlock(i);
    if (!raw_block) {
      return;
    }
    writeSTLinkTransaction(block_id, *raw_block);
  }
  // Linking has fully completed and we should not have any more ST temporary blocks left. Therefore, make sure we don't
  // have any value for the latest ST temporary block ID cache.
  state_transfer_block_chain_.resetChain();
}

// Atomic delete from state transfer and add to blockchain
void KeyValueBlockchain::writeSTLinkTransaction(const BlockId block_id, RawBlock& block) {
  auto write_batch = native_client_->getBatch();
  state_transfer_block_chain_.deleteBlock(block_id, write_batch);
  auto new_block_id = addBlock(std::move(block.data.updates), write_batch);
  native_client_->write(std::move(write_batch));

  block_chain_.setAddedBlockId(new_block_id);
}

}  // namespace concord::kvbc::categorization
