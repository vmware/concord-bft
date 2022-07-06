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
#include "bftengine/ControlStateManager.hpp"
#include "diagnostics.h"
#include "performance_handler.h"
#include "kvbc_key_types.hpp"
#include "categorization/db_categories.h"
#include "endianness.hpp"
#include "migrations/block_merkle_latest_ver_cf_migration.h"
#include "categorization/details.h"
#include "ReplicaConfig.hpp"
#include "throughput.hpp"

#include <algorithm>
#include <iterator>
#include <stdexcept>

namespace concord::kvbc::categorization {

using ::bftEngine::bcst::computeBlockDigest;

template <typename T>
void nullopts(std::vector<std::optional<T>>& vec, std::size_t count) {
  vec.resize(count, std::nullopt);
}

KeyValueBlockchain::Recorders KeyValueBlockchain::histograms_;

KeyValueBlockchain::KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
                                       bool link_st_chain,
                                       const std::optional<std::map<std::string, CATEGORY_TYPE>>& category_types)
    : native_client_{native_client},
      block_chain_{native_client_},
      state_transfer_block_chain_{native_client_},
      delete_metrics_comp_{
          concordMetrics::Component("kv_blockchain_deletes", std::make_shared<concordMetrics::Aggregator>())},
      versioned_num_of_deletes_keys_{delete_metrics_comp_.RegisterCounter("numOfVersionedKeysDeleted")},
      immutable_num_of_deleted_keys_{delete_metrics_comp_.RegisterCounter("numOfImmutableKeysDeleted")},
      merkle_num_of_deleted_keys_{delete_metrics_comp_.RegisterCounter("numOfMerkleKeysDeleted")},
      add_metrics_comp_{
          concordMetrics::Component("kv_blockchain_adds", std::make_shared<concordMetrics::Aggregator>())},
      versioned_num_of_keys_{add_metrics_comp_.RegisterCounter("numOfVersionedKeys")},
      immutable_num_of_keys_{add_metrics_comp_.RegisterCounter("numOfImmutableKeys")},
      merkle_num_of_keys_{add_metrics_comp_.RegisterCounter("numOfMerkleKeys")} {
  if (detail::createColumnFamilyIfNotExisting(detail::CAT_ID_TYPE_CF, *native_client_.get())) {
    LOG_INFO(CAT_BLOCK_LOG, "Created [" << detail::CAT_ID_TYPE_CF << "] column family for the category types");
  }

  loadCategories();
  if (category_types_.empty()) {
    initNewBlockchainCategories(category_types);
  } else {
    initExistingBlockchainCategories(category_types);
  }

  if (!link_st_chain) return;
  // Make sure that if linkSTChainFrom() has been interrupted (e.g. a crash or an abnormal shutdown), all DBAdapter
  // methods will return the correct values. For example, if state transfer had completed and linkSTChainFrom() was
  // interrupted, getLatestBlockId() should be equal to getLastReachableBlockId() on the next startup. Another example
  // is getValue() that returns keys from the blockchain only and ignores keys in the temporary state
  // transfer chain.
  LOG_INFO(CAT_BLOCK_LOG, "Try to link ST temporary chain, this might take some time...");
  auto old_last_reachable_block_id = getLastReachableBlockId();
  linkSTChainFrom(old_last_reachable_block_id + 1);
  auto new_last_reachable_block_id = getLastReachableBlockId();
  LOG_INFO(CAT_BLOCK_LOG,
           "Done linking ST temporary chain:" << KVLOG(old_last_reachable_block_id, new_last_reachable_block_id));
  delete_metrics_comp_.Register();
  add_metrics_comp_.Register();

  // When we use this version of the code that uses the migrated DB format (or a completely fresh blockchain), we no
  // longer need migration. That assumes we never run this version of the code on an old DB format (before migrating).
  native_client_->put(migrations::BlockMerkleLatestVerCfMigration::migrationKey(),
                      migrations::BlockMerkleLatestVerCfMigration::kStateMigrationNotNeededOrCompleted);
}

void KeyValueBlockchain::initNewBlockchainCategories(
    const std::optional<std::map<std::string, CATEGORY_TYPE>>& category_types) {
  if (!category_types) {
    const auto msg = "Category types needed when constructing a KeyValueBlockchain for a new blockchain";
    LOG_ERROR(CAT_BLOCK_LOG, msg);
    throw std::invalid_argument{msg};
  }
  // Add all categories passed by the user.
  for (const auto& [category_id, type] : *category_types) {
    addNewCategory(category_id, type);
  }
}

void KeyValueBlockchain::initExistingBlockchainCategories(
    const std::optional<std::map<std::string, CATEGORY_TYPE>>& category_types) {
  if (!category_types) {
    return;
  }

  // Make sure the user passed all existing categories on disk with their correct types.
  for (const auto& [category_id, type] : category_types_) {
    auto it = category_types->find(category_id);
    if (it == category_types->cend()) {
      const auto msg =
          "Category ID [" + category_id + "] exists on disk, but is missing from the given category types parameter";
      LOG_ERROR(CAT_BLOCK_LOG, msg);
      throw std::invalid_argument{msg};
    } else if (it->second != type) {
      const auto msg = "Category ID [" + category_id + "] parameter with type [" + categoryStringType(it->second) +
                       "] differs from type on disk [" + categoryStringType(type) + "]";
      LOG_ERROR(CAT_BLOCK_LOG, msg);
      throw std::invalid_argument{msg};
    }
  }

  // If the user passed a new category, add it.
  for (const auto& [category_id, type] : *category_types) {
    if (category_types_.find(category_id) == category_types_.cend()) {
      addNewCategory(category_id, type);
    }
  }
}

void KeyValueBlockchain::loadCategories() {
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
        categories_.emplace(itr.key(), detail::BlockMerkleCategory{native_client_});
        category_types_[itr.key()] = CATEGORY_TYPE::block_merkle;
        LOG_INFO(CAT_BLOCK_LOG, "Created category [" << itr.key() << "] as type BlockMerkleCategory");
        break;
      case CATEGORY_TYPE::immutable:
        categories_.emplace(itr.key(), detail::ImmutableKeyValueCategory{itr.key(), native_client_});
        category_types_[itr.key()] = CATEGORY_TYPE::immutable;
        LOG_INFO(CAT_BLOCK_LOG, "Created category [" << itr.key() << "] as type ImmutableKeyValueCategory");
        break;
      case CATEGORY_TYPE::versioned_kv:
        categories_.emplace(itr.key(), detail::VersionedKeyValueCategory{itr.key(), native_client_});
        category_types_[itr.key()] = CATEGORY_TYPE::versioned_kv;
        LOG_INFO(CAT_BLOCK_LOG, "Created category [" << itr.key() << "] as type VersionedKeyValueCategory");
        break;
      default:
        ConcordAssert(false);
        break;
    }
    itr.next();
  }
}

void KeyValueBlockchain::addGenesisBlockKey(Updates& updates) const {
  const auto stale_on_update = true;
  updates.addCategoryIfNotExisting<VersionedInput>(kConcordInternalCategoryId);
  updates.appendKeyValue<VersionedUpdates>(
      kConcordInternalCategoryId,
      std::string{keyTypes::genesis_block_key},
      VersionedUpdates::ValueType{concordUtils::toBigEndianStringBuffer(getGenesisBlockId()), stale_on_update});
}

// 1) Defines a new block
// 2) calls per cateogry with its updates
// 3) inserts the updates KV to the DB updates set per column family
// 4) add the category block data into the new block
BlockId KeyValueBlockchain::addBlock(Updates&& updates) {
  diagnostics::TimeRecorder scoped_timer(*histograms_.addBlock);
  // Use new client batch and column families
  auto write_batch = native_client_->getBatch();
  addGenesisBlockKey(updates);
  auto block_id = addBlock(updates.categoryUpdates(), write_batch);
  native_client_->write(std::move(write_batch));
  block_chain_.setAddedBlockId(block_id);
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
        [&new_block, category_id = category_id, &write_batch, &last_raw_block, this](auto&& update) {
          auto block_updates =
              handleCategoryUpdates(new_block.id(), category_id, std::forward<decltype(update)>(update), write_batch);
          addRootHash(category_id, last_raw_block, block_updates);
          new_block.add(category_id, std::move(block_updates));
        },
        std::move(update));
  }
  new_block.data.parent_digest = parent_digest_future.get();
  last_raw_block.parent_digest = new_block.data.parent_digest;
  block_chain_.addBlock(new_block, write_batch);
  LOG_DEBUG(CAT_BLOCK_LOG, "Writing block [" << new_block.id() << "] to the blocks cf");
  write_batch.put(detail::BLOCKS_CF, Block::generateKey(new_block.id()), Block::serialize(new_block));
  add_metrics_comp_.UpdateAggregator();
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

const Category* KeyValueBlockchain::getCategoryPtr(const std::string& cat_id) const {
  auto it = categories_.find(cat_id);
  if (it == categories_.cend()) {
    return nullptr;
  }
  return &it->second;
}

Category* KeyValueBlockchain::getCategoryPtr(const std::string& cat_id) {
  auto it = categories_.find(cat_id);
  if (it == categories_.end()) {
    return nullptr;
  }
  return &it->second;
}

const Category& KeyValueBlockchain::getCategoryRef(const std::string& cat_id) const {
  const auto category = getCategoryPtr(cat_id);
  if (!category) {
    throw std::runtime_error{"Category does not exist = " + cat_id};
  }
  return *category;
}

Category& KeyValueBlockchain::getCategoryRef(const std::string& cat_id) {
  const auto category = getCategoryPtr(cat_id);
  if (!category) {
    throw std::runtime_error{"Category does not exist = " + cat_id};
  }
  return *category;
}

std::optional<Value> KeyValueBlockchain::get(const std::string& category_id,
                                             const std::string& key,
                                             BlockId block_id) const {
  diagnostics::TimeRecorder<true> scoped_timer(*histograms_.get);
  const auto category = getCategoryPtr(category_id);
  if (!category) {
    return std::nullopt;
  }
  std::optional<Value> ret;
  std::visit([&key, &block_id, &ret](const auto& category) { ret = category.get(key, block_id); }, *category);
  return ret;
}

std::optional<Value> KeyValueBlockchain::getLatest(const std::string& category_id, const std::string& key) const {
  diagnostics::TimeRecorder<true> scoped_timer(*histograms_.getLatest);
  const auto category = getCategoryPtr(category_id);
  if (!category) {
    return std::nullopt;
  }
  std::optional<Value> ret;
  std::visit([&key, &ret](const auto& category) { ret = category.getLatest(key); }, *category);
  return ret;
}

void KeyValueBlockchain::multiGet(const std::string& category_id,
                                  const std::vector<std::string>& keys,
                                  const std::vector<BlockId>& versions,
                                  std::vector<std::optional<Value>>& values) const {
  diagnostics::TimeRecorder<true> scoped_timer(*histograms_.multiGet);
  const auto category = getCategoryPtr(category_id);
  if (!category) {
    nullopts(values, keys.size());
    return;
  }
  std::visit([&keys, &versions, &values](const auto& category) { category.multiGet(keys, versions, values); },
             *category);
}

void KeyValueBlockchain::multiGetLatest(const std::string& category_id,
                                        const std::vector<std::string>& keys,
                                        std::vector<std::optional<Value>>& values) const {
  diagnostics::TimeRecorder<true> scoped_timer(*histograms_.multiGetLatest);
  const auto category = getCategoryPtr(category_id);
  if (!category) {
    nullopts(values, keys.size());
    return;
  }
  std::visit([&keys, &values](const auto& category) { category.multiGetLatest(keys, values); }, *category);
}

std::optional<categorization::TaggedVersion> KeyValueBlockchain::getLatestVersion(const std::string& category_id,
                                                                                  const std::string& key) const {
  const auto category = getCategoryPtr(category_id);
  if (!category) {
    return std::nullopt;
  }
  std::optional<categorization::TaggedVersion> ret;
  std::visit([&key, &ret](const auto& category) { ret = category.getLatestVersion(key); }, *category);
  return ret;
}

void KeyValueBlockchain::multiGetLatestVersion(
    const std::string& category_id,
    const std::vector<std::string>& keys,
    std::vector<std::optional<categorization::TaggedVersion>>& versions) const {
  const auto category = getCategoryPtr(category_id);
  if (!category) {
    nullopts(versions, keys.size());
    return;
  }
  std::visit([&keys, &versions](const auto& catagory) { catagory.multiGetLatestVersion(keys, versions); }, *category);
}

std::optional<Updates> KeyValueBlockchain::getBlockUpdates(BlockId block_id) const {
  auto raw = getRawBlock(block_id);
  if (!raw) {
    return std::nullopt;
  }
  return Updates{std::move(raw->data.updates)};
}

std::map<std::string, std::vector<std::string>> KeyValueBlockchain::getBlockStaleKeys(BlockId block_id) const {
  // Get block node from storage
  auto block = block_chain_.getBlock(block_id);
  if (!block) {
    const auto msg = "Failed to get block node for block ID = " + std::to_string(block_id);
    throw std::runtime_error{msg};
  }

  std::map<std::string, std::vector<std::string>> stale_keys;
  for (auto&& [category_id, update_info] : block.value().data.categories_updates_info) {
    stale_keys[category_id] =
        std::visit([&block_id, category_id = category_id, this](
                       const auto& update_info) { return getStaleKeys(block_id, category_id, update_info); },
                   update_info);
  }
  return stale_keys;
}

std::map<std::string, std::set<std::string>> KeyValueBlockchain::getStaleActiveKeys(BlockId block_id) const {
  // Get block node from storage
  auto block = block_chain_.getBlock(block_id);
  if (!block) {
    const auto msg = "Failed to get block node for block ID = " + std::to_string(block_id);
    throw std::runtime_error{msg};
  }

  std::map<std::string, std::set<std::string>> stale_keys;
  for (auto&& [category_id, update_info] : block.value().data.categories_updates_info) {
    stale_keys[category_id] =
        std::visit([&block_id, category_id = category_id, this](
                       const auto& update_info) { return getStaleActiveKeys(block_id, category_id, update_info); },
                   update_info);
  }
  return stale_keys;
}

void KeyValueBlockchain::trimBlocksFromSnapshot(BlockId block_id_at_checkpoint) {
  ConcordAssertGE(block_id_at_checkpoint, INITIAL_GENESIS_BLOCK_ID);
  ConcordAssertLE(block_id_at_checkpoint, getLastReachableBlockId());
  while (block_id_at_checkpoint < getLastReachableBlockId()) {
    LOG_INFO(GL,
             "Deleting last reachable block = " << getLastReachableBlockId() << ", DB checkpoint = " << db()->path());
    deleteLastReachableBlock();
  }
}

/////////////////////// Delete block ///////////////////////
bool KeyValueBlockchain::deleteBlock(const BlockId& block_id) {
  diagnostics::TimeRecorder scoped_timer(*histograms_.deleteBlock);
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

  auto currTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
  if (currTime - last_dump_time_ >= dump_delete_metrics_interval_) {
    last_dump_time_ = currTime;
    auto merkle_deleted_from_last_dump = merkle_num_of_deleted_keys_.Get().Get() - latest_deleted_merkle_dump;
    latest_deleted_merkle_dump = merkle_num_of_deleted_keys_.Get().Get();
    auto versioned_deleted_from_last_dump = versioned_num_of_deletes_keys_.Get().Get() - latest_deleted_versioned;
    latest_deleted_versioned = versioned_num_of_deletes_keys_.Get().Get();
    auto immutable_deleted_from_last_dump = immutable_num_of_deleted_keys_.Get().Get() - latest_deleted_immutbale;
    latest_deleted_immutbale = immutable_num_of_deleted_keys_.Get().Get();
    auto total_deleted_keys_from_last_dump =
        merkle_deleted_from_last_dump + versioned_deleted_from_last_dump + immutable_deleted_from_last_dump;
    auto total_deleted_keys = merkle_num_of_deleted_keys_.Get().Get() + versioned_num_of_deletes_keys_.Get().Get() +
                              immutable_num_of_deleted_keys_.Get().Get();
    LOG_INFO(CAT_BLOCK_LOG,
             "kv_blockchain delete metrics:" << KVLOG(genesis_block_id,
                                                      last_reachable_block_id,
                                                      merkle_deleted_from_last_dump,
                                                      versioned_deleted_from_last_dump,
                                                      immutable_deleted_from_last_dump,
                                                      total_deleted_keys_from_last_dump,
                                                      total_deleted_keys));
  }

  if (block_id == last_reachable_block_id && block_id == genesis_block_id) {
    throw std::logic_error{"Deleting the only block in the system is not supported"};
  } else if (block_id == last_reachable_block_id) {
    deleteLastReachableBlock();
  } else if (block_id == genesis_block_id) {
    deleteGenesisBlock();
  } else {
    throw std::invalid_argument{"Cannot delete blocks in the middle of the blockchain"};
  }
  // Lets update the delete metrics component
  delete_metrics_comp_.UpdateAggregator();
  return true;
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
  // If a versioned/Merkle category contains more keys than concurrent_threshold
  // It will be executed in a separate thread.
  const auto concurrent_threshold = 10;
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
  const auto start = std::chrono::steady_clock::now();
  std::vector<std::future<void>> futures;
  futures.reserve((*block).data.categories_updates_info.size());
  std::vector<detail::LocalWriteBatch> write_batches;
  write_batches.reserve((*block).data.categories_updates_info.size());
  for (auto&& [category_id, update_info] : (*block).data.categories_updates_info) {
    uint64_t num_of_keys = 0;
    // Decide whether to perform the deletion on a separate thread based on the number of keys
    // Immutable category, should always be sequential as its deletion is fast.
    if (std::holds_alternative<VersionedOutput>(update_info)) {
      num_of_keys = std::get<VersionedOutput>(update_info).keys.size();
    } else if (std::holds_alternative<BlockMerkleOutput>(update_info)) {
      num_of_keys = std::get<BlockMerkleOutput>(update_info).keys.size();
    }
    std::visit(
        [genesis_id, category_id = category_id, &write_batches, &futures, &num_of_keys, this](const auto& update_info) {
          write_batches.push_back(detail::LocalWriteBatch());
          if (num_of_keys > concurrent_threshold) {
            futures.push_back(prunning_thread_pool_.async(
                [&](BlockId genesis_id,
                    std::string category_id,
                    const auto& update_info,
                    detail::LocalWriteBatch& write_batch) {
                  LOG_DEBUG(CAT_BLOCK_LOG, "Deletion of " << category_id << " will be performed in a seperate thread");
                  deleteGenesisBlock(genesis_id, category_id, update_info, write_batch);
                },
                genesis_id,
                category_id,
                std::ref(update_info),
                std::ref(write_batches.back())));

          } else {
            deleteGenesisBlock(genesis_id, category_id, update_info, write_batches.back());
          }
        },
        update_info);
  }
  for (auto& future : futures) {
    future.get();
  }
  for (auto& write_batche : write_batches) {
    write_batche.moveToBatch(write_batch);
  }

  native_client_->write(std::move(write_batch));

  auto jobDuration =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
  LOG_INFO(CAT_BLOCK_LOG, "CONC_DEL deleteGenesisBlock took " << jobDuration << " micro, block " << genesis_id);
  // Increment the genesis block ID cache.
  block_chain_.setGenesisBlockId(genesis_id + 1);
}

// 1 - Get last id block from DB.
// 2 - Iterate over the update_info and calls the corresponding deleteLastReachableBlock
// 3 - Perform the delete
// 4 - Increment the genesis block id.
void KeyValueBlockchain::deleteLastReachableBlock() {
  diagnostics::TimeRecorder scoped_timer(*histograms_.deleteLastReachableBlock);
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

  // Iterate over groups and call corresponding deleteLastReachableBlock,
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

std::vector<std::string> KeyValueBlockchain::getStaleKeys(BlockId block_id,
                                                          const std::string& category_id,
                                                          const ImmutableOutput& updates_info) const {
  return std::get<detail::ImmutableKeyValueCategory>(getCategoryRef(category_id))
      .getBlockStaleKeys(block_id, updates_info);
}

std::vector<std::string> KeyValueBlockchain::getStaleKeys(BlockId block_id,
                                                          const std::string& category_id,
                                                          const VersionedOutput& updates_info) const {
  return std::get<detail::VersionedKeyValueCategory>(getCategoryRef(category_id))
      .getBlockStaleKeys(block_id, updates_info);
}

std::vector<std::string> KeyValueBlockchain::getStaleKeys(BlockId block_id,
                                                          const std::string& category_id,
                                                          const BlockMerkleOutput& updates_info) const {
  return std::get<detail::BlockMerkleCategory>(getCategoryRef(category_id)).getBlockStaleKeys(block_id, updates_info);
}

std::set<std::string> KeyValueBlockchain::getStaleActiveKeys(BlockId block_id,
                                                             const std::string& category_id,
                                                             const ImmutableOutput& updates_info) const {
  return std::get<detail::ImmutableKeyValueCategory>(getCategoryRef(category_id))
      .getStaleActiveKeys(block_id, updates_info);
}

std::set<std::string> KeyValueBlockchain::getStaleActiveKeys(BlockId block_id,
                                                             const std::string& category_id,
                                                             const VersionedOutput& updates_info) const {
  return std::get<detail::VersionedKeyValueCategory>(getCategoryRef(category_id))
      .getStaleActiveKeys(block_id, updates_info);
}

std::set<std::string> KeyValueBlockchain::getStaleActiveKeys(BlockId block_id,
                                                             const std::string& category_id,
                                                             const BlockMerkleOutput& updates_info) const {
  return std::get<detail::BlockMerkleCategory>(getCategoryRef(category_id)).getStaleActiveKeys(block_id, updates_info);
}

// Deletes per category
void KeyValueBlockchain::deleteGenesisBlock(BlockId block_id,
                                            const std::string& category_id,
                                            const ImmutableOutput& updates_info,
                                            detail::LocalWriteBatch& batch) {
  immutable_num_of_deleted_keys_ += std::get<detail::ImmutableKeyValueCategory>(getCategoryRef(category_id))
                                        .deleteGenesisBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteGenesisBlock(BlockId block_id,
                                            const std::string& category_id,
                                            const VersionedOutput& updates_info,
                                            detail::LocalWriteBatch& batch) {
  versioned_num_of_deletes_keys_ += std::get<detail::VersionedKeyValueCategory>(getCategoryRef(category_id))
                                        .deleteGenesisBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteGenesisBlock(BlockId block_id,
                                            const std::string& category_id,
                                            const BlockMerkleOutput& updates_info,
                                            detail::LocalWriteBatch& batch) {
  merkle_num_of_deleted_keys_ += std::get<detail::BlockMerkleCategory>(getCategoryRef(category_id))
                                     .deleteGenesisBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteLastReachableBlock(BlockId block_id,
                                                  const std::string& category_id,
                                                  const ImmutableOutput& updates_info,
                                                  storage::rocksdb::NativeWriteBatch& batch) {
  immutable_num_of_deleted_keys_ += updates_info.tagged_keys.size();
  std::get<detail::ImmutableKeyValueCategory>(getCategoryRef(category_id))
      .deleteLastReachableBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteLastReachableBlock(BlockId block_id,
                                                  const std::string& category_id,
                                                  const VersionedOutput& updates_info,
                                                  storage::rocksdb::NativeWriteBatch& batch) {
  versioned_num_of_deletes_keys_ += updates_info.keys.size();
  std::get<detail::VersionedKeyValueCategory>(getCategoryRef(category_id))
      .deleteLastReachableBlock(block_id, updates_info, batch);
}

void KeyValueBlockchain::deleteLastReachableBlock(BlockId block_id,
                                                  const std::string& category_id,
                                                  const BlockMerkleOutput& updates_info,
                                                  storage::rocksdb::NativeWriteBatch& batch) {
  merkle_num_of_deleted_keys_ += updates_info.keys.size();
  std::get<detail::BlockMerkleCategory>(getCategoryRef(category_id))
      .deleteLastReachableBlock(block_id, updates_info, batch);
}

// Updates per category
void KeyValueBlockchain::insertCategoryMapping(const std::string& cat_id, const CATEGORY_TYPE type) {
  // check if we know this category type already
  ConcordAssertEQ(category_types_.count(cat_id), 0);

  // cache the type in memory and store it in DB
  const auto inserted = category_types_.try_emplace(cat_id, type).second;
  if (!inserted) {
    LOG_FATAL(CAT_BLOCK_LOG, "Category [" << cat_id << "] already exists in type map");
    ConcordAssert(false);
  }
  native_client_->put(detail::CAT_ID_TYPE_CF, cat_id, std::string(1, static_cast<char>(type)));
}

void KeyValueBlockchain::addNewCategory(const std::string& cat_id, CATEGORY_TYPE type) {
  insertCategoryMapping(cat_id, type);
  auto inserted = false;
  switch (type) {
    case CATEGORY_TYPE::block_merkle:
      inserted = categories_.try_emplace(cat_id, detail::BlockMerkleCategory{native_client_}).second;
      break;
    case CATEGORY_TYPE::immutable:
      inserted = categories_.try_emplace(cat_id, detail::ImmutableKeyValueCategory{cat_id, native_client_}).second;
      break;
    case CATEGORY_TYPE::versioned_kv:
      inserted = categories_.try_emplace(cat_id, detail::VersionedKeyValueCategory{cat_id, native_client_}).second;
      break;
    default:
      ConcordAssert(false);
  }
  if (!inserted) {
    LOG_FATAL(CAT_BLOCK_LOG, "Category [" << cat_id << "] already exists in categories map");
    ConcordAssert(false);
  }
}

BlockMerkleOutput KeyValueBlockchain::handleCategoryUpdates(BlockId block_id,
                                                            const std::string& category_id,
                                                            BlockMerkleInput&& updates,
                                                            concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  auto itr = categories_.find(category_id);
  if (itr == categories_.end()) {
    throw std::runtime_error{"Category does not exist = " + category_id};
  }
  LOG_DEBUG(CAT_BLOCK_LOG, "Adding updates of block [" << block_id << "] to the BlockMerkleCategory");
  merkle_num_of_keys_ += updates.kv.size();
  return std::get<detail::BlockMerkleCategory>(itr->second).add(block_id, std::move(updates), write_batch);
}

VersionedOutput KeyValueBlockchain::handleCategoryUpdates(BlockId block_id,
                                                          const std::string& category_id,
                                                          VersionedInput&& updates,
                                                          concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  auto itr = categories_.find(category_id);
  if (itr == categories_.end()) {
    throw std::runtime_error{"Category does not exist = " + category_id};
  }
  versioned_num_of_keys_ += updates.kv.size();
  LOG_DEBUG(CAT_BLOCK_LOG, "Adding updates of block [" << block_id << "] to the VersionedKeyValueCategory");
  return std::get<detail::VersionedKeyValueCategory>(itr->second).add(block_id, std::move(updates), write_batch);
}

ImmutableOutput KeyValueBlockchain::handleCategoryUpdates(BlockId block_id,
                                                          const std::string& category_id,
                                                          ImmutableInput&& updates,
                                                          concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  auto itr = categories_.find(category_id);
  if (itr == categories_.end()) {
    throw std::runtime_error{"Category does not exist = " + category_id};
  }
  immutable_num_of_keys_ += updates.kv.size();
  LOG_DEBUG(CAT_BLOCK_LOG, "Adding updates of block [" << block_id << "] to the ImmutableKeyValueCategory");
  return std::get<detail::ImmutableKeyValueCategory>(itr->second).add(block_id, std::move(updates), write_batch);
}

/////////////////////// state transfer blockchain ///////////////////////
void KeyValueBlockchain::addRawBlock(const RawBlock& block, const BlockId& block_id, bool lastBlock) {
  diagnostics::TimeRecorder scoped_timer(*histograms_.addRawBlock);
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

  if (lastBlock) {
    try {
      linkSTChainFrom(last_reachable_block + 1);
    } catch (const std::exception& e) {
      LOG_FATAL(CAT_BLOCK_LOG,
                "Aborting due to failure to link chains after block has been added, reason: " << e.what());
      std::terminate();
    } catch (...) {
      LOG_FATAL(CAT_BLOCK_LOG, "Aborting due to failure to link chains after block has been added");
      std::terminate();
    }
  }
}

std::optional<RawBlock> KeyValueBlockchain::getRawBlock(const BlockId& block_id) const {
  diagnostics::TimeRecorder<true> scoped_timer(*histograms_.getRawBlock);
  const auto last_reachable_block = getLastReachableBlockId();

  // Try to take it from the ST chain
  if (block_id > last_reachable_block) {
    return state_transfer_block_chain_.getRawBlock(block_id);
  }
  // Try from the blockchain itself
  return block_chain_.getRawBlock(block_id, categories_);
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

size_t KeyValueBlockchain::linkUntilBlockId(BlockId from_block_id, BlockId until_block_id) {
  static constexpr uint64_t report_thresh{1000};
  static uint64_t report_counter{};
  const auto last_block_id = state_transfer_block_chain_.getLastBlockId();

  if (last_block_id == 0) {
    return 0;
  }

  concord::util::DurationTracker<std::chrono::milliseconds> link_duration("link_duration", true);
  for (auto i = from_block_id; i <= until_block_id; ++i) {
    auto raw_block = state_transfer_block_chain_.getRawBlock(i);
    if (!raw_block) {
      // we didn't find the next block
      return i - from_block_id;
    }

    // First prune and then link the block to the chain. Rationale is that this will preserve the same order of block
    // deletes relative to block adds on source and destination replicas.
    pruneOnSTLink(*raw_block);
    writeSTLinkTransaction(i, *raw_block);
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
  return until_block_id - from_block_id + 1;
}

// tries to remove blocks form the state transfer chain to the blockchain
void KeyValueBlockchain::linkSTChainFrom(BlockId block_id) {
  const auto last_block_id = state_transfer_block_chain_.getLastBlockId();
  if (last_block_id == 0) return;

  for (auto i = block_id; i <= last_block_id; ++i) {
    auto raw_block = state_transfer_block_chain_.getRawBlock(i);
    if (!raw_block) {
      return;
    }
    // First prune and then link the block to the chain. Rationale is that this will preserve the same order of block
    // deletes relative to block adds on source and destination replicas.
    pruneOnSTLink(*raw_block);
    writeSTLinkTransaction(i, *raw_block);
  }

  // Linking has fully completed and we should not have any more ST temporary blocks left. Therefore, make sure we don't
  // have any value for the latest ST temporary block ID cache.
  state_transfer_block_chain_.resetChain();
}

void KeyValueBlockchain::pruneOnSTLink(const RawBlock& block) {
  auto cat_it = block.data.updates.kv.find(kConcordInternalCategoryId);
  if (cat_it == block.data.updates.kv.cend()) {
    return;
  }
  const auto& internal_kvs = std::get<VersionedInput>(cat_it->second).kv;
  auto key_it = internal_kvs.find(keyTypes::genesis_block_key);
  if (key_it != internal_kvs.cend()) {
    const auto block_genesis_id = concordUtils::fromBigEndianBuffer<BlockId>(key_it->second.data.data());
    if (getGenesisBlockId() >= INITIAL_GENESIS_BLOCK_ID && getGenesisBlockId() < getLastReachableBlockId()) {
      for (auto i = getGenesisBlockId(); i < block_genesis_id; i++) {
        ConcordAssert(deleteBlock(i));
      }
    }
  }
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
