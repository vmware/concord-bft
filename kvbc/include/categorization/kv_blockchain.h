// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
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
#include "blocks.h"
#include "blockchain.h"
#include "immutable_kv_category.h"
#include "block_merkle_category.h"
#include "versioned_kv_category.h"
#include "kv_types.hpp"
#include "categorization/types.h"
#include "thread_pool.hpp"
#include "Metrics.hpp"
#include "scope_exit.hpp"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <type_traits>

namespace concord::kvbc::categorization {

// Represents a key-value blockchain.
//
// -------------------------------------- Notes on multi-threading and Pruning --------------------------------------
// * Adding blocks via addBlock() and addRawBlock() can be done from a single thread only.
// * Reading and calling const methods can be done in parallel to adding blocks and from multiple threads.
// * Pruning via the deleteBlocksUntil() method can be done from a single thread only and in parallel to the thread that
//   is adding blocks and the threads that are reading. Essentially, deleteBlocksUntil() will pause if there are
//   blocks to be added and resume once they are added. Adding blocks has a higher priority over pruning.
// * The deleteBlock() method should not be called if multiple threads are in use.
class KeyValueBlockchain {
  using VersionedRawBlock = std::pair<BlockId, std::optional<categorization::RawBlockData>>;

 public:
  // Creates a key-value blockchain.
  // If `category_types` is nullopt, the persisted categories in storage will be used.
  // If `category_types` has a value, it should contain all persisted categories in storage at a minimum. New ones will
  // be created and persisted.
  // Users are required to pass a value for `category_types` on first construction (i.e. a new blockchain) in order to
  // specify the categories in use. Failure to do so will generate an exception.
  KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
                     bool link_st_chain,
                     const std::optional<std::map<std::string, CATEGORY_TYPE>>& category_types = std::nullopt);
  /////////////////////// Add Block ///////////////////////

  BlockId addBlock(Updates&& updates);

  /////////////////////// Delete block ///////////////////////

  bool deleteBlock(const BlockId& blockId);
  void deleteLastReachableBlock();

  // Deletes blocks in the [genesis, until) range. If the until value is bigger than the last block, blocks in the
  // range [genesis, lastBlock] will be deleted.
  // Deletes one block at a time. Therefore, the operation is atomic only for a single block deletion. If something
  // fails, less blocks than requested might be deleted.
  // Can be called from a single thread while another thread calls addBlock() or addRawBlock().
  // Returns the last deleted block ID.
  // Throws on errors or if until <= genesis .
  BlockId deleteBlocksUntil(BlockId until);

  /////////////////////// Raw Blocks ///////////////////////

  // Adds raw block and tries to link the state transfer blockchain to the main blockchain
  void addRawBlock(const RawBlock& block, const BlockId& block_id);
  std::optional<RawBlock> getRawBlock(const BlockId& block_id) const;

  /////////////////////// Info ///////////////////////
  BlockId getGenesisBlockId() const { return block_chain_.getGenesisBlockId(); }
  BlockId getLastReachableBlockId() const { return block_chain_.getLastReachableBlockId(); }
  std::optional<BlockId> getLastStatetransferBlockId() const {
    if (state_transfer_block_chain_.getLastBlockId() == 0) return std::nullopt;
    return state_transfer_block_chain_.getLastBlockId();
  }

  std::optional<Hash> parentDigest(BlockId block_id) const;
  bool hasBlock(BlockId block_id) const;

  std::vector<std::string> getStaleKeys(BlockId block_id,
                                        const std::string& category_id,
                                        const ImmutableOutput& updates_info) const;

  std::vector<std::string> getStaleKeys(BlockId block_id,
                                        const std::string& category_id,
                                        const VersionedOutput& updates_info) const;

  std::vector<std::string> getStaleKeys(BlockId block_id,
                                        const std::string& category_id,
                                        const BlockMerkleOutput& updates_info) const;

  /////////////////////// Read interface ///////////////////////

  // Gets the value of a key by the exact blockVersion.
  std::optional<Value> get(const std::string& category_id, const std::string& key, BlockId block_id) const;

  std::optional<Value> getLatest(const std::string& category_id, const std::string& key) const;

  void multiGet(const std::string& category_id,
                const std::vector<std::string>& keys,
                const std::vector<BlockId>& versions,
                std::vector<std::optional<Value>>& values) const;

  void multiGetLatest(const std::string& category_id,
                      const std::vector<std::string>& keys,
                      std::vector<std::optional<Value>>& values) const;

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string& category_id,
                                                                const std::string& key) const;

  void multiGetLatestVersion(const std::string& category_id,
                             const std::vector<std::string>& keys,
                             std::vector<std::optional<categorization::TaggedVersion>>& versions) const;

  // Get the updates that were used to create `block_id`.
  std::optional<Updates> getBlockUpdates(BlockId block_id) const;

  // Get a map of category_id and stale keys for `block_id`
  std::map<std::string, std::vector<std::string>> getBlockStaleKeys(BlockId block_id) const;

  // Get a map from category ID -> type for all known categories in the blockchain.
  const std::map<std::string, CATEGORY_TYPE>& blockchainCategories() const { return category_types_; }

  std::shared_ptr<concord::storage::rocksdb::NativeClient> db() { return native_client_; }
  std::shared_ptr<const concord::storage::rocksdb::NativeClient> db() const { return native_client_; }

 private:
  BlockId addBlock(CategoryInput&& category_updates, concord::storage::rocksdb::NativeWriteBatch& write_batch);

  // tries to link the state transfer chain to the main blockchain
  void linkSTChainFrom(BlockId block_id);
  void writeSTLinkTransaction(const BlockId block_id, RawBlock& block);

  // computes the digest of a raw block which is the parent of block_id i.e. block_id - 1
  std::future<BlockDigest> computeParentBlockDigest(const BlockId block_id, VersionedRawBlock&& cached_raw_block);

  /////////////////////// Categories operations ///////////////////////

  void initNewBlockchainCategories(const std::optional<std::map<std::string, CATEGORY_TYPE>>& category_types);
  void initExistingBlockchainCategories(const std::optional<std::map<std::string, CATEGORY_TYPE>>& category_types);
  // iterate over the categories column family and instantiate the stored categories.
  void loadCategories();
  // insert a new category into the categories column family and instantiate it.
  void insertCategoryMapping(const std::string& cat_id, const CATEGORY_TYPE type);
  void addNewCategory(const std::string& cat_id, CATEGORY_TYPE type);

  // Return nullptr if the category doesn't exist.
  const Category* getCategoryPtr(const std::string& cat_id) const;
  Category* getCategoryPtr(const std::string& cat_id);

  // Throw if the category doesn't exist.
  const Category& getCategoryRef(const std::string& cat_id) const;
  Category& getCategoryRef(const std::string& cat_id);

  /////////////////////// deletes ///////////////////////

  void deleteStateTransferBlock(const BlockId block_id);
  void deleteGenesisBlock();

  // Delete per category
  void deleteGenesisBlock(BlockId block_id,
                          const std::string& category_id,
                          const ImmutableOutput& updates_info,
                          storage::rocksdb::NativeWriteBatch&);

  void deleteGenesisBlock(BlockId block_id,
                          const std::string& category_id,
                          const VersionedOutput& updates_info,
                          storage::rocksdb::NativeWriteBatch&);

  void deleteGenesisBlock(BlockId block_id,
                          const std::string& category_id,
                          const BlockMerkleOutput& updates_info,
                          storage::rocksdb::NativeWriteBatch&);

  void deleteLastReachableBlock(BlockId block_id,
                                const std::string& category_id,
                                const ImmutableOutput& updates_info,
                                storage::rocksdb::NativeWriteBatch&);

  void deleteLastReachableBlock(BlockId block_id,
                                const std::string& category_id,
                                const VersionedOutput& updates_info,
                                storage::rocksdb::NativeWriteBatch&);

  void deleteLastReachableBlock(BlockId block_id,
                                const std::string& category_id,
                                const BlockMerkleOutput& updates_info,
                                storage::rocksdb::NativeWriteBatch&);

  /////////////////////// Updates ///////////////////////

  // Update per category
  BlockMerkleOutput handleCategoryUpdates(BlockId block_id,
                                          const std::string& category_id,
                                          BlockMerkleInput&& updates,
                                          concord::storage::rocksdb::NativeWriteBatch& write_batch);

  VersionedOutput handleCategoryUpdates(BlockId block_id,
                                        const std::string& category_id,
                                        VersionedInput&& updates,
                                        concord::storage::rocksdb::NativeWriteBatch& write_batch);
  ImmutableOutput handleCategoryUpdates(BlockId block_id,
                                        const std::string& category_id,
                                        ImmutableInput&& updates,
                                        concord::storage::rocksdb::NativeWriteBatch& write_batch);

  // Utility that allows calling addBlock methods concurrently with deleteBlocksUntil(), i.e. pruning.
  template <typename Fn>
  typename std::invoke_result<Fn>::type addBlock(Fn f) {
    // Set prior to locking write_mtx_ in order to avoid potentially unfair mutex policies.
    pending_add_ = true;
    auto l = std::unique_lock{write_mtx_};
    auto s = util::ScopeExit{[&]() {
      l.unlock();
      // Unset prior to notifying the pruning thread, ensuring it will not miss the signal.
      pending_add_ = false;
      add_completed_.notify_one();
    }};
    return f();
  }

  /////////////////////// Members ///////////////////////

  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  CategoriesMap categories_;
  std::map<std::string, CATEGORY_TYPE> category_types_;
  detail::Blockchain block_chain_;
  detail::Blockchain::StateTransfer state_transfer_block_chain_;

  // Holds the last raw block of the last corresponding block that was added.
  // Used to save construction of a raw block per addBlock method.
  // E.L - compare this with getRawBlock to see they are equal
  VersionedRawBlock last_raw_block_;

  // currently we are operating with single thread
  util::ThreadPool thread_pool_{1};

  // Whether block addition (raw from ST or user-supplied) is pending. This flag is set prior to locking write_mtx_ in
  // order to avoid potentially unfair mutex policies.
  std::atomic_bool pending_add_{false};
  // Used to serialize writes, namely block addition and pruning.
  std::mutex write_mtx_;
  // Used to signal the pruning thread that an add operation has completed.
  std::condition_variable add_completed_;

  // metrics
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
  concordMetrics::Component delete_metrics_comp_;
  concordMetrics::CounterHandle versioned_num_of_deletes_keys_;
  concordMetrics::CounterHandle immutable_num_of_deleted_keys_;
  concordMetrics::CounterHandle merkle_num_of_deleted_keys_;

  concordMetrics::Component add_metrics_comp_;
  concordMetrics::CounterHandle versioned_num_of_keys_;
  concordMetrics::CounterHandle immutable_num_of_keys_;
  concordMetrics::CounterHandle merkle_num_of_keys_;

 public:
  struct KeyValueBlockchain_tester {
    void loadCategories(KeyValueBlockchain& kvbc) { kvbc.loadCategories(); }
    const auto& getCategories(const KeyValueBlockchain& kvbc) const { return kvbc.categories_; }

    const auto& getStateTransferBlockchain(KeyValueBlockchain& kvbc) { return kvbc.state_transfer_block_chain_; }

    const Category& getCategory(const std::string& cat_id, KeyValueBlockchain& kvbc) const {
      return kvbc.getCategoryRef(cat_id);
    }

    detail::Blockchain& getBlockchain(KeyValueBlockchain& kvbc) { return kvbc.block_chain_; }

    const VersionedRawBlock& getLastRawBlock(KeyValueBlockchain& kvbc) { return kvbc.last_raw_block_; }
  };  // namespace concord::kvbc::categorization

  std::string getPruningStatus();

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    aggregator_ = aggregator;
    delete_metrics_comp_.SetAggregator(aggregator_);
    add_metrics_comp_.SetAggregator(aggregator);
  }
  friend struct KeyValueBlockchain_tester;
};

}  // namespace concord::kvbc::categorization
