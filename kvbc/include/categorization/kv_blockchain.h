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
#include "blockchain_misc.hpp"
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
#include "diagnostics.h"
#include "performance_handler.h"
#include "bftengine/ReplicaConfig.hpp"
#include "categorized_kvbc_msgs.cmf.hpp"

#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace concord::kvbc::categorization {

class KeyValueBlockchain {
  using VersionedRawBlock = std::pair<BlockId, std::optional<categorization::RawBlockData>>;

 public:
  // Creates a key-value blockchain.
  // If `category_types` is nullopt, the persisted categories in storage will be used.
  // If `category_types` has a value, it should contain all persisted categories in storage at a minimum. New ones
  // will be created and persisted. Users are required to pass a value for `category_types` on first construction
  // (i.e. a new blockchain) in order to specify the categories in use. Failure to do so will generate an exception.
  KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
                     bool link_st_chain,
                     const std::optional<std::map<std::string, CATEGORY_TYPE>>& category_types = std::nullopt);
  /////////////////////// Add Block ///////////////////////

  BlockId addBlock(Updates&& updates);

  /////////////////////// Delete block ///////////////////////

  bool deleteBlock(const BlockId& blockId);
  void deleteLastReachableBlock();

  /////////////////////// Raw Blocks ///////////////////////

  // Adds raw block and tries to link the state transfer blockchain to the main blockchain
  // If linkSTChainFrom is true, try to remove blocks form the state transfer chain to the blockchain
  void addRawBlock(const RawBlock& block, const BlockId& block_id, bool lastBlock = true);
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

  std::set<std::string> getStaleActiveKeys(BlockId block_id,
                                           const std::string& category_id,
                                           const ImmutableOutput& updates_info) const;

  std::set<std::string> getStaleActiveKeys(BlockId block_id,
                                           const std::string& category_id,
                                           const VersionedOutput& updates_info) const;

  std::set<std::string> getStaleActiveKeys(BlockId block_id,
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
  std::map<std::string, std::set<std::string>> getStaleActiveKeys(BlockId block_id) const;

  // Get a map from category ID -> type for all known categories in the blockchain.
  const std::map<std::string, CATEGORY_TYPE>& blockchainCategories() const { return category_types_; }

  // from_block_id is given as a hint, as supposed to be the next block after the last reachable block.
  // until_block_id is the maximum block ID to be linked. If there are blocks with a block ID larger than
  // until_block_id, they can be linked on future calls.
  // returns the number of blocks connected
  size_t linkUntilBlockId(BlockId from_block_id, BlockId until_block_id);

  std::shared_ptr<concord::storage::rocksdb::NativeClient> db() { return native_client_; }
  std::shared_ptr<const concord::storage::rocksdb::NativeClient> db() const { return native_client_; }

  // Trims the DB snapshot such that its last reachable block is equal to `block_id_at_checkpoint`.
  // This method is supposed to be called on DB snapshots only and not on the actual blockchain.
  // Precondition1: The current KeyValueBlockchain instance points to a DB snapshot.
  // Precondition2: `block_id_at_checkpoint` >= INITIAL_GENESIS_BLOCK_ID
  // Precondition3: `block_id_at_checkpoint` <= getLastReachableBlockId()
  void trimBlocksFromSnapshot(BlockId block_id_at_checkpoint);

  // The key used in the default column family for persisting the current public state hash.
  static std::string publicStateHashKey();

 private:
  BlockId addBlock(CategoryInput&& category_updates, concord::storage::rocksdb::NativeWriteBatch& write_batch);

  // tries to link the state transfer chain to the main blockchain
  void linkSTChainFrom(BlockId block_id);
  void writeSTLinkTransaction(const BlockId block_id, RawBlock& block);

  // If a block has the genesis ID key, prune up to it. Rationale is that this will preserve the same order of block
  // deletes relative to block adds on source and destination replicas.
  void pruneOnSTLink(const RawBlock& block);

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
                          detail::LocalWriteBatch&);

  void deleteGenesisBlock(BlockId block_id,
                          const std::string& category_id,
                          const VersionedOutput& updates_info,
                          detail::LocalWriteBatch&);

  void deleteGenesisBlock(BlockId block_id,
                          const std::string& category_id,
                          const BlockMerkleOutput& updates_info,
                          detail::LocalWriteBatch&);

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

  void addGenesisBlockKey(Updates& updates) const;

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
  // For concurrent deletion of the categories inside a block.
  util::ThreadPool prunning_thread_pool_{2};

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

  std::chrono::seconds dump_delete_metrics_interval_{bftEngine::ReplicaConfig::instance().deleteMetricsDumpInterval};
  std::chrono::seconds last_dump_time_{0};
  uint64_t latest_deleted_merkle_dump{0};
  uint64_t latest_deleted_versioned{0};
  uint64_t latest_deleted_immutbale{0};

 public:
  struct KeyValueBlockchain_tester {
    void loadCategories(KeyValueBlockchain& kvbc) { kvbc.loadCategories(); }
    const auto& getCategories(KeyValueBlockchain& kvbc) { return kvbc.categories_; }

    const auto& getStateTransferBlockchain(KeyValueBlockchain& kvbc) { return kvbc.state_transfer_block_chain_; }

    const Category& getCategory(const std::string& cat_id, KeyValueBlockchain& kvbc) const {
      return kvbc.getCategoryRef(cat_id);
    }

    detail::Blockchain& getBlockchain(KeyValueBlockchain& kvbc) { return kvbc.block_chain_; }

    const VersionedRawBlock& getLastRawBlock(KeyValueBlockchain& kvbc) { return kvbc.last_raw_block_; }
  };  // namespace concord::kvbc::categorization

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    aggregator_ = aggregator;
    delete_metrics_comp_.SetAggregator(aggregator_);
    add_metrics_comp_.SetAggregator(aggregator_);
  }
  friend struct KeyValueBlockchain_tester;

 private:
  // 5 Minutes
  static constexpr int64_t MAX_VALUE_MICROSECONDS = 1000 * 1000 * 60 * 5;
  // 1 second
  static constexpr int64_t MAX_VALUE_NANOSECONDS = 1000 * 1000 * 1000;
  using Recorder = concord::diagnostics::Recorder;

  struct Recorders {
    Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.registerComponent("kvbc",
                                       {addBlock,
                                        addRawBlock,
                                        getRawBlock,
                                        deleteBlock,
                                        deleteLastReachableBlock,
                                        get,
                                        getLatest,
                                        multiGet,
                                        multiGetLatest});
    }

    ~Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.unRegisterComponent("kvbc");
    }

    // DEFINE_SHARED_RECORDER(may_have_conflict_between, 1, MAX_VALUE_NANOSECONDS, 3,
    // concord::diagnostics::Unit::NANOSECONDS);
    DEFINE_SHARED_RECORDER(addBlock, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(addRawBlock, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(getRawBlock, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(deleteBlock, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        deleteLastReachableBlock, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(get, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(getLatest, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(multiGet, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(multiGetLatest, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  };

  static Recorders histograms_;
};

}  // namespace concord::kvbc::categorization
