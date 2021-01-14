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
#include <memory>
#include "blocks.h"
#include "blockchain.h"
#include "immutable_kv_category.h"
#include "block_merkle_category.h"
#include "versioned_kv_category.h"
#include "kv_types.hpp"
#include "categorization/types.h"
#include "thread_pool.hpp"

namespace concord::kvbc::categorization {

class KeyValueBlockchain {
  using VersionedRawBlock = std::pair<BlockId, std::optional<categorization::RawBlockData>>;

 public:
  KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client, bool link_st_chain);
  /////////////////////// Add Block ///////////////////////

  BlockId addBlock(Updates&& updates);

  /////////////////////// Delete block ///////////////////////

  bool deleteBlock(const BlockId& blockId);
  void deleteLastReachableBlock();

  /////////////////////// Raw Blocks ///////////////////////

  // Adds raw block and tries to link the state transfer blockchain to the main blockchain
  void addRawBlock(const RawBlock& block, const BlockId& block_id);
  std::optional<RawBlock> getRawBlock(const BlockId& block_id) const;

  /////////////////////// Info ///////////////////////
  BlockId getGenesisBlockId() const { return block_chain_.getGenesisBlockId(); }
  BlockId getLastReachableBlockId() const { return block_chain_.getLastReachableBlockId(); }
  std::optional<BlockId> getLastStatetransferBlockId() const { return state_transfer_block_chain_.getLastBlockId(); }

  std::optional<Hash> parentDigest(BlockId block_id) const;
  bool hasBlock(BlockId block_id) const;

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

 private:
  BlockId addBlock(CategoryInput&& category_updates, concord::storage::rocksdb::NativeWriteBatch& write_batch);

  // tries to link the state transfer chain to the main blockchain
  void linkSTChainFrom(BlockId block_id);
  void writeSTLinkTransaction(const BlockId block_id, RawBlock& block);

  // computes the digest of a raw block which is the parent of block_id i.e. block_id - 1
  std::future<BlockDigest> computeParentBlockDigest(const BlockId block_id, VersionedRawBlock&& cached_raw_block);

  /////////////////////// Categories operations ///////////////////////

  // iterate over the categories column family and instantiate the stored categories.
  void instantiateCategories();
  // insert a new category into the categories column family and instantiate it.
  bool insertCategoryMapping(const std::string& cat_id,
                             const detail::CATEGORY_TYPE type,
                             concord::storage::rocksdb::NativeWriteBatch& write_batch);


  const Category& getCategory(const std::string& cat_id) const;
  Category& getCategory(const std::string& cat_id);
  bool hasCategory(const std::string& cat_id) const;

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

  /////////////////////// Members ///////////////////////

  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  CategoriesMap categorires_;
  std::map<std::string, detail::CATEGORY_TYPE> categorires_types_;
  detail::Blockchain block_chain_;
  detail::Blockchain::StateTransfer state_transfer_block_chain_;

  // Holds the last raw block of the last corresponding block that was added.
  // Used to save construction of a raw block per addBlock method.
  // E.L - compare this with getRawBlock to see they are equal
  VersionedRawBlock last_raw_block_;

  // currently we are operating with single thread
  util::ThreadPool thread_pool_{1};

 public:
  struct KeyValueBlockchain_tester {
    void instantiateCategories(KeyValueBlockchain& kvbc) { kvbc.instantiateCategories(); }
    const auto& getCategories(KeyValueBlockchain& kvbc) { return kvbc.categorires_; }

    const std::
        variant<detail::ImmutableKeyValueCategory, detail::BlockMerkleCategory, detail::VersionedKeyValueCategory>&
        getCategory(const std::string& cat_id, KeyValueBlockchain& kvbc) const {
      return kvbc.getCategory(cat_id);
    }

    detail::Blockchain& getBlockchain(KeyValueBlockchain& kvbc) { return kvbc.block_chain_; }

    const VersionedRawBlock& getLastRawBlocked(KeyValueBlockchain& kvbc) { return kvbc.last_raw_block_; }
  };  // namespace concord::kvbc::categorization
  friend struct KeyValueBlockchain_tester;
};

}  // namespace concord::kvbc::categorization
