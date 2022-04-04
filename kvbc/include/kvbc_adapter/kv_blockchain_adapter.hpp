// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
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

#include <string>
#include <vector>
#include <memory>

#include "kv_types.hpp"
#include "rocksdb/native_client.h"
#include "categorization/base_types.h"
#include "categorization/updates.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "blockchain_type.hpp"
#include "ReplicaConfig.hpp"

using concord::storage::rocksdb::NativeClient;

namespace concord::kvbc::adapter {
class KeyValueBlockchain {
 public:
  template <typename OPTIONS,
            typename = std::enable_if_t<std::is_same_v<OPTIONS, NativeClient::DefaultOptions> ||
                                        std::is_same_v<OPTIONS, NativeClient::ExistingOptions> ||
                                        std::is_same_v<OPTIONS, NativeClient::UserOptions>>>
  [[maybe_unused]] explicit KeyValueBlockchain(const std::string& db_path,
                                               bool read_only,
                                               bool link_st_chain,
                                               const OPTIONS& options) {
    // The logic of choosing type of block chain will happen here.
    if (bftEngine::ReplicaConfig::instance().kvBlockchainVersion == BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN) {
      auto kv = std::make_shared<CatBCimpl::element_type>(NativeClient::newClient(db_path, read_only, options),
                                                          link_st_chain);
      kvbc_.emplace<CatBCimpl>(kv);
    }
  }

  // Creates a key-value blockchain.
  // If `category_types` is nullopt, the persisted categories in storage will be used.
  // If `category_types` has a value, it should contain all persisted categories in storage at a minimum. New ones
  // will be created and persisted. Users are required to pass a value for `category_types` on first construction
  // (i.e. a new blockchain) in order to specify the categories in use. Failure to do so will generate an exception.
  explicit KeyValueBlockchain(
      const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
      bool link_st_chain,
      const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>>& category_types = std::nullopt) {
    if (bftEngine::ReplicaConfig::instance().kvBlockchainVersion == BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN) {
      auto kv = std::make_shared<CatBCimpl::element_type>(native_client, link_st_chain, category_types);
      kvbc_.emplace<CatBCimpl>(kv);
    }
  }

  // The noop converter returns the input string as is, without modifying it.
  static const categorization::Converter kNoopConverter;

  /////////////////////// Add Block ///////////////////////

  BlockId addBlock(categorization::Updates&& updates);

  /////////////////////// Delete block ///////////////////////

  bool deleteBlock(const BlockId& blockId);
  void deleteLastReachableBlock();

  /////////////////////// Raw Blocks ///////////////////////

  // Adds raw block and tries to link the state transfer blockchain to the main blockchain
  // If linkSTChainFrom is true, try to remove blocks form the state transfer chain to the blockchain
  void addRawBlock(const categorization::RawBlock& block, const BlockId& block_id, bool lastBlock = true);
  std::optional<categorization::RawBlock> getRawBlock(const BlockId& block_id) const;

  /////////////////////// Info ///////////////////////
  BlockId getGenesisBlockId() const;
  BlockId getLastReachableBlockId() const;
  std::optional<BlockId> getLastStatetransferBlockId() const;

  std::optional<categorization::Hash> parentDigest(BlockId block_id) const;
  bool hasBlock(BlockId block_id) const;

  std::vector<std::string> getStaleKeys(BlockId block_id,
                                        const std::string& category_id,
                                        const categorization::ImmutableOutput& updates_info) const;

  std::vector<std::string> getStaleKeys(BlockId block_id,
                                        const std::string& category_id,
                                        const categorization::VersionedOutput& updates_info) const;

  std::vector<std::string> getStaleKeys(BlockId block_id,
                                        const std::string& category_id,
                                        const categorization::BlockMerkleOutput& updates_info) const;

  /////////////////////// Read interface ///////////////////////

  // Gets the value of a key by the exact blockVersion.
  std::optional<categorization::Value> get(const std::string& category_id,
                                           const std::string& key,
                                           BlockId block_id) const;

  std::optional<categorization::Value> getLatest(const std::string& category_id, const std::string& key) const;

  void multiGet(const std::string& category_id,
                const std::vector<std::string>& keys,
                const std::vector<BlockId>& versions,
                std::vector<std::optional<categorization::Value>>& values) const;

  void multiGetLatest(const std::string& category_id,
                      const std::vector<std::string>& keys,
                      std::vector<std::optional<categorization::Value>>& values) const;

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string& category_id,
                                                                const std::string& key) const;

  void multiGetLatestVersion(const std::string& category_id,
                             const std::vector<std::string>& keys,
                             std::vector<std::optional<categorization::TaggedVersion>>& versions) const;

  // Get the updates that were used to create `block_id`.
  std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const;

  // Get a map of category_id and stale keys for `block_id`
  std::map<std::string, std::vector<std::string>> getBlockStaleKeys(BlockId block_id) const;

  // Get a map from category ID -> type for all known categories in the blockchain.
  const std::map<std::string, categorization::CATEGORY_TYPE>& blockchainCategories() const;

  // from_block_id is given as a hint, as supposed to be the next block after the last reachable block.
  // until_block_id is the maximum block ID to be linked. If there are blocks with a block ID larger than
  // until_block_id, they can be linked on future calls.
  // returns the number of blocks connected
  size_t linkUntilBlockId(BlockId from_block_id, BlockId until_block_id);

  std::shared_ptr<concord::storage::rocksdb::NativeClient> db();
  std::shared_ptr<const concord::storage::rocksdb::NativeClient> db() const;

  //////// Extra functions ////////
  std::string getPruningStatus();

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator);

  // The key used in the default column family for persisting the current public state hash.
  static std::string publicStateHashKey();

 private:
  KVBlockChain kvbc_;
};

}  // namespace concord::kvbc::adapter
