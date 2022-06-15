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
#include <type_traits>

#include "assertUtils.hpp"

#include "kv_types.hpp"
#include "rocksdb/native_client.h"
#include "blockchain_misc.hpp"
#include "ReplicaConfig.hpp"
#include "db_interfaces.h"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "state_snapshot_interface.hpp"
#include "ISystemResourceEntity.hpp"
#include "replica_adapter_auxilliary_types.hpp"
#include "categorization/kv_blockchain.h"
#include "v4blockchain/v4_blockchain.h"
#include "storage/db_interface.h"

namespace concord::kvbc::adapter {
class ReplicaBlockchain : public IBlocksDeleter,
                          public IReader,
                          public IBlockAdder,
                          public bftEngine::bcst::IAppState,
                          public IKVBCStateSnapshot,
                          public IDBCheckpoint {
 public:
  virtual ~ReplicaBlockchain();
  explicit ReplicaBlockchain(
      const std::shared_ptr<concord::storage::rocksdb::NativeClient> &native_client,
      bool link_st_chain,
      const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>> &category_types = std::nullopt,
      const std::optional<aux::AdapterAuxTypes> &aux_types = std::nullopt);

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlocksDeleter implementation
  void deleteGenesisBlock() override final { return deleter_->deleteGenesisBlock(); }
  BlockId deleteBlocksUntil(BlockId until) override final { return deleter_->deleteBlocksUntil(until); }
  void deleteLastReachableBlock() override final { return deleter_->deleteLastReachableBlock(); }

  // Helper method, not part of the interface
  void onFinishDeleteLastReachable() {
    if (v4_kvbc_) {
      v4_kvbc_->onFinishDeleteLastReachable();
    }
  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IReader
  std::optional<categorization::Value> get(const std::string &category_id,
                                           const std::string &key,
                                           BlockId block_id) const override final {
    get_counter++;
    return reader_->get(category_id, key, block_id);
  }

  std::optional<categorization::Value> getLatest(const std::string &category_id,
                                                 const std::string &key) const override final {
    return reader_->getLatest(category_id, key);
  }

  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<categorization::Value>> &values) const override final {
    return reader_->multiGet(category_id, keys, versions, values);
  }

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<categorization::Value>> &values) const override final {
    auto start = std::chrono::steady_clock::now();
    reader_->multiGetLatest(category_id, keys, values);
    multiget_latest_duration.Get().Set(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count());
  }

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                const std::string &key) const override final {
    return reader_->getLatestVersion(category_id, key);
  }

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::TaggedVersion>> &versions) const override final {
    multiget_lat_version_counter++;
    auto start = std::chrono::steady_clock::now();
    reader_->multiGetLatestVersion(category_id, keys, versions);
    multiget_version_duration.Get().Set(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count());
  }

  std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const override final {
    return reader_->getBlockUpdates(block_id);
  }

  // Get the current genesis block ID in the system.
  BlockId getGenesisBlockId() const override final { return reader_->getGenesisBlockId(); }

  // Get the last block ID in the system.
  BlockId getLastBlockId() const override final { return reader_->getLastBlockId(); }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlockAdder
  BlockId add(categorization::Updates &&updates) override final {
    auto start = std::chrono::steady_clock::now();
    auto id = adder_->add(std::move(updates));
    add_block_duration.Get().Set(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count());
    if (id % 20 == 0) {
      metrics_comp_.UpdateAggregator();
    }
    return id;
  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IAppState implementation
  bool hasBlock(BlockId blockId) const override final { return app_state_->hasBlock(blockId); }
  bool getBlock(uint64_t blockId,
                char *outBlock,
                uint32_t outBlockMaxSize,
                uint32_t *outBlockActualSize) const override final {
    return app_state_->getBlock(blockId, outBlock, outBlockMaxSize, outBlockActualSize);
  }

  std::future<bool> getBlockAsync(uint64_t blockId,
                                  char *outBlock,
                                  uint32_t outBlockMaxSize,
                                  uint32_t *outBlockActualSize) override final {
    ConcordAssert(false);
    return std::async([]() { return false; });
  }

  bool getPrevDigestFromBlock(uint64_t blockId, bftEngine::bcst::StateTransferDigest *st_digest) const override final {
    return app_state_->getPrevDigestFromBlock(blockId, st_digest);
  }

  void getPrevDigestFromBlock(const char *blockData,
                              const uint32_t blockSize,
                              bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const override final {
    return app_state_->getPrevDigestFromBlock(blockData, blockSize, outPrevBlockDigest);
  }

  bool putBlock(const uint64_t blockId,
                const char *blockData,
                const uint32_t blockSize,
                bool lastBlock = true) override final {
    return app_state_->putBlock(blockId, blockData, blockSize, lastBlock);
  }

  std::future<bool> putBlockAsync(uint64_t blockId,
                                  const char *block,
                                  const uint32_t blockSize,
                                  bool lastBlock) override final {
    // This functions should not be called
    ConcordAssert(false);
    return std::async([]() { return false; });
  }

  uint64_t getLastReachableBlockNum() const override final { return app_state_->getLastReachableBlockNum(); }
  uint64_t getGenesisBlockNum() const override final { return app_state_->getGenesisBlockNum(); }
  // This method is used by state-transfer in order to find the latest block id in either the state-transfer chain or
  // the main blockchain
  uint64_t getLastBlockNum() const override final { return app_state_->getLastBlockNum(); }
  size_t postProcessUntilBlockId(uint64_t max_block_id) override final {
    return app_state_->postProcessUntilBlockId(max_block_id);
  }
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  ////////////////////////////////////IKVBCStateSnapshot////////////////////////////////////////////////////////////////
  void computeAndPersistPublicStateHash(
      BlockId checkpoint_block_id,
      const Converter &value_converter = [](std::string &&s) -> std::string { return std::move(s); }) override final {
    state_snapshot_->computeAndPersistPublicStateHash(checkpoint_block_id, value_converter);
  }

  std::optional<categorization::PublicStateKeys> getPublicStateKeys() const override final {
    return state_snapshot_->getPublicStateKeys();
  }

  void iteratePublicStateKeyValues(const std::function<void(std::string &&, std::string &&)> &f) const override final {
    state_snapshot_->iteratePublicStateKeyValues(f);
  }

  bool iteratePublicStateKeyValues(const std::function<void(std::string &&, std::string &&)> &f,
                                   const std::string &after_key) const override final {
    return state_snapshot_->iteratePublicStateKeyValues(f, after_key);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  ////////////////////////////////////IDBCheckpoint/////////////////////////////////////////////////////////////////////
  void trimBlocksFromCheckpoint(BlockId block_id_at_checkpoint) override final {
    return db_chkpt_->trimBlocksFromCheckpoint(block_id_at_checkpoint);
  }

  void checkpointInProcess(bool flag, kvbc::BlockId bid) override final { db_chkpt_->checkpointInProcess(flag, bid); }
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////db editor support//////////////////////////
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> blockchainCategories() const {
    return kvbc_ == nullptr ? v4_kvbc_->getCategories() : kvbc_->blockchainCategories();
  }
  concord::kvbc::BLOCKCHAIN_VERSION blockchainVersion() const { return version_; }
  const concord::kvbc::categorization::KeyValueBlockchain *const getReadOnlyCategorizedBlockchain() {
    return kvbc_.get();
  }
  std::map<std::string, std::vector<std::string>> getBlockStaleKeys(BlockId block_id) const {
    return kvbc_ == nullptr ? v4_kvbc_->getBlockStaleKeys(block_id) : kvbc_->getBlockStaleKeys(block_id);
  }
  std::shared_ptr<storage::IDBClient> asIDBClient() const { return native_client_->asIDBClient(); }
  std::unordered_set<std::string> columnFamilies() const { return native_client_->columnFamilies(); }
  void getCFProperty(const std::string &cf, const std::string &property, uint64_t *val) const {
    auto handle = native_client_->columnFamilyHandle(cf);
    native_client_->rawDB().GetIntProperty(handle, rocksdb::Slice(property.c_str(), property.length()), val);
  }

 private:
  void switch_to_rawptr();

 private:
  logging::Logger logger_;
  //////////Common Interfaces /////////////////

  ////////////////Set A of Unique Ptrs for better memory management ////////////////////////////////////////////////////
  std::unique_ptr<IBlocksDeleter> up_deleter_;
  std::unique_ptr<IReader> up_reader_;
  std::unique_ptr<IBlockAdder> up_adder_;
  std::unique_ptr<bftEngine::bcst::IAppState> up_app_state_;
  std::unique_ptr<IKVBCStateSnapshot> up_state_snapshot_;
  std::unique_ptr<IDBCheckpoint> up_db_chkpt_;

  ////////////////Set A of Ptrs for better performance ////////////////////////////////////////////////////
  IBlocksDeleter *deleter_{nullptr};
  IReader *reader_{nullptr};
  IBlockAdder *adder_{nullptr};
  bftEngine::bcst::IAppState *app_state_{nullptr};
  IKVBCStateSnapshot *state_snapshot_{nullptr};
  IDBCheckpoint *db_chkpt_{nullptr};

  //////////////Blockchain Abstractions //////////////////
  std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain> kvbc_{nullptr};
  std::shared_ptr<concord::kvbc::v4blockchain::KeyValueBlockchain> v4_kvbc_{nullptr};

  //////////////helpers///////////////////////////
  concord::kvbc::BLOCKCHAIN_VERSION version_;
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  ////Metrics
  concordMetrics::Component metrics_comp_;
  mutable concordMetrics::GaugeHandle add_block_duration;
  mutable concordMetrics::GaugeHandle multiget_latest_duration;
  mutable concordMetrics::GaugeHandle multiget_version_duration;
  mutable concordMetrics::CounterHandle get_counter;
  mutable concordMetrics::CounterHandle multiget_lat_version_counter;
};
}  // namespace concord::kvbc::adapter
