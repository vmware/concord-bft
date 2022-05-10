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
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IReader
  std::optional<categorization::Value> get(const std::string &category_id,
                                           const std::string &key,
                                           BlockId block_id) const override final {
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
    return reader_->multiGetLatest(category_id, keys, values);
  }

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                const std::string &key) const override final {
    return reader_->getLatestVersion(category_id, key);
  }

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::TaggedVersion>> &versions) const override final {
    return reader_->multiGetLatestVersion(category_id, keys, versions);
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
  BlockId add(categorization::Updates &&updates) override final { return adder_->add(std::move(updates)); }
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

  void checkpointInProcess(bool flag) override final { db_chkpt_->checkpointInProcess(flag); }
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
};
}  // namespace concord::kvbc::adapter
