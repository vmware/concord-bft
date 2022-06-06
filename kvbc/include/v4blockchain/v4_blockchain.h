// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
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

#include "categorization/updates.h"
#include "rocksdb/native_client.h"
#include "v4blockchain/detail/st_chain.h"
#include "v4blockchain/detail/latest_keys.h"
#include "v4blockchain/detail/blockchain.h"
#include <memory>
#include <string>

namespace concord::kvbc::v4blockchain {
/*
This class is the entrypoint to storage.
It dispatches all calls to the relevant targets (blockchain,latest keys,state transfer) and glues the flows.
*/
class KeyValueBlockchain {
 public:
  KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient> &,
                     bool link_st_chain,
                     const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>
                         &category_types = std::nullopt);

  /////////////////////// Add Block ///////////////////////
  BlockId add(categorization::Updates &&);
  BlockId add(const categorization::Updates &, storage::rocksdb::NativeWriteBatch &);
  ////////////////////// DELETE //////////////////////////
  BlockId deleteBlocksUntil(BlockId until);
  void deleteGenesisBlock();
  void deleteLastReachableBlock();
  ///////////////////// State Transfer////////////////////
  // Returns true if a block exists in the blockchain or state-transfer chain
  bool hasBlock(BlockId) const;
  // if the block exists, returns the content of the block i.e. raw block
  // the block origin can be the blockchain or the state-transfer chain
  std::optional<std::string> getBlockData(const BlockId &) const;
  // Insert the block buffer to the ST chain, if last block is true, it links the ST chain
  // To the blockchain.
  void addBlockToSTChain(const BlockId &, const char *block, const uint32_t blockSize, bool lastBlock);
  // Adds a range of blocks from the ST chain to the blockchain,
  // The rangs starts from the blockchain last_reachable +1
  size_t linkUntilBlockId(BlockId until_block_id);
  // Adds consecutive blocks from the ST chain to the blockchain until ST chain is empty or a gap is found.
  void linkSTChain();
  // Atomic delete block from the ST chain and add to the blockchain.
  void writeSTLinkTransaction(const BlockId, const categorization::Updates &);
  // Each block contains the genesis block at the time of that block insertion.
  // On State-transfer, we read this key and prune up to this block.
  void pruneOnSTLink(const categorization::Updates &);
  // Gets the digest from block, the digest represents the digest of the previous block i.e. parent digest
  concord::util::digest::BlockDigest parentDigest(BlockId block_id) const;
  std::optional<BlockId> getLastStatetransferBlockId() const;

  //////////////////Garbage collection for Keys that use TimeStamp API///////////////////////////
  // Using the RocksDB timestamp API, means that older user versions are not being deleted
  // On compaction unless they are mark as safe to delete.
  // If we get a new sequnce number it means that the previous sequence nunber was committed and it's
  // safe to trim up to the last block that was added during that sn.
  // An exception is when db checkpoint is being taken where no trimming is allowed.
  uint64_t markHistoryForGarbageCollectionIfNeeded(const categorization::Updates &updates);
  void checkpointInProcess(bool flag) { checkpointInProcess_ = flag; }
  uint64_t getBlockSequenceNumber(const categorization::Updates &updates) const;
  std::optional<uint64_t> getLastBlockSequenceNumber() { return last_block_sn_; }
  void setLastBlockSequenceNumber(uint64_t sn) { last_block_sn_ = sn; }
  // Stats for testing
  uint64_t gc_counter{};

  // In v4 storage in contrast to the categorized storage, pruning does not impact the state i.e. the digest
  // Of the blocks, in order to restrict deviation in the tail we add the genesis at the time the block is added,
  // as part of the block.
  // On state transfer completion this value can be used for pruning.
  void addGenesisBlockKey(categorization::Updates &updates) const;

  const v4blockchain::detail::Blockchain &getBlockchain() const { return block_chain_; };
  const v4blockchain::detail::StChain &getStChain() const { return state_transfer_chain_; };
  const v4blockchain::detail::LatestKeys &getLatestKeys() const { return latest_keys_; };
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IReader
  std::optional<categorization::Value> get(const std::string &category_id,
                                           const std::string &key,
                                           BlockId block_id) const;

  std::optional<categorization::Value> getLatest(const std::string &category_id, const std::string &key) const;

  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<categorization::Value>> &values) const;

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<categorization::Value>> &values) const;

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                const std::string &key) const;

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::TaggedVersion>> &versions) const;

  std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const {
    return block_chain_.getBlockUpdates(block_id);
  }

  // Get the current genesis block ID in the system.
  BlockId getGenesisBlockId() const { return block_chain_.getGenesisBlockId(); }

  // Get the last block ID in the system.
  BlockId getLastReachableBlockId() const { return block_chain_.getLastReachable(); }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // Trims the DB snapshot such that its last reachable block is equal to `block_id_at_checkpoint`.
  // This will trim in latest keys and blocks column families
  // This method is supposed to be called on DB snapshots only and not on the actual blockchain.
  // Precondition1: The current KeyValueBlockchain instance points to a DB snapshot.
  // Precondition2: `block_id_at_checkpoint` >= INITIAL_GENESIS_BLOCK_ID
  // Precondition3: `block_id_at_checkpoint` <= getLastReachableBlockId()
  void trimBlocksFromSnapshot(BlockId block_id_at_checkpoint);

 private:  // Member functons
  std::optional<categorization::Value> getValueFromUpdate(BlockId block_id,
                                                          const std::string &key,
                                                          const categorization::BlockMerkleInput &category_input) const;
  std::optional<categorization::Value> getValueFromUpdate(BlockId block_id,
                                                          const std::string &key,
                                                          const categorization::VersionedInput &category_input) const;
  std::optional<categorization::Value> getValueFromUpdate(BlockId block_id,
                                                          const std::string &key,
                                                          const categorization::ImmutableInput &category_input) const;

 private:  // Data members
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  v4blockchain::detail::Blockchain block_chain_;
  v4blockchain::detail::StChain state_transfer_chain_;
  v4blockchain::detail::LatestKeys latest_keys_;
  // flag to mark whether a checkpoint is being taken.
  std::atomic_bool checkpointInProcess_{false};
  std::optional<uint64_t> last_block_sn_;
};

}  // namespace concord::kvbc::v4blockchain
