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
  BlockId add(const categorization::Updates &,
              v4blockchain::detail::Block &block,
              storage::rocksdb::NativeWriteBatch &);
  ////////////////////// DELETE //////////////////////////
  BlockId deleteBlocksUntil(BlockId until);
  void deleteGenesisBlock();
  void deleteLastReachableBlock();
  void onFinishDeleteLastReachable();
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

  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> getCategories() const {
    return latest_keys_.getCategories();
  }

  std::string getCategoryFromPrefix(const std::string &p) const { return latest_keys_.getCategoryFromPrefix(p); }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // Trims the DB snapshot such that its last reachable block is equal to `block_id_at_checkpoint`.
  // This will trim in latest keys and blocks column families
  // This method is supposed to be called on DB snapshots only and not on the actual blockchain.
  // Precondition1: The current KeyValueBlockchain instance points to a DB snapshot.
  // Precondition2: `block_id_at_checkpoint` >= INITIAL_GENESIS_BLOCK_ID
  // Precondition3: `block_id_at_checkpoint` <= getLastReachableBlockId()
  void trimBlocksFromSnapshot(BlockId block_id_at_checkpoint);

  //////////////////Recovery infra///////////////////////////
  /*
  Recovery is needed when we crash while executing a BFT sequence number's requests or when trimming the blocks that
  were added during a checkpoint creation. The complexity is to revert the latest column family to the state prior the
  block addition, as a key can get updated but its previous version is not known. On BFT recovery, we detect that a BFT
  sequence number has changed, and we can take a rocksdb snapshot of its blockchain state to use for recovery if the
  current sn execution fails. the deletion process itself is a two-phase operation: 1 - open the blockchain in readonly
  mode 2 - get the updates needed for reverting the last block, using the snapshot of either BFT or checkpoint. 3 - save
  the serialized updates in a different recovery DB. 4- start the blockchain in write mode and apply the updates.

  */

  // take snapshot and release the prev if BFT sn has changed.
  uint64_t onNewBFTSequenceNumber(const categorization::Updates &updates);
  // take snapshot and release on start and end of checkpoint.
  void checkpointInProcess(bool flag, kvbc::BlockId block_id_at_chkpnt);
  uint64_t getBlockSequenceNumber(const categorization::Updates &updates) const;
  std::optional<uint64_t> getLastBlockSequenceNumber() { return last_block_sn_; }
  void setLastBlockSequenceNumber(uint64_t sn) { last_block_sn_ = sn; }
  const ::rocksdb::Snapshot *getSnapShot() { return snap_shot_; }
  std::optional<const ::rocksdb::Snapshot *> getCheckpointSnapShot(const kvbc::BlockId id) {
    if (chkpnt_snap_shots_.count(id) == 0) return std::nullopt;
    return chkpnt_snap_shots_[id];
  }
  // open an instance of DB for storing recovery udpates.
  std::shared_ptr<storage::rocksdb::NativeClient> getRecoveryDB();

  // helper class for clients to call when recovery is needed.
  // path - the path of the blockchain db.
  struct BlockchainRecovery {
    BlockchainRecovery(const std::string &path, const std::optional<kvbc::BlockId> &block_id_at_chkpnt);
    static std::shared_ptr<storage::rocksdb::NativeClient> getRecoveryDB(const std::string &path, bool is_chkpnt);
    static void removeRecoveryDB(const std::string &path, bool is_chkpnt);
    static std::string getPath(const std::string &path, bool is_chkpnt);
  };

  /*
    this class is used on recovery, it's instantiated from the persisted RocksDB sequnce number,
    and is given to RocksDB "get" method in the read options, in order to read values from a stable state.
    */
  struct RecoverySnapshot {
    struct V4snapshot : ::rocksdb::Snapshot {
      V4snapshot(::rocksdb::SequenceNumber sn) : sn_(sn) {}
      virtual ::rocksdb::SequenceNumber GetSequenceNumber() const override { return sn_; }
      virtual ~V4snapshot(){};
      ::rocksdb::SequenceNumber sn_;
    };

    RecoverySnapshot(::rocksdb::DB *db) : db_(db) { sh_ = db_->GetSnapshot(); }
    RecoverySnapshot(::rocksdb::DB *db, ::rocksdb::Snapshot *sh) : db_(db), sh_(sh) {}
    /*
     Get the value to store in the data base which is concatenation of the rocksdb sn,
     and the Concord stable version that can be either BFT sn or block id depends whether
     the context is recovery or trim block for checkpoint.
     once the value is taken we don't wont to use the RAII property.
    */
    std::string getStorableSeqNumAndPreventRelease(const uint64_t stable_version) {
      release = false;
      return concordUtils::toBigEndianStringBuffer(sh_->GetSequenceNumber()) +
             concordUtils::toBigEndianStringBuffer(stable_version);
    }

    static std::string getStorableKey(std::optional<kvbc::BlockId> block_id_at_chkpnt) {
      if (block_id_at_chkpnt.has_value()) {
        return kvbc::keyTypes::v4_snapshot_sequence_checkpoint +
               concordUtils::toBigEndianStringBuffer(*block_id_at_chkpnt);
      }
      return kvbc::keyTypes::v4_snapshot_sequence;
    }

    static std::unique_ptr<V4snapshot> getV4SnapShotFromSeqnum(const std::string &sn) {
      return std::make_unique<V4snapshot>(concordUtils::fromBigEndianBuffer<::rocksdb::SequenceNumber>(sn.data()));
    }
    // Get the BFT data part of the stored value
    static uint64_t getStableVersion(const std::string &sn) {
      return concordUtils::fromBigEndianBuffer<uint64_t>(sn.data() + sizeof(::rocksdb::SequenceNumber));
    }
    const ::rocksdb::Snapshot *get() { return sh_; }

    // check if it's not V4snapshot
    void releasePreviousSnapshot(const ::rocksdb::Snapshot *sh) { releasePreviousSnapshot(sh, db_); }
    static void releasePreviousSnapshot(const ::rocksdb::Snapshot *sh, ::rocksdb::DB *db) {
      if (dynamic_cast<const RecoverySnapshot::V4snapshot *>(sh) != nullptr) {
        LOG_FATAL(V4_BLOCK_LOG, "Can't call rocksdb release with v4 snapshot");
        ConcordAssert(false);
      }
      db->ReleaseSnapshot(sh);
    }

    ~RecoverySnapshot() {
      if (release) {
        db_->ReleaseSnapshot(sh_);
      }
    }
    ::rocksdb::DB *db_{nullptr};
    const ::rocksdb::Snapshot *sh_{nullptr};
    bool release{true};
  };

  // Gets a set of updates needed to revert a last reachable block and stores them in a recovery db.
  void storeLastReachableRevertBatch(const std::optional<kvbc::BlockId> &block_id_at_chkpnt);

  // stale keys helper methods for tests
  std::map<std::string, std::vector<std::string>> getBlockStaleKeys(BlockId block_id) const;
  std::vector<std::string> getStaleKeys(BlockId block_id,
                                        const std::string &category_id,
                                        const categorization::BlockMerkleInput &updates_input) const;

  std::vector<std::string> getStaleKeys(BlockId block_id,
                                        const std::string &category_id,
                                        const categorization::VersionedInput &updates_input) const;

  std::vector<std::string> getStaleKeys(BlockId block_id,
                                        const std::string &category_id,
                                        const categorization::ImmutableInput &updates_input) const;

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
  std::optional<uint64_t> last_block_sn_;
  const float updates_to_final_size_ration_{2.5};
  // Not owner of the object, do not need to delete
  const ::rocksdb::Snapshot *snap_shot_{nullptr};
  std::map<kvbc::BlockId, const ::rocksdb::Snapshot *> chkpnt_snap_shots_;
  // const ::rocksdb::Snapshot *chkpoint_snap_shot_{nullptr};
  util::ThreadPool thread_pool_{1};
  std::optional<kvbc::BlockId> chkpnt_block_id_;

  // Metrics
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
  concordMetrics::Component v4_metrics_comp_;
  concordMetrics::GaugeHandle blocks_deleted_;

 public:
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    aggregator_ = aggregator;
    v4_metrics_comp_.SetAggregator(aggregator_);
  }
};

}  // namespace concord::kvbc::v4blockchain
