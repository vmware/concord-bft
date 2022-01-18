// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// This file contains simple wrapper types around a steady_clock. Its
// replacement code for prior type wrappers around uint64_t and int64_t that
// were less safe. It shouldn't be used outside the bftEngine and only exists to
// allow making the minimal possible changes to allow using std::chrono. It may
// be removed in the future and to use the std::types directly. However, it's
// nice to force the use of steady_clock to avoid mistakes in using the wrong
// clock.

#pragma once

#include <memory>

#include "DataStore.hpp"

using BlockId = std::uint64_t;

namespace bftEngine::bcst::impl {

using CheckpointDesc = DataStore::CheckpointDesc;
using RVBGroupId = uint64_t;
using RVBId = uint64_t;
using RVBIndex = uint64_t;

class RangeValidationTree;

/**
 * Range Validation Blocks Manager (RVBM) is part of BCStateTran (State Transfer). It's responsible for managing RVB
 data
 * and provided services as a source/destination which support BFT block validation and block collection in
 * chronological order. Without block validation, while collecting blocks in chronological order (old to new), any
 * Byzantine source replica may cause DOS on State Transfer.
 *
 * RVBM currently holds its main (RVB) data structure as a RangeValidationTree (RVT). It makes sure for the integrity,
 * persistency and update of this data. We can divide its duties and services to BCStateTran (by its public API) into
 * few areas:
 *
 * 1) During checkpointing - as a consensus replica:
 *  a) Collects pruned blocks digest and persists them until the next checkpoint.
 *  b) In each checkpoint updates the RVT according to the recent blocks added/removed to/from storage during the last
 * checkpoint.
 *
 * 2) During State Transfer - as a source replica/consensus replica:
 *  a) Each replica sends its serialized in-memory RVT for a specifically requested checkpoint to a destination.
 *  b) Source sends requested RVB group digests, which can only be found in storage (level 0 of the RVT). These digests
 *     will support RVB validation during block collection in a destination. RVB validation group is a group of RVB
 *     which can help validate RVTs level 1 node values which are already found in destination (received during
 *     checkpoint summaries stage).
 *
 .3) During State Transfer - as a destination replica: RVBM holds the target checkpoint RVT
 *   and provides services to validate RVB group and RVB blocks. 4) During initialization: a) Loads pruned blocks
 *   digests and the last checkpoint RVB data. b) If there is no last checkpoint, RVBM reconstructs the RVB data from
 *   storage.
 **/

class RVBManager {
  // For testing only
  friend class BcStTestDelegator;

 public:
  enum class RvbDataInitialSource { FROM_STORAGE_CP, FROM_NETWORK, FROM_STORAGE_RECONSTRUCTION, NIL };

 public:
  // Init / Destroy functions
  RVBManager() = delete;
  RVBManager(const Config& config, const IAppState* state_api, const std::shared_ptr<DataStore>& ds);
  void init(bool fetching);

  // Update the RVB data (up to last_checkpoint_desc.maBlockId) according to recent checkpoint  storage updates (added
  // and pruned blocks)
  void updateRvbDataDuringCheckpoint(CheckpointDesc& last_checkpoint_desc);

  // Get a serialized RVB data. Used by ST source (during checkpoint summaries)
  std::ostringstream getRvbData() const;

  // Get a serialized RVB data. Used by ST destination (during checkpoint summaries)
  bool setRvbData(char* data, size_t data_size);

  // Called during ST GettingMissingBlocks by source when received FetchBlocksMsg with rvb_group_id != 0
  // Returns number of bytes filled. We assume that rvb_group_id must exist. This can be checked by calling
  // when sizeOnly==true, buff and buff_max_size must be null and only size in bytes is returned.
  // when sizeOnly=false the digests aere serialized into buff and the total size returned. If buff_max_size is too
  // small, 0 is returned.
  size_t getSerializedDigestsOfRvbGroup(int64_t rvb_group_id, char* buff, size_t buff_max_size, bool size_only) const;

  // Called during ST GettingMissingBlocks by destination, to set RVB group digests.
  // data,data_size is to provide the serialized data (blocks digests)
  // min_fetch_block_id,max_fetch_block_id are the current fetch range, and are used for validating the RVB group.
  // The digeses are stored inside stored_rvb_digests_ after validated. If validation failed thedigests are not set and
  // false is returned.
  bool setSerializedDigestsOfRvbGroup(char* data,
                                      size_t data_size,
                                      BlockId min_fetch_block_id,
                                      BlockId max_fetch_block_id,
                                      BlockId max_block_id_in_cycle);

  // Called during ST GettingMissingBlocks by destination, to get an RVB digest from stored_rvb_digests_.
  // If RVB digest not found - a null optional is returned.
  std::optional<std::reference_wrapper<const STDigest>> getDigestFromStoredRvb(BlockId block_id) const;

  // Returns the next required RVB group ID if needed. Called during  FetchBlocksMsg by destination.
  // If no RVBGroupId is required, 0 is returned.
  RVBGroupId getFetchBlocksRvbGroupId(BlockId from_block_id, BlockId to_block_id) const;

  // This one is called by pruning thread context. It must be called that way to save and persist pruned blocks
  // digests. lastAgreedPrunableBlockId is the maximal block ID to be pruned (already agreed by consensus)
  // For persistency, digests are kept in pruned_blocks_digests_ until the next checkpoint.
  void reportLastAgreedPrunableBlockId(BlockId lastAgreedPrunableBlockId);

  // Returns a string representation of the current state of the full RVB data
  std::string getStateOfRvbData() const;

  // Resets the RVBM by clearing all the data structures, except pruned_blocks_digests_
  // inital_source cna be passed to mark the source (reason) for the reset
  void reset(RvbDataInitialSource inital_source = RvbDataInitialSource::NIL);

  // Validate integrity of RVBM data. In particular, validated the RVT
  bool validate() const;

  // For the range [from_block_id, to_block_id], returns a block id BID, such that:
  // 1) Find all RVB group IDS for that range [RVBG_1,RVBG_2 ... RVBG_n]
  // 2) Remove all the already stored RVB groups. We remian with an i>=1: [RVBG_i,RVBG_i+1 ... RVBG_n]
  // 3) BID is the max RVB block ID in RVBG_i
  // This is done to simplify RVB digests fetching, in order to ask for a single group of digests per a single
  // FetchBlocksMsg
  BlockId getRvbGroupMaxBlockIdOfNonStoredRvbGroup(BlockId from_block_id, BlockId to_block_id) const;

  // Returns the source in which the RVB data was loaded, since last boot. This is useful for debugging.
  RvbDataInitialSource getRvbDataSource() const { return rvb_data_source_; }

 protected:
  // logging
  logging::Logger logger_;
  const std::set<std::string> debug_prints_log_level{"trace", "debug"};

  // config, storage and data store
  const Config& config_;
  const IAppState* as_;
  const std::shared_ptr<DataStore>& ds_;

  // RVB data (during ST as destination)
  std::map<BlockId, STDigest> stored_rvb_digests_;
  std::vector<RVBGroupId> stored_rvb_digests_group_ids_;

  // RVB data update during checkpointing - pruning
  std::vector<std::pair<BlockId, STDigest>> pruned_blocks_digests_;
  std::mutex pruned_blocks_digests_mutex_;
  CheckpointDesc last_checkpoint_desc_;

  // Actual RVB data
  // RangeValidationTree is an incomplete type, define a deleter for the unique ptr
  struct rvt_deleter {
    void operator()(RangeValidationTree*) const;
  };
  std::unique_ptr<RangeValidationTree, rvt_deleter> in_mem_rvt_;
  RvbDataInitialSource rvb_data_source_;

 protected:
  const STDigest getBlockAndComputeDigest(uint64_t block_id) const;
  void computeDigestOfBlock(const uint64_t block_id,
                            const char* block,
                            const uint32_t block_size,
                            char* out_digest) const;
  void addRvbDataOnBlockRange(uint64_t min_block_id,
                              uint64_t max_block_id,
                              const std::optional<STDigest>& digest_of_max_block_id);
  // returns the next RVB ID after block_id. If block_id is an RVB ID, returns block_id.
  inline BlockId nextRvbBlockId(BlockId block_id) const;

  // returns the previous RVB ID to block_id. If block_id is an RVB ID, returns block_id.
  BlockId prevRvbBlockId(BlockId block_id) const {
    return config_.fetchRangeSize * (block_id / config_.fetchRangeSize);
  }
  void pruneRvbDataDuringCheckpoint(const CheckpointDesc& new_checkpoint_desc);
  // Returns 0 if no such ID
  RVBGroupId getNextRequiredRvbGroupid(RVBId from_rvb_id, RVBId to_rvb_id) const;

  std::string getLogLevel() const;
#pragma pack(push, 1)
  struct rvbDigestInfo {
    BlockId block_id;
    STDigest digest;
  };
#pragma pack(pop)
};  // class RVBManager

}  // namespace bftEngine::bcst::impl