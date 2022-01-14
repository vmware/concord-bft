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

#include "RangeValidationTree.hpp"
#include "SimpleBCStateTransfer.hpp"
#include "DataStore.hpp"

using BlockId = std::uint64_t;

namespace bftEngine::bcst::impl {

using CheckpointDesc = DataStore::CheckpointDesc;

/**
 * @brief TODO - write explanation of this class
 *
 */
class RVBManager {
  friend class BcStTestDelegator;

 public:
  enum class RvbDataInitialSource { FROM_STORAGE_CP, FROM_NETWORK, FROM_STORAGE_RECONSTRUCTION, NIL };

 public:
  // Init / Destroy functions
  RVBManager() = delete;
  RVBManager(const Config& config, const IAppState* state_api, const std::shared_ptr<DataStore>& ds);
  // stored_cp_num might be CheckpointBeingFetched if replica is in ST, or LastStoredCheckpoint if in consensus
  void init(bool fetching);

  // Called during checkpointing
  void updateRvbDataDuringCheckpoint(CheckpointDesc& last_checkpoint_desc);

  // Called during checkpoint summaries (source)
  std::ostringstream getRvbData() const { return in_mem_rvt_->getSerializedRvbData(); }

  // Called during checkpoint summaries stage by destination
  bool setRvbData(char* data, size_t data_size);

  // Called during ST GettingMissingBlocks by source when received FetchBlocksMsg with rvb_group_id != 0
  // Returns number of bytes filled. We assume that rvb_group_id must exist. This can be checked by calling
  // getSerializedByteSizeOfRvbGroup before calling this function.
  // when sizeOnly==true, buff and buff_max_size must be null and only size in bytes is returned
  size_t getSerializedDigestsOfRvbGroup(int64_t rvb_group_id, char* buff, size_t buff_max_size, bool size_only) const;

  // Called during ST GettingMissingBlocks by destination
  bool setSerializedDigestsOfRvbGroup(char* data,
                                      size_t data_size,
                                      BlockId min_fetch_block_id,
                                      BlockId max_fetch_block_id,
                                      BlockId max_block_id_in_cycle);
  std::optional<std::reference_wrapper<const STDigest>> getDigestFromRvbGroup(BlockId block_id) const;

  // This one should be called during FetchBlocksMsg by dest.
  // If returned value is 0, no RVB group shouldn't be requested when sending FetchBlocksMsg
  // This is due to the fact that all data is already stored in current_rvb_group_
  uint64_t getFetchBlocksRvbGroupId(BlockId from_block_id, BlockId to_block_id) const;

  // TODO - there is one case in PruningHandler::pruneThroughBlockId that might be not covered by this callback
  // This one is called by different thread context. It must b called that way to save and persist pruned blocks
  // digests
  void reportLastAgreedPrunableBlockId(BlockId lastAgreedPrunableBlockId);

  std::string getDigestOfRvbData() const { return in_mem_rvt_->getRootHashVal(); }
  void reset();

  // For the range [from_block_id, to_block_id], returns a block id BID, such that:
  // 1) Find all RVB group IDS for that range [RVBG_1,RVBG_2 ... RVBG_n]
  // 2) Remove all the already stored RVB groups. We remian with an i>=1: [RVBG_i,RVBG_i+1 ... RVBG_n]
  // 3) BID is the max RVB block ID in RVBG_i
  // This is done to simplify RVB digests fetching, in order to ask for a single group of digests per a single
  // FetchBlocksMsg
  BlockId getRvbGroupMaxBlockIdOfNonStoredRvbGroup(BlockId from_block_id, BlockId to_block_id) const;

  RvbDataInitialSource getRvbDataSource() const { return rvb_data_source_; }

 protected:
  logging::Logger logger_;
  const Config& config_;
  const IAppState* as_;
  const std::shared_ptr<DataStore>& ds_;
  std::unique_ptr<RangeValidationTree> in_mem_rvt_;
  RvbDataInitialSource rvb_data_source_;
  std::map<BlockId, STDigest> stored_rvb_digests_;
  std::vector<RVBGroupId> stored_rvb_digests_group_ids_;
  CheckpointDesc last_checkpoint_desc_;
  std::vector<std::pair<BlockId, STDigest>> pruned_blocks_digests_;
  std::mutex pruned_blocks_digests_mutex_;

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

  // Returns 0 if no such ID
  RVBGroupId getNextRequiredRvbGroupid(RVBId from_rvb_id, RVBId to_rvb_id) const;
#pragma pack(push, 1)
  struct rvbDigestInfo {
    BlockId block_id;
    STDigest digest;
  };
#pragma pack(pop)
};  // class RVBManager

}  // namespace bftEngine::bcst::impl
