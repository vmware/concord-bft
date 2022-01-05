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
 public:
  // Init / Destroy functions
  RVBManager() = delete;
  RVBManager(const Config& config, const IAppState* state_api, const std::shared_ptr<DataStore>& ds);
  void init();

  // Called during checkpointing
  void updateRvbDataDuringCheckpoint(CheckpointDesc& last_checkpoint_desc);

  // Called during checkpoint summaries (source)
  std::ostringstream getRvbData() const { return in_mem_rvt_->getSerializedRvbData(); }

  // Called during checkpoint summaries stage by destination
  void setRvbData(std::shared_ptr<char> data, size_t data_size);

  // Called during ST GettingMissingBlocks by source when received FetchBlocksMsg with rvb_group_id != 0
  // Returns number of bytes filled
  size_t getSerializedRvbGroup(int64_t rvb_group_id, char* buff, size_t buff_max_size) const;

  // Called during ST GettingMissingBlocks by destination
  bool setSerializedRvbGroup(char* data, size_t data_size);
  std::optional<std::reference_wrapper<const STDigest>> getDigestFromRvbGroup(BlockId block_id) const;

  // This one should be called during FetchBlocksMsg by dest.
  // If returned value is 0, no RVB group should be requested when sending FetchBlocksMsg
  int64_t getRvbGroupId(BlockId from_block_id, BlockId to_block_id) const;

  // TODO - there is one case in PruningHandler::pruneThroughBlockId that might be not covered by this callback
  // This one is called by different thread context. It must b called that way to save and persist pruned blocks
  // digests
  void reportLastAgreedPrunableBlockId(BlockId lastAgreedPrunableBlockId);

 protected:
  logging::Logger logger_;
  const Config& config_;
  std::unique_ptr<RangeValidationTree> in_mem_rvt_;
  std::unordered_map<BlockId, STDigest> current_rvb_group_;
  const IAppState* as_;
  const std::shared_ptr<DataStore>& ds_;
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
  inline BlockId computeNextRvbBlockId(BlockId block_id) const;
};  // class RVBManager

}  // namespace bftEngine::bcst::impl
