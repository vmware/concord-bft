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

#include <algorithm>

#include "RVBManager.hpp"

using namespace std;

namespace bftEngine::bcst::impl {

RVBManager::RVBManager(const Config& config, const IAppState* state_api, DataStore* ds)
    : logger_{logging::getLogger("concord.bft.st.rvb")},
      config_{config},
      in_mem_rvt_{std::make_unique<RangeValidationTree>(logger_, config_.RVT_K, config_.fetchRangeSize)},
      as_{state_api},
      ds_{ds} {
  LOG_TRACE(logger_, "");
  last_checkpoint_desc_.makeZero();
}

void RVBManager::init() {
  LOG_TRACE(logger_, "");
  bool reconstruct_rvb_data = true;
  static constexpr bool print_rvt = true;
  CheckpointDesc desc{};
  uint64_t last_stored_cp_num = ds_->getLastStoredCheckpoint();
  std::lock_guard<std::mutex> guard(pruned_blocks_digests_mutex_);

  // Get pruned blocks digests
  pruned_blocks_digests_ = ds_->getPrunedBlocksDigests();

  // Try to get RVT from persistent storage. Even if the tree exist we need to check if it
  // match current configuration
  if (last_stored_cp_num > 0) {
    desc = ds_->getCheckpointDesc(last_stored_cp_num);
    if (!desc.rvbData.empty()) {
      // There is RVB data in this checkpoint - try to load it
      std::istringstream rvb_data(std::string(reinterpret_cast<const char*>(desc.rvbData.data()), desc.rvbData.size()));
      // TODO - deserialize should return a bool - it might fail due to logical/config issue.
      in_mem_rvt_->deserialize(rvb_data);
      reconstruct_rvb_data = false;
    }
  }

  if (reconstruct_rvb_data && (desc.maxBlockId > 0)) {
    // If desc data is valid, try to reconstruct by reading digests from storage (no persistency data was found)
    addRvbDataOnBlockRange(
        as_->getGenesisBlockNum(), desc.maxBlockId, std::optional<STDigest>(desc.digestOfMaxBlockId));
  }

  // TODO - print also the root hash
  LOG_INFO(logger_, std::boolalpha << KVLOG(pruned_blocks_digests_.size(), last_stored_cp_num, reconstruct_rvb_data));
  if (print_rvt) {
#ifdef USE_LOG4CPP
    auto log_level = logging::Logger::getRoot().getLogLevel();
    if ((log_level == log4cplus::DEBUG_LOG_LEVEL) || (log_level == log4cplus::TRACE_LOG_LEVEL)) {
      in_mem_rvt_->printToLog(false);
    }
#else
    // TODO - check that this compiles
    auto log_level = logger_.getLogLevel();
    if ((log_level == logging::LogLevel::debug) || (log_level == logging::LogLevel::trace)) {
      in_mem_rvt_->printToLog(false);
    }
#endif
  }
}

void RVBManager::updateRvbDataDuringCheckpoint(CheckpointDesc& new_checkpoint_desc) {
  LOG_DEBUG(logger_,
            "Updating RVB data for" << KVLOG(new_checkpoint_desc.checkpointNum,
                                             new_checkpoint_desc.maxBlockId,
                                             last_checkpoint_desc_.checkpointNum,
                                             last_checkpoint_desc_.maxBlockId));
  ConcordAssertAND((last_checkpoint_desc_.maxBlockId <= new_checkpoint_desc.maxBlockId),
                   (last_checkpoint_desc_.checkpointNum < new_checkpoint_desc.checkpointNum));

  BlockId block_id =
      (last_checkpoint_desc_.checkpointNum == 0) ? as_->getGenesisBlockNum() : last_checkpoint_desc_.maxBlockId + 1;
  addRvbDataOnBlockRange(block_id, new_checkpoint_desc.maxBlockId, new_checkpoint_desc.digestOfMaxBlockId);

  {  // start of critical section A
    std::lock_guard<std::mutex> guard(pruned_blocks_digests_mutex_);
    size_t i{};

    // first, see if we can prune and persist pruned_blocks_digests_. We do this while comparing to
    // last_checkpoint_desc_, while we know that all digests  up to that point are removed from RVT and RVT is persisted
    if ((last_checkpoint_desc_.checkpointNum > 0) && (!pruned_blocks_digests_.empty())) {
      for (i = 0; i < pruned_blocks_digests_.size(); ++i) {
        if (pruned_blocks_digests_[i].first > last_checkpoint_desc_.maxBlockId) {
          break;
        }
      }
      if (i > 0) {
        LOG_DEBUG(logger_,
                  "Remove " << i << " digests from pruned_blocks_digests_, from/to block IDs:"
                            << KVLOG(pruned_blocks_digests_[0].first, pruned_blocks_digests_[i - 1].first));
        pruned_blocks_digests_.erase(pruned_blocks_digests_.begin(), pruned_blocks_digests_.begin() + i);
        ds_->setPrunedBlocksDigests(pruned_blocks_digests_);
      }

      // Now, remove all block digests rlast_checkpoint_desc_elated to the current checlast_checkpoint_desc_kpointing.
      // Theoratically, there might be block digests which belong to the next checkpoint
      for (i = 0; i < pruned_blocks_digests_.size(); ++i) {
        if (pruned_blocks_digests_[i].first <= new_checkpoint_desc.maxBlockId)
          in_mem_rvt_->removeNode(pruned_blocks_digests_[i].first, pruned_blocks_digests_[i].second);
        else
          break;
      }
      if (i > 0) {
        LOG_DEBUG(logger_,
                  "Remove " << i << " digests from in_mem_rvt_, from/to block IDs:"
                            << KVLOG(pruned_blocks_digests_[0].first, pruned_blocks_digests_[i - 1].first));
        pruned_blocks_digests_.erase(pruned_blocks_digests_.begin(), pruned_blocks_digests_.begin() + i);
      }
      // We relay on caller to persist new_checkpoint_desc, and leave pruned_blocks_digests_ persisted before erase was
      // done (some redundent digests)
    }
    in_mem_rvt_->printToLog(false);
  }  // end of critical section A

  std::ostringstream rvb_data;
  in_mem_rvt_->serialize(rvb_data);
  const std::string s = rvb_data.str();
  std::copy(s.c_str(), s.c_str() + s.length(), back_inserter(new_checkpoint_desc.rvbData));

  last_checkpoint_desc_ = new_checkpoint_desc;
}

std::ostringstream RVBManager::getRvbData() const {
  LOG_TRACE(logger_, "");
  std::ostringstream rvb_data;
  in_mem_rvt_->serialize(rvb_data);
  return rvb_data;
}

void RVBManager::setRvbData(std::shared_ptr<char> data, size_t data_size) {
  LOG_TRACE(logger_, "");
  std::istringstream rvb_data(std::string(reinterpret_cast<const char*>(data.get()), data_size));
  // TODO - deserialize should return a bool - it might fail due to logical/config issue. handle error in that case.
  in_mem_rvt_->deserialize(rvb_data);
}

// TODO - implement
size_t RVBManager::getSerializedRvbGroup(int64_t rvb_group_id, char* buff, size_t buff_max_size) const {
  LOG_TRACE(logger_, "");
  ConcordAssert(0);
  return 0;
}

// TODO - implement
bool RVBManager::setSerializedRvbGroup(char* data, size_t data_size) {
  LOG_TRACE(logger_, "");
  ConcordAssert(0);
  return false;
}

std::optional<std::reference_wrapper<const STDigest>> RVBManager::getDigestFromRvbGroup(BlockId block_id) const {
  LOG_TRACE(logger_, KVLOG(block_id));
  const auto iter = current_rvb_group_.find(block_id);
  if (iter == current_rvb_group_.end()) {
    ostringstream oss;
    oss << KVLOG(block_id) << " not found in current_rvb_group_";
    LOG_ERROR(logger_, KVLOG(block_id) << " not found in current_rvb_group_");
    return std::nullopt;
  }
  return std::optional<std::reference_wrapper<const STDigest>>(iter->second);
}

// TODO - implement
int64_t RVBManager::getRvbGroupId(BlockId from_block_id, BlockId to_block_id) const {
  LOG_TRACE(logger_, KVLOG(from_block_id, to_block_id));
  ConcordAssert(0);
  return 0;
}

void RVBManager::computeDigestOfBlock(const uint64_t block_id,
                                      const char* block,
                                      const uint32_t block_size,
                                      char* out_digest) const {
  ConcordAssertGT(block_id, 0);
  ConcordAssertGT(block_size, 0);
  DigestContext c;
  c.update(reinterpret_cast<const char*>(&block_id), sizeof(block_id));
  c.update(block, block_size);
  c.writeDigest(out_digest);
}

// TODO - BCStateTran have a similar function + computeDigestOfBlock . Move common functions into
// BCStateTranCommon.hpp/cpp
const STDigest RVBManager::getBlockAndComputeDigest(uint64_t block_id) const {
  static std::unique_ptr<char[]> buffer(new char[config_.maxBlockSize]);
  STDigest digest{0};
  uint32_t block_size{0};
  as_->getBlock(block_id, buffer.get(), config_.maxBlockSize, &block_size);
  computeDigestOfBlock(block_id, buffer.get(), block_size, digest.getForUpdate());
  return digest;
}

void RVBManager::addRvbDataOnBlockRange(uint64_t min_block_id,
                                        uint64_t max_block_id,
                                        const std::optional<STDigest>& digest_of_max_block_id) {
  LOG_TRACE(logger_, KVLOG(min_block_id, max_block_id));
  std::once_flag call_once_flag;
  uint64_t current_rvb_id = computeNextRvbBlockId(min_block_id);
  while (current_rvb_id < max_block_id) {
    // TODO - As a 2nd phase - should use the thread pool to fetch a batch of digests or move to a background process
    std::call_once(call_once_flag, [&, this] {
      LOG_INFO(logger_,
               "Update RVT (add):" << KVLOG(min_block_id, max_block_id, current_rvb_id, as_->getLastBlockNum()));
    });

    STDigest digest;
    as_->getPrevDigestFromBlock(current_rvb_id + 1, reinterpret_cast<StateTransferDigest*>(digest.getForUpdate()));
    LOG_TRACE(logger_, KVLOG(current_rvb_id, digest.toString()));  // TODO - remove later
    in_mem_rvt_->addNode(current_rvb_id, digest);
    current_rvb_id += config_.fetchRangeSize;
  }
  if (current_rvb_id == max_block_id) {
    if (digest_of_max_block_id)
      in_mem_rvt_->addNode(current_rvb_id, digest_of_max_block_id.value());
    else {
      in_mem_rvt_->addNode(current_rvb_id, getBlockAndComputeDigest(max_block_id));
    }
  }
}

BlockId RVBManager::computeNextRvbBlockId(BlockId block_id) const {
  uint64_t next_rvb_id = config_.fetchRangeSize * (block_id / config_.fetchRangeSize);
  if (next_rvb_id < block_id) {
    next_rvb_id += config_.fetchRangeSize;
  }
  return next_rvb_id;
}

void RVBManager::reportLastAgreedPrunableBlockId(uint64_t lastAgreedPrunableBlockId) {
  LOG_TRACE(logger_, KVLOG(lastAgreedPrunableBlockId));
  std::lock_guard<std::mutex> guard(pruned_blocks_digests_mutex_);
  auto initial_size = pruned_blocks_digests_.size();
  BlockId min_block_id = pruned_blocks_digests_.empty() ? as_->getGenesisBlockNum() : pruned_blocks_digests_[0].first;
  uint64_t start_rvb_id = computeNextRvbBlockId(min_block_id);
  uint64_t current_rvb_id = start_rvb_id;
  int32_t num_digests_added{0};
  while (current_rvb_id <= lastAgreedPrunableBlockId) {
    STDigest digest;
    as_->getPrevDigestFromBlock(current_rvb_id + 1, reinterpret_cast<StateTransferDigest*>(digest.getForUpdate()));
    pruned_blocks_digests_.push_back(std::make_pair(current_rvb_id, std::move(digest)));
    current_rvb_id += config_.fetchRangeSize;
    ++num_digests_added;
  }

  if (initial_size != pruned_blocks_digests_.size()) {
    ds_->setPrunedBlocksDigests(pruned_blocks_digests_);
  }
  LOG_INFO(
      logger_,
      num_digests_added << " digests saved:"
                        << KVLOG(
                               start_rvb_id, current_rvb_id, lastAgreedPrunableBlockId, pruned_blocks_digests_.size()));
}

}  // namespace bftEngine::bcst::impl
