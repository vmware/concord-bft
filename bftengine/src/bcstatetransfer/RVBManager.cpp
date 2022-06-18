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

#include <algorithm>

#include "RangeValidationTree.hpp"
#include "RVBManager.hpp"
#include "throughput.hpp"

using concord::util::digest::DigestUtil;
using namespace std;
using namespace concord::util;

namespace bftEngine::bcst::impl {

template <typename T>
static inline std::string vecToStr(const std::vector<T>& vec);

void RVBManager::rvt_deleter::operator()(RangeValidationTree* ptr) const { delete ptr; }  // used for pimpl

RVBManager::RVBManager(const Config& config, const IAppState* state_api, const std::shared_ptr<DataStore>& ds)
    : logger_{logging::getLogger("concord.bft.st.rvb")},
      config_{config},
      as_{state_api},
      ds_{ds},
      in_mem_rvt_{new RangeValidationTree(logger_, config_.RVT_K, config_.fetchRangeSize)},
      rvb_data_source_(RvbDataInitialSource::NIL),
      metrics_component_{
          concordMetrics::Component("state_transfer_rvb_manager", std::make_shared<concordMetrics::Aggregator>())},
      metrics_{metrics_component_.RegisterCounter("report_during_checkpointing_errors"),
               metrics_component_.RegisterCounter("pruning_reports"),
               metrics_component_.RegisterCounter("failures_while_setting_serialized_rvt"),
               metrics_component_.RegisterGauge("pruning_vector_elements_count", 0),
               metrics_component_.RegisterGauge("pruning_vector_size_in_bytes", 0),
               metrics_component_.RegisterGauge("stored_rvb_digests_size_in_bytes", 0)} {
  LOG_TRACE(logger_, "");
  last_checkpoint_desc_.makeZero();
}

void RVBManager::init(bool fetching) {
  LOG_TRACE(logger_, "");
  bool loaded_from_data_store = false;
  static constexpr bool print_rvt = true;
  CheckpointDesc desc{0};

  if (ds_->hasCheckpointBeingFetched()) {
    ConcordAssert(fetching);
    desc = ds_->getCheckpointBeingFetched();
  } else {
    // unknown state for RVBM
    auto last_stored_cp_num = ds_->getLastStoredCheckpoint();
    if (last_stored_cp_num > 0) {
      desc = ds_->getCheckpointDesc(last_stored_cp_num);
      last_checkpoint_desc_ = desc;
    }
  }

  // Get pruned blocks digests
  pruned_blocks_digests_ = ds_->getPrunedBlocksDigests();
  metrics_.pruning_vector_elements_count_.Get().Set(pruned_blocks_digests_.size());
  metrics_.pruning_vector_size_in_bytes_.Get().Set(pruned_blocks_digests_.size() * (sizeof(BlockId) + sizeof(Digest)));

  if (config_.enableStoreRvbDataDuringCheckpointing) {
    // Try to get RVT from persistent storage. Even if the tree exist we need to check if it
    // match current configuration
    if ((desc.checkpointNum > 0) && (!desc.rvbData.empty())) {
      // There is RVB data in this checkpoint - try to load it
      std::istringstream rvb_data(std::string(reinterpret_cast<const char*>(desc.rvbData.data()), desc.rvbData.size()));
      loaded_from_data_store = in_mem_rvt_->setSerializedRvbData(rvb_data);
      if (!loaded_from_data_store) {
        LOG_ERROR(logger_, "Failed to load RVB data from stored checkpoint" << KVLOG(desc.checkpointNum));
        metrics_.failures_while_setting_serialized_rvt_++;
      } else {
        rvb_data_source_ = RvbDataInitialSource::FROM_STORAGE_CP;

        if (!in_mem_rvt_->validate()) {
          // in some cases, if not fetching, we might try to look for other checkpoints but this is fatal enough to
          // reconstruct from storage
          LOG_ERROR(logger_, "Failed to validate loaded RVB data from stored checkpoint" << KVLOG(desc.checkpointNum));
          loaded_from_data_store = false;
          in_mem_rvt_->clear();
          rvb_data_source_ = RvbDataInitialSource::NIL;
        } else {
          LOG_INFO(logger_, "Success setting and validating new RVB data from data store!");
        }
      }
    }
  }

  if (!loaded_from_data_store && (desc.maxBlockId > 0)) {
    uint64_t num_rvbs_added{};
    // If desc data is valid, try to reconstruct by reading digests from storage (no persistency data was found)
    DurationTracker<std::chrono::milliseconds> reconstruct_dt("reconstruct_dt", true);
    auto genesis_block_id = as_->getGenesisBlockNum();
    LOG_INFO(logger_, "Reconstructing RVB data" << KVLOG(loaded_from_data_store, desc.maxBlockId, genesis_block_id));
    if (genesis_block_id <= desc.maxBlockId) {
      num_rvbs_added =
          addRvbDataOnBlockRange(genesis_block_id, desc.maxBlockId, std::optional<Digest>(desc.digestOfMaxBlockId));
      if (!in_mem_rvt_->validate()) {
        LOG_FATAL(logger_, "Failed to validate reconstructed RVB data from storage" << KVLOG(desc.checkpointNum));
        ConcordAssert(false);
      }
    }
    rvb_data_source_ = RvbDataInitialSource::FROM_STORAGE_RECONSTRUCTION;
    auto total_duration = reconstruct_dt.totalDuration(true);
    LOG_INFO(logger_, "Done Reconstructing RVB data from storage" << KVLOG(total_duration, num_rvbs_added));
  }

  LOG_INFO(logger_, std::boolalpha << KVLOG(pruned_blocks_digests_.size(), desc.checkpointNum, loaded_from_data_store));
  if (print_rvt && (debug_prints_log_level.find(getLogLevel()) != debug_prints_log_level.end())) {
    in_mem_rvt_->printToLog(LogPrintVerbosity::SUMMARY, "init");
  }
}

// Remove (Prune) blocks from RVT, and from pruned_blocks_digests_ data structure
void RVBManager::pruneRvbDataDuringCheckpoint(const CheckpointDesc& new_checkpoint_desc) {
  LOG_TRACE(logger_,
            KVLOG(pruned_blocks_digests_.size(),
                  new_checkpoint_desc.checkpointNum,
                  last_checkpoint_desc_.checkpointNum,
                  new_checkpoint_desc.maxBlockId,
                  last_checkpoint_desc_.maxBlockId));
  size_t i{};
  if (pruned_blocks_digests_.empty()) {
    return;
  }
  ConcordAssertGE(new_checkpoint_desc.checkpointNum, last_checkpoint_desc_.checkpointNum);
  ConcordAssertGE(new_checkpoint_desc.maxBlockId, last_checkpoint_desc_.maxBlockId);

  // First, Remove old blocks from pruned_blocks_digests_, then persist it. These blocks were already removed from RVB
  // data and the removal is reflected in the RVB persisted tree of the last checkpoint.
  //
  // We must prune the pruning vector based on the fact that the tree is persisted at least once in the past.
  // We cannot allow losing digests, we will never get them back (blocks are not in storage anymore) and will have
  // to reconstruct the whole tree.
  if (last_checkpoint_desc_.checkpointNum > 0) {
    auto min_rvb_id = in_mem_rvt_->getMinRvbId();
    for (i = 0; i < pruned_blocks_digests_.size(); ++i) {
      if (pruned_blocks_digests_[i].first >= min_rvb_id) {
        break;
      }
    }
    if (i > 0) {
      auto min_block_id = pruned_blocks_digests_[0].first;
      auto max_block_id = pruned_blocks_digests_[i - 1].first;
      LOG_DEBUG(logger_,
                "Remove " << i << " digests from pruned_blocks_digests_, from/to block IDs:"
                          << KVLOG(min_block_id, max_block_id));
      pruned_blocks_digests_.erase(pruned_blocks_digests_.begin(), pruned_blocks_digests_.begin() + i);
      ds_->setPrunedBlocksDigests(pruned_blocks_digests_);
      metrics_.pruning_vector_elements_count_.Get().Set(pruned_blocks_digests_.size());
      metrics_.pruning_vector_size_in_bytes_.Get().Set(pruned_blocks_digests_.size() *
                                                       (sizeof(BlockId) + sizeof(Digest)));
    }
  }

  // Second, prune in_mem_rvt_ up to new_checkpoint_desc.maxBlockId.
  // Theoretically, there might be block digests which belong to the next checkpoint
  //
  // Comment: assume RVT span on the range [160 ... 1500] min/max RVB IDs respectively. FetchRangeSize=16 and
  // new_checkpoint_desc.maxBlockId is 1500.
  // We might have in the pruning vector the next digests 144, 160 .... 1488, 1504 ...
  // 144 and 1504 should not be pruned. 144 triggers skip (continue). 1504 triggers break
  BlockId from_block_id{}, to_block_id{};
  for (i = 0; i < pruned_blocks_digests_.size(); ++i) {
    RVBId rvb_id = pruned_blocks_digests_[i].first;
    auto digest = pruned_blocks_digests_[i].second;
    if ((rvb_id <= new_checkpoint_desc.maxBlockId) && (rvb_id == in_mem_rvt_->getMinRvbId())) {
      LOG_TRACE(logger_, "Remove digest for block " << rvb_id << " ,Digest: " << digest.toString());
      if (!from_block_id) {
        from_block_id = rvb_id;
      }
      to_block_id = rvb_id;
      in_mem_rvt_->removeLeftNode(rvb_id, digest.get(), DIGEST_SIZE);
    } else if (rvb_id > new_checkpoint_desc.maxBlockId) {
      break;
    }
  }
  if (from_block_id > 0) {
    LOG_INFO(logger_,
             "Updated RVT (remove): Removed "
                 << i << " digests from in_mem_rvt_, from/to block IDs:" << KVLOG(from_block_id, to_block_id));
  }
  if (!pruned_blocks_digests_.empty() && (debug_prints_log_level.find(getLogLevel()) != debug_prints_log_level.end())) {
    ostringstream oss;
    oss << "pruned_blocks_digests_: size: " << pruned_blocks_digests_.size() << " Pairs: ";
    for (auto const& pair : pruned_blocks_digests_) {
      oss << ",[" << std::to_string(pair.first) << "," << pair.second.toString() << "]";
    }
    LOG_DEBUG(logger_, oss.str());
  }

  // We relay on caller to persist new_checkpoint_desc, and leave pruned_blocks_digests_ persisted before erase
  // was done (some redundent digests stay in vector till next checkpointing)
}

void RVBManager::updateRvbDataDuringCheckpoint(CheckpointDesc& new_checkpoint_desc) {
  BlockId add_range_min_block_id{};

  LOG_DEBUG(logger_,
            "Updating RVB data for" << KVLOG(new_checkpoint_desc.checkpointNum,
                                             new_checkpoint_desc.maxBlockId,
                                             last_checkpoint_desc_.checkpointNum,
                                             last_checkpoint_desc_.maxBlockId));
  ConcordAssertAND((last_checkpoint_desc_.maxBlockId <= new_checkpoint_desc.maxBlockId),
                   (last_checkpoint_desc_.checkpointNum < new_checkpoint_desc.checkpointNum));

  // First, remove blocks from RVT (if any blocks were pruned during the checkpoint window)
  pruneRvbDataDuringCheckpoint(new_checkpoint_desc);

  // Second, Add blocks to RVT (if any blocks were added during the checkpoint window)
  auto max_rvb_id = in_mem_rvt_->getMaxRvbId();
  if (last_checkpoint_desc_.checkpointNum != 0) {
    add_range_min_block_id = std::min(new_checkpoint_desc.maxBlockId, last_checkpoint_desc_.maxBlockId + 1);
  } else {
    if (new_checkpoint_desc.checkpointNum == 1) {
      add_range_min_block_id = as_->getGenesisBlockNum();
    } else if (ds_->hasCheckpointDesc(new_checkpoint_desc.checkpointNum - 1)) {
      last_checkpoint_desc_ = ds_->getCheckpointDesc(new_checkpoint_desc.checkpointNum - 1);
      add_range_min_block_id = std::min(new_checkpoint_desc.maxBlockId, last_checkpoint_desc_.maxBlockId + 1);
    } else {
      if (in_mem_rvt_->empty()) {
        add_range_min_block_id = as_->getGenesisBlockNum();
      } else {
        add_range_min_block_id = max_rvb_id + config_.fetchRangeSize;
      }
    }
  }
  add_range_min_block_id = (max_rvb_id == 0) ? add_range_min_block_id : max_rvb_id;
  if (!pruned_blocks_digests_.empty()) {
    add_range_min_block_id = std::max(add_range_min_block_id, pruned_blocks_digests_.back().first + 1);
  }

  addRvbDataOnBlockRange(
      add_range_min_block_id, new_checkpoint_desc.maxBlockId, new_checkpoint_desc.digestOfMaxBlockId);

  // Third, fill checkpoint and print tree
  if (!in_mem_rvt_->empty()) {
    ConcordAssert(in_mem_rvt_->validate());

    // TODO - convert straight into a vector, using stream iterator
    if (config_.enableStoreRvbDataDuringCheckpointing) {
      // keep serialied RVB data in new checkpoint descriptor
      auto rvb_data = in_mem_rvt_->getSerializedRvbData();
      const std::string s = rvb_data.str();
      ConcordAssert(!s.empty());
      std::copy(s.c_str(), s.c_str() + s.length(), back_inserter(new_checkpoint_desc.rvbData));
    } else {
      new_checkpoint_desc.rvbData.clear();
    }
    std::string label{"updateRvbDataDuringCheckpoint"};
    label += std::to_string(new_checkpoint_desc.checkpointNum);
    in_mem_rvt_->printToLog(LogPrintVerbosity::SUMMARY, label.c_str());
  }
  last_checkpoint_desc_ = new_checkpoint_desc;
}

std::ostringstream RVBManager::getRvbData() const { return in_mem_rvt_->getSerializedRvbData(); }

bool RVBManager::setRvbData(char* data, size_t data_size, BlockId min_block_id_span, BlockId max_block_id_span) {
  LOG_TRACE(logger_, "");
  ConcordAssertOR(data && (data_size > 0), !data && (data_size == 0));
  std::istringstream rvb_data(std::string(data, data_size));

  if (data) {
    if (!in_mem_rvt_->setSerializedRvbData(rvb_data)) {
      in_mem_rvt_->clear();
      LOG_ERROR(logger_, "Failed setting RVB data! (setSerializedRvbData failed!)");
      metrics_.failures_while_setting_serialized_rvt_++;
      return false;
    }

    if (!in_mem_rvt_->validate()) {
      LOG_ERROR(logger_, "Failed to validate RVB serialized data");
      in_mem_rvt_->clear();
      return false;
    }
  } else {
    reset(RVBManager::RvbDataInitialSource::FROM_NETWORK);
  }

  // Validate that tree spans at least the needed collecting range
  RVBId min_rvb_in_rvt = in_mem_rvt_->getMinRvbId();
  RVBId max_rvb_in_rvt = in_mem_rvt_->getMaxRvbId();
  RVBId min_required_rvb_id = nextRvbBlockId(min_block_id_span);
  RVBId max_required_rvb_id = prevRvbBlockId(max_block_id_span);

  if (((min_required_rvb_id <= max_required_rvb_id) && !data) ||  // At least one RVB is needed, but data is empty
      ((min_required_rvb_id < min_rvb_in_rvt) || (max_rvb_in_rvt > max_required_rvb_id))) {  // not in required span
    LOG_ERROR(logger_,
              std::boolalpha << "Tree doesn't span the required collection range!"
                             << KVLOG(min_block_id_span,
                                      max_block_id_span,
                                      min_rvb_in_rvt,
                                      max_rvb_in_rvt,
                                      min_required_rvb_id,
                                      max_required_rvb_id,
                                      (bool)(data == nullptr)));
    in_mem_rvt_->clear();
    return false;
  }

  LOG_INFO(logger_, "Success setting new RVB data from network!");
  in_mem_rvt_->printToLog(LogPrintVerbosity::SUMMARY, "setRvbData");
  rvb_data_source_ = RvbDataInitialSource::FROM_NETWORK;
  if (!data) {
    LOG_WARN(logger_, "Empty RVB data in checkpoint!");
  }

  return true;
}

size_t RVBManager::getSerializedDigestsOfRvbGroup(int64_t rvb_group_id,
                                                  char* buff,
                                                  size_t buff_max_size,
                                                  bool size_only) const {
  LOG_TRACE(logger_, KVLOG(rvb_group_id, buff_max_size, size_only));
  ConcordAssertOR((size_only && !buff && buff_max_size == 0), (!size_only && buff && buff_max_size > 0));
  std::vector<RVBId> rvb_ids = in_mem_rvt_->getRvbIds(rvb_group_id);
  RvbDigestInfoPtr cur = size_only ? nullptr : reinterpret_cast<RvbDigestInfoPtr>(buff);

  // 1) Source is working based on "best-effort" - send what I have. Reject if I have not even a single block in the
  // requested RVB group. In the case of
  // 2) Destination has to validate source and fetch block digests from local storage if its pruning state is
  // not synced.
  //
  // Requirement - the returned digests must represent a continuous series of block IDs
  size_t num_elements{0};
  BlockId last_added_block_id = 0;
  for (const auto rvb_id : rvb_ids) {
    if ((last_added_block_id != 0) && (rvb_id != last_added_block_id + config_.fetchRangeSize)) {
      // non continuos!
      LOG_ERROR(logger_, KVLOG(last_added_block_id, config_.fetchRangeSize, rvb_id, num_elements, rvb_group_id));
      return 0;
    }

    // Two places where block digests can be found:
    // 1) In pruned_blocks_digests_
    // 2) In storage
    if (as_->hasBlock(rvb_id + 1)) {
      // have the next block - much faster to get only the digest
      if (!size_only) {
        if (!as_->getPrevDigestFromBlock(rvb_id + 1,
                                         reinterpret_cast<StateTransferDigest*>(cur->digest.getForUpdate()))) {
          LOG_FATAL(logger_, "Digest not found:" << KVLOG(rvb_id, last_added_block_id, num_elements, rvb_group_id));
          ConcordAssert(false);
        }
        cur->block_id = rvb_id;
      }
    } else if (as_->hasBlock(rvb_id)) {
      if (!size_only) {
        // compute the digests
        cur->digest = getBlockAndComputeDigest(rvb_id);
        cur->block_id = rvb_id;
      }
    } else if (auto digest_iter =
                   std::find_if(pruned_blocks_digests_.begin(),
                                pruned_blocks_digests_.end(),
                                [rvb_id](const std::pair<BlockId, Digest>& p) { return p.first == rvb_id; });
               digest_iter != pruned_blocks_digests_.end()) {
      if (!size_only) {
        cur->digest = digest_iter->second;
        cur->block_id = digest_iter->first;
      }
    } else {
      continue;
    }
    last_added_block_id = rvb_id;
    ++num_elements;
    ++cur;
  }
  return num_elements * sizeof(RvbDigestInfo);
}

bool RVBManager::setSerializedDigestsOfRvbGroup(char* data,
                                                size_t data_size,
                                                BlockId min_fetch_block_id,
                                                BlockId max_fetch_block_id,
                                                BlockId max_block_id_in_cycle) {
  LOG_TRACE(logger_, KVLOG(data_size));
  ConcordAssertNE(data, nullptr);
  RvbDigestInfoPtr cur = reinterpret_cast<RvbDigestInfoPtr>(data);
  std::map<BlockId, Digest> digests;
  BlockId block_id;
  Digest digest;
  size_t num_digests_in_data;
  std::vector<RVBGroupId> rvb_group_ids;
  static constexpr char error_prefix[] = "Invalid digests of RVB group:";
  RVBId max_block_in_rvt = in_mem_rvt_->getMaxRvbId();
  RVBId min_block_in_rvt = in_mem_rvt_->getMinRvbId();
  BlockId next_expected_rvb_id{};
  RVBGroupId next_required_rvb_group_id =
      getNextRequiredRvbGroupid(nextRvbBlockId(min_fetch_block_id), prevRvbBlockId(max_fetch_block_id));

  if (((data_size % sizeof(RvbDigestInfo)) != 0) || (data_size == 0)) {
    LOG_ERROR(logger_, error_prefix << KVLOG(data_size, sizeof(RvbDigestInfo)));
    return false;
  }
  num_digests_in_data = data_size / sizeof(RvbDigestInfo);
  if (num_digests_in_data > config_.RVT_K) {
    LOG_ERROR(logger_, error_prefix << KVLOG(num_digests_in_data, config_.RVT_K));
    return false;
  }
  if (num_digests_in_data == 0) {
    LOG_ERROR(logger_, error_prefix << KVLOG(num_digests_in_data));
    return false;
  }

  // 1st stage: we would like to construct a temporary map 'digests', nominated to be inserted into
  // stored_rvb_digests_ This will be done  after basic validations + validating this list of digests against the in
  // memory RVT We assume digests are ordered in accending block ID order
  for (size_t i{0}; i < num_digests_in_data; ++i, ++cur) {
    block_id = cur->block_id;
    if ((block_id % config_.fetchRangeSize) != 0) {
      LOG_ERROR(logger_,
                error_prefix << KVLOG(i, block_id, config_.fetchRangeSize, (block_id % config_.fetchRangeSize)));
      return false;
    }
    if (stored_rvb_digests_.find(block_id) != stored_rvb_digests_.end()) {
      LOG_WARN(logger_, error_prefix << KVLOG(block_id) << " is already inside stored_rvb_digests_ (continue)");
    }
    if (digests.find(block_id) != digests.end()) {
      LOG_WARN(logger_, error_prefix << KVLOG(block_id) << " is already inside digests (continue)");
    }
    if (block_id < min_block_in_rvt) {
      LOG_WARN(logger_, error_prefix << KVLOG(block_id, min_block_in_rvt) << " (continue)");
    }
    // Break in case that we passed the max_block_id_in_cycle or max_block_in_rvt
    if ((block_id > max_block_id_in_cycle) || (block_id > max_block_in_rvt)) {
      LOG_DEBUG(logger_, "Breaking:" << (KVLOG(block_id, max_block_id_in_cycle, max_block_in_rvt)));
      break;
    }
    if ((next_expected_rvb_id != 0) && (block_id != next_expected_rvb_id)) {
      LOG_ERROR(logger_, error_prefix << KVLOG(block_id, next_expected_rvb_id));
      return false;
    }
    rvb_group_ids = in_mem_rvt_->getRvbGroupIds(block_id, block_id, true);
    ConcordAssertEQ(rvb_group_ids.size(), 1);
    if (rvb_group_ids.empty()) {
      LOG_ERROR(logger_, "Bad Digests of RVB group: rvb_group_ids is empty!" << KVLOG(block_id));
      return false;
    }
    if (next_required_rvb_group_id != rvb_group_ids[0]) {
      LOG_ERROR(logger_, "Bad Digests of RVB group:" << KVLOG(block_id, rvb_group_ids[0], next_required_rvb_group_id));
      return false;
    }
    digest = cur->digest;
    digests.insert(make_pair(block_id, digest));
    next_expected_rvb_id = block_id + config_.fetchRangeSize;
  }  // for

  ConcordAssertLE(digests.size(), num_digests_in_data);
  if (digests.empty()) {
    LOG_ERROR(logger_, error_prefix << " digests map is empty!");
    return false;
  }

  // 2nd stage: Check that I have the exact digests to validate the RVB group. There are 4 type of RVB groups,
  // Level 0 in tree is represented (not in memory), 'x' represents an RVB node:
  // 1) Full RVB Group:            [xxxxxxxxx]
  //
  // 2) Partial right RVB Group:   [    xxxxx]    - some left RVB are pruned. This one represents "oldest" blocks in
  // RVT.
  //
  // 3) Partial left RVB Group:    [xxxxx    ]    - some right RVB are not yet added. This one represents
  // "newest" blocks in RVT.
  //
  // 4) Partial RVB Group:         [  xxxxx  ]      - some right and left RVBs are not part of
  // the tree. In this case we expect a single root tree!
  //
  // In all cases, we can validate the group only if have validate the EXACT digests - no one less and not a single
  // more!
  RVBGroupId rvb_group_id_added = rvb_group_ids[0];
  auto rvb_ids_local = in_mem_rvt_->getRvbIds(rvb_group_id_added);
  std::vector<RVBId> rvb_ids_src;
  std::transform(digests.begin(),
                 digests.end(),
                 std::back_inserter(rvb_ids_src),
                 [](const std::map<BlockId, Digest>::value_type& pair) { return pair.first; });
  if (rvb_ids_src != rvb_ids_local) {
    std::string rvb_ids_src_str = vecToStr(rvb_ids_src);
    std::string rvb_ids_local_str = vecToStr(rvb_ids_local);
    LOG_ERROR(logger_, error_prefix << KVLOG(rvb_ids_src_str, rvb_ids_local_str));
    return false;
  }

  // 3rd stage: we have constructed temporary map 'digests' of RVBs.
  // Lets validate them against the in memory tree. We assume that no pruning was done, so we have all the RVBs
  RangeValidationTree digests_rvt(logger_, config_.RVT_K, config_.fetchRangeSize);
  for (auto& p : digests) {
    digests_rvt.addRightNode(p.first, p.second.get(), DIGEST_SIZE);
  }
  const std::string digests_rvt_root_val = digests_rvt.getRootCurrentValueStr();
  const std::string rvt_parent_val = in_mem_rvt_->getDirectParentValueStr(digests.begin()->first);
  if (digests_rvt_root_val != rvt_parent_val) {
    LOG_ERROR(logger_,
              error_prefix << " digests validation failed against the in_mem_rvt_!"
                           << KVLOG(digests_rvt_root_val, rvt_parent_val));
    return false;
  }

  // 4th stage: validation is done!
  // insert the new group id and delete old ones, if needed. We keep the latest 2 group IDs after insertion
  RVBGroupId rvb_group_id_removed{0};
  stored_rvb_digests_group_ids_.push_back(rvb_group_id_added);
  size_t num_digests_removed{0};
  if (stored_rvb_digests_group_ids_.size() > 2) {
    rvb_group_id_removed = stored_rvb_digests_group_ids_[0];
    stored_rvb_digests_group_ids_.erase(stored_rvb_digests_group_ids_.begin());
    auto it = stored_rvb_digests_.begin();
    while (it != stored_rvb_digests_.end()) {
      rvb_group_ids = in_mem_rvt_->getRvbGroupIds(it->first, it->first, true);
      ConcordAssertEQ(rvb_group_ids.size(), 1);
      if (rvb_group_ids[0] == rvb_group_id_removed) {
        it = stored_rvb_digests_.erase(it);
        ++num_digests_removed;
      } else {
        ++it;
      }
    }
  }
  stored_rvb_digests_.merge(digests);
  metrics_.stored_rvb_digests_size_in_bytes_.Get().Set(stored_rvb_digests_.size() * (sizeof(BlockId) + sizeof(Digest)));
  LOG_INFO(logger_,
           "Done updating RVB stored digests:" << KVLOG(rvb_group_id_added,
                                                        rvb_group_id_removed,
                                                        num_digests_in_data,
                                                        digests.size(),
                                                        num_digests_removed,
                                                        stored_rvb_digests_.size(),
                                                        stored_rvb_digests_group_ids_.size()));
  return true;
}

std::optional<std::reference_wrapper<const Digest>> RVBManager::getDigestFromStoredRvb(BlockId block_id) const {
  LOG_TRACE(logger_, KVLOG(block_id));
  const auto iter = stored_rvb_digests_.find(block_id);
  if (iter == stored_rvb_digests_.end()) {
    LOG_ERROR(logger_, KVLOG(block_id) << " not found in stored_rvb_digests_");
    return std::nullopt;
  }
  return std::optional<std::reference_wrapper<const Digest>>(iter->second);
}

RVBGroupId RVBManager::getFetchBlocksRvbGroupId(BlockId from_block_id, BlockId to_block_id) const {
  LOG_TRACE(logger_, KVLOG(from_block_id, to_block_id));
  ConcordAssertLE(from_block_id, to_block_id);

  if (!stored_rvb_digests_.empty()) {
    BlockId min_stored_rvb_id, max_stored_rvb_id;
    min_stored_rvb_id = stored_rvb_digests_.begin()->first;
    max_stored_rvb_id = (--stored_rvb_digests_.end())->first;
    uint64_t diff = max_stored_rvb_id - min_stored_rvb_id;
    ConcordAssertEQ((diff % config_.fetchRangeSize), 0);
    ConcordAssertEQ((diff / config_.fetchRangeSize) + 1, stored_rvb_digests_.size());

    if ((from_block_id >= min_stored_rvb_id) && (to_block_id <= max_stored_rvb_id)) {
      // we have all the digests, no need to ask for anything for now
      return 0;
    }
  }

  // If we are here, we might have non of the digests, or only part of them. Return the next fully of partial
  // RVBGroupId which is not stored. As of now, requesting multiple RVBGroupId in a single request is not supported.
  // 0 is returned if there are not RVBs in the range [from_block_id, to_block_id]
  RVBId from_rvb_id = nextRvbBlockId(from_block_id);
  RVBId to_rvb_id = prevRvbBlockId(to_block_id);
  if (from_rvb_id > to_rvb_id) {
    // There are no RVBs in that range
    return 0;
  }
  return getNextRequiredRvbGroupid(from_rvb_id, to_rvb_id);
}

RVBGroupId RVBManager::getNextRequiredRvbGroupid(RVBId from_rvb_id, RVBId to_rvb_id) const {
  LOG_TRACE(logger_, KVLOG(from_rvb_id, to_rvb_id));
  if (from_rvb_id > to_rvb_id) return 0;
  const auto rvb_group_ids = in_mem_rvt_->getRvbGroupIds(from_rvb_id, to_rvb_id, true);
  for (const auto& id : rvb_group_ids) {
    if (std::find(stored_rvb_digests_group_ids_.begin(), stored_rvb_digests_group_ids_.end(), id) ==
        stored_rvb_digests_group_ids_.end()) {
      return id;
    }
  }
  return 0;
}

BlockId RVBManager::getRvbGroupMaxBlockIdOfNonStoredRvbGroup(BlockId from_block_id, BlockId to_block_id) const {
  LOG_TRACE(logger_, KVLOG(from_block_id, to_block_id));
  ConcordAssertLE(from_block_id, to_block_id);
  auto min_rvb_id = nextRvbBlockId(from_block_id);
  auto max_rvb_id = prevRvbBlockId(to_block_id);

  if (min_rvb_id >= max_rvb_id) {
    return to_block_id;
  }

  const auto rvb_group_ids = in_mem_rvt_->getRvbGroupIds(min_rvb_id, max_rvb_id, true);
  if (rvb_group_ids.size() < 2) {
    return to_block_id;
  }

  // if we have 2 or more, we need to take the 1st one which is not stored, and find the upper bound on the last RVB
  // in the list
  for (const auto& rvb_group_id : rvb_group_ids) {
    if (std::find(stored_rvb_digests_group_ids_.begin(), stored_rvb_digests_group_ids_.end(), rvb_group_id) ==
        stored_rvb_digests_group_ids_.end()) {
      auto rvb_ids = in_mem_rvt_->getRvbIds(rvb_group_id);
      ConcordAssert(!rvb_ids.empty());
      auto blockId = rvb_ids.back();
      ConcordAssertGE(blockId, min_rvb_id);
      return blockId;
    }
  }
  return to_block_id;
}

void RVBManager::computeDigestOfBlock(const uint64_t block_id,
                                      const char* block,
                                      const uint32_t block_size,
                                      char* out_digest) const {
  ConcordAssertGT(block_id, 0);
  ConcordAssertGT(block_size, 0);
  DigestUtil::Context c;
  c.update(reinterpret_cast<const char*>(&block_id), sizeof(block_id));
  c.update(block, block_size);
  c.writeDigest(out_digest);
}

// TODO - BCStateTran has a similar function + computeDigestOfBlock.
// Move common functions into BCStateTranCommon.hpp/cpp
const Digest RVBManager::getBlockAndComputeDigest(uint64_t block_id) const {
  LOG_TRACE(logger_, KVLOG(block_id));
  static std::unique_ptr<char[]> buffer(new char[config_.maxBlockSize]);
  Digest digest;
  uint32_t block_size{0};
  as_->getBlock(block_id, buffer.get(), config_.maxBlockSize, &block_size);
  computeDigestOfBlock(block_id, buffer.get(), block_size, digest.getForUpdate());
  return digest;
}

uint64_t RVBManager::addRvbDataOnBlockRange(uint64_t min_block_id,
                                            uint64_t max_block_id,
                                            const std::optional<Digest>& digest_of_max_block_id) {
  LOG_TRACE(logger_, KVLOG(min_block_id, max_block_id));
  ConcordAssertLE(min_block_id, max_block_id);
  uint64_t num_rvbs_added{};

  if (max_block_id == 0) {
    LOG_WARN(logger_, KVLOG(max_block_id));
    return 0;
  }
  uint64_t current_rvb_id = nextRvbBlockId(min_block_id);
  RVBId max_rvb_id_in_rvt = in_mem_rvt_->getMaxRvbId();
  while (current_rvb_id < max_block_id) {  // we handle case of current_rvb_id == max_block_id later
    // TODO - As a 2nd phase - should use the thread pool to fetch a batch of digests or move to a background
    // process
    if (current_rvb_id > max_rvb_id_in_rvt) {
      Digest digest;
      if (!as_->getPrevDigestFromBlock(current_rvb_id + 1,
                                       reinterpret_cast<StateTransferDigest*>(digest.getForUpdate()))) {
        LOG_FATAL(logger_,
                  "Digest not found:" << KVLOG(
                      min_block_id, max_block_id, current_rvb_id, max_rvb_id_in_rvt, num_rvbs_added));
        ConcordAssert(false);
      }
      LOG_DEBUG(logger_,
                "Add digest for block " << current_rvb_id << " "
                                        << " Digest: " << digest.toString());
      in_mem_rvt_->addRightNode(current_rvb_id, digest.getForUpdate(), DIGEST_SIZE);
      ++num_rvbs_added;
    }
    current_rvb_id += config_.fetchRangeSize;
  }
  if ((current_rvb_id == max_block_id) && (current_rvb_id > max_rvb_id_in_rvt)) {
    if (digest_of_max_block_id) {
      const auto& digest = digest_of_max_block_id.value();
      LOG_DEBUG(logger_,
                "Add digest for block " << current_rvb_id << " "
                                        << " ,Digest: " << digest.toString());
      in_mem_rvt_->addRightNode(current_rvb_id, digest.get(), DIGEST_SIZE);
      ++num_rvbs_added;
    } else {
      auto digest = getBlockAndComputeDigest(max_block_id);
      LOG_DEBUG(logger_,
                "Add digest for block " << current_rvb_id << " "
                                        << " ,Digest: " << digest.toString());
      in_mem_rvt_->addRightNode(current_rvb_id, digest.get(), DIGEST_SIZE);
      ++num_rvbs_added;
    }
  }
  if (num_rvbs_added > 0) {
    LOG_INFO(logger_,
             "Updated RVT (add):" << KVLOG(min_block_id, max_block_id, num_rvbs_added, as_->getLastBlockNum()));
  }
  return num_rvbs_added;
}

RVBId RVBManager::nextRvbBlockId(BlockId block_id) const {
  uint64_t next_rvb_id = config_.fetchRangeSize * (block_id / config_.fetchRangeSize);
  if (next_rvb_id < block_id) {
    next_rvb_id += config_.fetchRangeSize;
  }
  return next_rvb_id;
}

void RVBManager::reportLastAgreedPrunableBlockId(uint64_t lastAgreedPrunableBlockId) {
  DurationTracker<std::chrono::milliseconds> store_pruned_digests_dt("store_pruned_digests_dt", true);
  LOG_TRACE(logger_, KVLOG(lastAgreedPrunableBlockId));
  metrics_.pruning_reports_++;
  auto initial_size = pruned_blocks_digests_.size();
  RVBId start_rvb_id = in_mem_rvt_->getMinRvbId();

  if (start_rvb_id == 0) {
    // In some cases tree is still not built, we still have to keep the pruned digests
    start_rvb_id = pruned_blocks_digests_.empty() ? as_->getGenesisBlockNum() : pruned_blocks_digests_.back().first;
  }

  start_rvb_id = nextRvbBlockId(start_rvb_id);
  if (lastAgreedPrunableBlockId < start_rvb_id) {
    LOG_WARN(logger_,
             "Current pruning report has no impact on RVB data:" << KVLOG(lastAgreedPrunableBlockId, start_rvb_id));
    return;
  }

  uint64_t current_rvb_id = start_rvb_id;
  int32_t num_digests_added{0};
  while (current_rvb_id <= lastAgreedPrunableBlockId) {
    Digest digest;
    if (!pruned_blocks_digests_.empty() && (current_rvb_id <= pruned_blocks_digests_.back().first)) {
      current_rvb_id += config_.fetchRangeSize;
      continue;
    }
    if (!as_->getPrevDigestFromBlock(current_rvb_id + 1,
                                     reinterpret_cast<StateTransferDigest*>(digest.getForUpdate()))) {
      LOG_FATAL(logger_,
                "Digest not found:" << KVLOG(lastAgreedPrunableBlockId,
                                             initial_size,
                                             start_rvb_id,
                                             current_rvb_id,
                                             num_digests_added,
                                             pruned_blocks_digests_.size()));
      ConcordAssert(false);
    }
    pruned_blocks_digests_.push_back(std::make_pair(current_rvb_id, std::move(digest)));
    current_rvb_id += config_.fetchRangeSize;
    ++num_digests_added;
  }

  if (initial_size != pruned_blocks_digests_.size()) {
    auto total_duration = store_pruned_digests_dt.totalDuration();
    ds_->setPrunedBlocksDigests(pruned_blocks_digests_);
    LOG_INFO(logger_,
             num_digests_added << " pruned block digests saved:"
                               << KVLOG(start_rvb_id,
                                        current_rvb_id,
                                        lastAgreedPrunableBlockId,
                                        pruned_blocks_digests_.size(),
                                        total_duration));
  } else {
    LOG_INFO(logger_,
             "pruned block digests was not updated:" << KVLOG(initial_size,
                                                              pruned_blocks_digests_.size(),
                                                              pruned_blocks_digests_.front().first,
                                                              pruned_blocks_digests_.back().first));
  }
  metrics_.pruning_vector_elements_count_.Get().Set(pruned_blocks_digests_.size());
  metrics_.pruning_vector_size_in_bytes_.Get().Set(pruned_blocks_digests_.size() * (sizeof(BlockId) + sizeof(Digest)));
}

std::string RVBManager::getStateOfRvbData() const {
  auto val = in_mem_rvt_->getRootCurrentValueStr();
  return val.empty() ? "EMPTY" : val;
}

void RVBManager::UpdateAggregator() {
  metrics_component_.UpdateAggregator();
  in_mem_rvt_->UpdateAggregator();
}

concordMetrics::Component& RVBManager::getRvtMetricComponent() { return in_mem_rvt_->getMetricComponent(); }

void RVBManager::setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  metrics_component_.SetAggregator(aggregator);
  in_mem_rvt_->setAggregator(aggregator);
}

bool RVBManager::validate() const {
  // TODO - consider a pedantic mode, in which we also validate level 1 (end of ST) as well by fetching all block
  // digests
  return in_mem_rvt_->validate();
}

void RVBManager::reset(RvbDataInitialSource inital_source) {
  LOG_TRACE(logger_, "");
  in_mem_rvt_->clear();
  stored_rvb_digests_.clear();
  stored_rvb_digests_group_ids_.clear();
  last_checkpoint_desc_.makeZero();
  rvb_data_source_ = inital_source;
  // we do not clear the pruned digests
}

// TODO - move to a new ST utility file
std::string RVBManager::getLogLevel() const {
  auto log_level = logger_.getLogLevel();
#ifdef USE_LOG4CPP
  return (log_level == log4cplus::TRACE_LOG_LEVEL)
             ? "trace"
             : (log_level == log4cplus::DEBUG_LOG_LEVEL)
                   ? "trace"
                   : (log_level == log4cplus::INFO_LOG_LEVEL)
                         ? "info"
                         : (log_level == log4cplus::WARN_LOG_LEVEL)
                               ? "warning"
                               : (log_level == log4cplus::ERROR_LOG_LEVEL)
                                     ? "error"
                                     : (log_level == log4cplus::FATAL_LOG_LEVEL) ? "fatal" : "info";
#else
  return (log_level == logging::LogLevel::trace)
             ? "trace"
             : (log_level == logging::LogLevel::debug)
                   ? "trace"
                   : (log_level == logging::LogLevel::info)
                         ? "info"
                         : (log_level == logging::LogLevel::warning)
                               ? "warning"
                               : (log_level == logging::LogLevel::error)
                                     ? "error"
                                     : (log_level == logging::LogLevel::fatal) ? "fatal" : "info";
#endif
}

template <typename T>
static inline std::string vecToStr(const std::vector<T>& vec) {
  std::stringstream ss;
  for (size_t i{0}; i < vec.size(); ++i) {
    if (i != 0) ss << ",";
    ss << vec[i];
  }
  return ss.str();
}

}  // namespace bftEngine::bcst::impl
