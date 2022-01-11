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

RVBManager::RVBManager(const Config& config, const IAppState* state_api, const std::shared_ptr<DataStore>& ds)
    : logger_{logging::getLogger("concord.bft.st.rvb")},
      config_{config},
      as_{state_api},
      ds_{ds},
      in_mem_rvt_{std::make_unique<RangeValidationTree>(logger_, config_.RVT_K, config_.fetchRangeSize)} {
  LOG_TRACE(logger_, "");
  last_checkpoint_desc_.makeZero();
}

void RVBManager::init(bool fetching) {
  LOG_TRACE(logger_, "");
  bool loaded_from_data_store = false;
  static constexpr bool print_rvt = true;
  CheckpointDesc desc{0};
  std::lock_guard<std::mutex> guard(pruned_blocks_digests_mutex_);

  // RVB Manager is unaware of the ST state. It first looks for a checkpoint being fetch. If not found, it looks for
  // last stored cp
  if (ds_->hasCheckpointBeingFetched()) {
    ConcordAssert(fetching);
    desc = ds_->getCheckpointBeingFetched();
  } else {
    ConcordAssert(!fetching);
    auto last_stored_cp_num = ds_->getLastStoredCheckpoint();
    if (last_stored_cp_num > 0) {
      desc = ds_->getCheckpointDesc(last_stored_cp_num);
    }
  }

  // Get pruned blocks digests
  pruned_blocks_digests_ = ds_->getPrunedBlocksDigests();

  // Try to get RVT from persistent storage. Even if the tree exist we need to check if it
  // match current configuration
  if ((desc.checkpointNum > 0) && (!desc.rvbData.empty())) {
    // There is RVB data in this checkpoint - try to load it
    std::istringstream rvb_data(std::string(reinterpret_cast<const char*>(desc.rvbData.data()), desc.rvbData.size()));
    // TODO - deserialize should return a bool - it might fail due to logical/config issue.
    loaded_from_data_store = in_mem_rvt_->setSerializedRvbData(rvb_data);
    if (!loaded_from_data_store) {
      LOG_ERROR(logger_, "Failed to load RVB data from stored checkpoint" << KVLOG(desc.checkpointNum));
    }
  }

  if (!loaded_from_data_store && (desc.maxBlockId > 0)) {
    // If desc data is valid, try to reconstruct by reading digests from storage (no persistency data was found)
    LOG_ERROR(logger_, "Reconstructing RVB data" << KVLOG(loaded_from_data_store, desc.maxBlockId));
    addRvbDataOnBlockRange(
        as_->getGenesisBlockNum(), desc.maxBlockId, std::optional<STDigest>(desc.digestOfMaxBlockId));
  }

  // TODO - print also the root hash
  LOG_INFO(logger_, std::boolalpha << KVLOG(pruned_blocks_digests_.size(), desc.checkpointNum, loaded_from_data_store));
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

    // First, prune and persist pruned_blocks_digests_. We do this while comparing to
    // last_checkpoint_desc_, while we know that all digests up to that point are removed from RVT and RVT is persisted
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

      // Second, prune in_mem_rvt_ up to new_checkpoint_desc.maxBlockId.
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

  // TODO - replace with in_mem_rvt_->empty()
  if (!in_mem_rvt_->getRootHashVal().empty()) {
    auto rvb_data = in_mem_rvt_->getSerializedRvbData();
    // TODO - see if we can convert the rvb_data stream straight into a vector, using stream interator
    const std::string s = rvb_data.str();
    ConcordAssert(!s.empty());
    std::copy(s.c_str(), s.c_str() + s.length(), back_inserter(new_checkpoint_desc.rvbData));
  }
  last_checkpoint_desc_ = new_checkpoint_desc;
}

void RVBManager::setRvbData(char* data, size_t data_size) {
  LOG_TRACE(logger_, "");
  std::istringstream rvb_data(std::string(data, data_size));
  // TODO - deserialize should return a bool - it might fail due to logical/config issue. handle error in that case.
  in_mem_rvt_->setSerializedRvbData(rvb_data);
}

size_t RVBManager::getSerializedByteSizeOfRvbGroup(int64_t rvb_group_id) const {
  LOG_TRACE(logger_, "");
  // TODO - we are calling getRvbIds twice (see getSerializedDigestsOfRvbGroup), we can optimize this
  std::vector<RVBId> rvb_ids = in_mem_rvt_->getRvbIds(rvb_group_id);
  return rvb_ids.size() * sizeof(RVBManager::rvbDigestInfo);
}

size_t RVBManager::getSerializedDigestsOfRvbGroup(int64_t rvb_group_id, char* buff, size_t buff_max_size) const {
  LOG_TRACE(logger_, "");
  std::vector<RVBId> rvb_ids = in_mem_rvt_->getRvbIds(rvb_group_id);
  size_t total_size = rvb_ids.size() * sizeof(RVBManager::rvbDigestInfo);
  ConcordAssertLE(total_size, buff_max_size);
  RVBManager::rvbDigestInfo* cur = reinterpret_cast<RVBManager::rvbDigestInfo*>(buff);
  size_t num_elements{0};
  for (const auto rvb_id : rvb_ids) {
    cur->block_id = rvb_id;
    if (as_->hasBlock(rvb_id + 1)) {
      as_->getPrevDigestFromBlock(rvb_id + 1, reinterpret_cast<StateTransferDigest*>(cur->digest.getForUpdate()));
    } else if (as_->hasBlock(rvb_id)) {
      cur->digest = getBlockAndComputeDigest(rvb_id);
    } else {
      // It is not guaranteed that replicas holds the whole RVB group. In that case only part of it is sent.
      break;
    }
    ++num_elements;
    ++cur;
  }
  return num_elements * sizeof(RVBManager::rvbDigestInfo);
}

bool RVBManager::setSerializedDigestsOfRvbGroup(char* data,
                                                size_t data_size,
                                                BlockId min_fetch_block_id,
                                                BlockId max_fetch_block_id,
                                                BlockId max_block_id_in_cycle) {
  LOG_TRACE(logger_, KVLOG(data_size));
  ConcordAssertNE(data, nullptr);
  rvbDigestInfo* cur = reinterpret_cast<rvbDigestInfo*>(data);
  std::map<BlockId, STDigest> digests;
  BlockId block_id;
  STDigest digest;
  size_t num_digests_in_data;
  std::vector<RVBGroupId> rvb_group_ids /*prev_rvb_group_ids*/;
  static constexpr char error_prefix[] = "Invalid digests of RVB group:";
  // BlockId next_expected_rvb_id = stored_rvb_digests_.empty()
  //                                    ? computeNextRvbBlockId(min_fetch_block_id)
  //                                    : (stored_rvb_digests_.rbegin()->first + config_.fetchRangeSize);
  BlockId next_expected_rvb_id {};
  RVBGroupId next_required_rvb_group_id =
      getNextRequiredRvbGroupid(computeNextRvbBlockId(min_fetch_block_id), computePrevRvbBlockId(max_fetch_block_id));
  std::function<std::string(std::vector<RVBGroupId>&)> vec_to_str = [&](std::vector<RVBGroupId>& v) {
    std::stringstream ss;
    for (size_t i{0}; i < v.size(); ++i) {
      if (i != 0) ss << ",";
      ss << v[i];
    }
    return ss.str();
  };

  if (((data_size % sizeof(rvbDigestInfo)) != 0) || (data_size == 0)) {
    LOG_ERROR(logger_, error_prefix << KVLOG(data_size, sizeof(rvbDigestInfo)));
    return false;
  }
  num_digests_in_data = data_size / sizeof(rvbDigestInfo);
  if (num_digests_in_data > config_.RVT_K) {
    LOG_ERROR(logger_, error_prefix << KVLOG(num_digests_in_data, config_.RVT_K));
    return false;
  }
  if (num_digests_in_data == 0) {
    LOG_ERROR(logger_, error_prefix << KVLOG(num_digests_in_data));
    return false;
  }

  // 1st stage: Awe would like to construct a temporary map 'digests', nominated to be inserted into stored_rvb_digests_
  // This will be done  after basic validations + validating this list of digests agains the in memory RVT
  // We assume digests are ordered in accending block ID order
  for (size_t i{0}; i < num_digests_in_data; ++i, ++cur) {
    memcpy(&block_id, &cur->block_id, sizeof(block_id));
    if (stored_rvb_digests_.find(block_id) != stored_rvb_digests_.end()) {
      LOG_WARN(logger_, error_prefix << KVLOG(block_id) << " is already inside stored_rvb_digests_ (continue)");
    }
    if (digests.find(block_id) != digests.end()) {
      LOG_WARN(logger_,
               error_prefix << KVLOG(block_id) << " is already inside in digests (continue)");
    }
    if ((block_id % config_.fetchRangeSize) != 0) {
      LOG_ERROR(logger_,
                error_prefix << KVLOG(i, block_id, config_.fetchRangeSize, (block_id % config_.fetchRangeSize)));
      return false;
    }
    // Break in case that we passed the max_block_id_in_cycle
    if (block_id > max_block_id_in_cycle) {
      LOG_INFO(logger_, "Breaking:" << (KVLOG(block_id, max_block_id_in_cycle)));
      break;
    }
    if ((next_expected_rvb_id != 0) && (block_id != next_expected_rvb_id)) {
      LOG_ERROR(logger_, error_prefix << KVLOG(block_id, next_expected_rvb_id));
      return false;
    }
    rvb_group_ids = in_mem_rvt_->getRvbGroupIds(block_id, block_id);
    ConcordAssertEQ(rvb_group_ids.size(), 1);
    if (rvb_group_ids.empty()) {
      LOG_ERROR(logger_, "Bad Digests of RVB group: rvb_group_ids is empty!" << KVLOG(block_id));
      return false;
    }
    // if (!prev_rvb_group_ids.empty()) {
    //   bool result = std::equal(prev_rvb_group_ids.begin(), prev_rvb_group_ids.end(), rvb_group_ids.begin());
    //   if (!result) {
    //     std::string prev_rvb_group_ids_str = vec_to_str(prev_rvb_group_ids);
    //     std::string rvb_group_ids_str = vec_to_str(rvb_group_ids);
    //     LOG_ERROR(logger_,
    //               "Bad Digests of RVB group: rvb_group_ids != prev_rvb_group_ids"
    //                   << KVLOG(block_id, prev_rvb_group_ids_str, rvb_group_ids_str));
    //     return false;
    //   }
    // }
    // prev_rvb_group_ids = rvb_group_ids;
    if (next_required_rvb_group_id != rvb_group_ids[0]) {
      LOG_ERROR(logger_, "Bad Digests of RVB group:" << KVLOG(block_id, rvb_group_ids[0], next_required_rvb_group_id));
      return false;
    }
    memcpy(&digest, &cur->digest, sizeof(digest));
    digests.insert(make_pair(block_id, digest));
    next_expected_rvb_id = block_id + config_.fetchRangeSize;
  }  // for

  ConcordAssertLE(digests.size(), num_digests_in_data);
  if (digests.empty()) {
    LOG_ERROR(logger_, error_prefix << " digests map is empty!");
    return false;
  }
  RVBGroupId rvb_group_id_added = rvb_group_ids[0];

  // 2nd stage - This one isn't a mandatory, but it can help debugging - lets check that digests hold the exact needed
  // RVB digests to build a temporary tree
  auto rvb_ids = in_mem_rvt_->getRvbIds(rvb_group_id_added);
  std::vector<RVBId> keys;
  std::transform(digests.begin(),
                 digests.end(),
                 std::back_inserter(keys),
                 [](const std::map<BlockId, STDigest>::value_type& pair) { return pair.first; });
  if (keys != rvb_ids) {
    std::string keys_str = vec_to_str(keys);
    std::string rvb_ids_str = vec_to_str(rvb_ids);
    LOG_ERROR(logger_, error_prefix << KVLOG(keys_str, rvb_ids_str));
    return false;
  }

  // 3rd stage: we have constructed temporary map 'digests' of RVBs.
  // Lets validate them against the in memory tree. We assume that no pruning was done, so we have all the RVBs
  RangeValidationTree digests_rvt(logger_, config_.RVT_K, config_.fetchRangeSize);
  for (const auto& p : digests) {
    digests_rvt.addNode(p.first, p.second);
  }
  const std::string digests_rvt_root_val = digests_rvt.getRootHashVal();
  const std::string rvt_parent_val = in_mem_rvt_->getDirectParentHashVal(digests.begin()->first);
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
      rvb_group_ids = in_mem_rvt_->getRvbGroupIds(it->first, it->first);
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

std::optional<std::reference_wrapper<const STDigest>> RVBManager::getDigestFromRvbGroup(BlockId block_id) const {
  LOG_TRACE(logger_, KVLOG(block_id));
  const auto iter = stored_rvb_digests_.find(block_id);
  if (iter == stored_rvb_digests_.end()) {
    ostringstream oss;
    oss << KVLOG(block_id) << " not found in stored_rvb_digests_";
    LOG_ERROR(logger_, KVLOG(block_id) << " not found in stored_rvb_digests_");
    return std::nullopt;
  }
  return std::optional<std::reference_wrapper<const STDigest>>(iter->second);
}

uint64_t RVBManager::getFetchBlocksRvbGroupId(BlockId from_block_id, BlockId to_block_id) const {
  ConcordAssertLE(from_block_id, to_block_id);
  LOG_TRACE(logger_, KVLOG(from_block_id, to_block_id));

  BlockId min_stored_rvb_id, max_stored_rvb_id;
  if (!stored_rvb_digests_.empty()) {
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
  RVBId from_rvb_id = computeNextRvbBlockId(from_block_id);
  RVBId to_rvb_id = computePrevRvbBlockId(to_block_id);
  if (from_rvb_id > to_rvb_id) {
    // There are no RVBs in that range
    return 0;
  }
  return getNextRequiredRvbGroupid(from_rvb_id, to_rvb_id);
}

RVBGroupId RVBManager::getNextRequiredRvbGroupid(RVBId from_rvb_id, RVBId to_rvb_id) const {
  if (from_rvb_id > to_rvb_id) return 0;
  const auto rvb_group_ids = in_mem_rvt_->getRvbGroupIds(from_rvb_id, to_rvb_id);
  for (const auto& id : rvb_group_ids) {
    if (std::find(stored_rvb_digests_group_ids_.begin(), stored_rvb_digests_group_ids_.end(), id) ==
        stored_rvb_digests_group_ids_.end()) {
      return id;
    }
  }
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
  BlockId start_scan_block_id =
      pruned_blocks_digests_.empty() ? as_->getGenesisBlockNum() : pruned_blocks_digests_.back().first;
  uint64_t start_rvb_id = computeNextRvbBlockId(start_scan_block_id);
  if (lastAgreedPrunableBlockId <= start_rvb_id) {
    LOG_ERROR(logger_, "Inconsistent prune report ignored:" << KVLOG(lastAgreedPrunableBlockId, start_rvb_id));
    return;
  }
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
    LOG_INFO(logger_,
             num_digests_added << " digests saved:"
                               << KVLOG(start_rvb_id,
                                        start_scan_block_id,
                                        current_rvb_id,
                                        lastAgreedPrunableBlockId,
                                        pruned_blocks_digests_.size()));
  }
}

void RVBManager::reset() {
  LOG_TRACE(logger_, "");
  stored_rvb_digests_.clear();
  stored_rvb_digests_group_ids_.clear();
  last_checkpoint_desc_.makeZero();
}

}  // namespace bftEngine::bcst::impl
