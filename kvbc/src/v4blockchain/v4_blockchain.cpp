// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <variant>

#include "v4blockchain/v4_blockchain.h"
#include "v4blockchain/detail/detail.h"
#include "categorization/base_types.h"
#include "categorization/db_categories.h"
#include "kvbc_key_types.hpp"
#include "block_metadata.hpp"
#include "throughput.hpp"
#include "blockchain_misc.hpp"
#include "util/filesystem.hpp"

namespace concord::kvbc::v4blockchain {

using namespace concord::kvbc;

KeyValueBlockchain::KeyValueBlockchain(
    const std::shared_ptr<concord::storage::rocksdb::NativeClient> &native_client,
    bool link_st_chain,
    const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>> &category_types)
    : native_client_{native_client},
      block_chain_{native_client_},
      state_transfer_chain_{native_client_},
      latest_keys_{native_client_, category_types},
      v4_metrics_comp_{concordMetrics::Component("v4_blockchain", std::make_shared<concordMetrics::Aggregator>())},
      blocks_deleted_{v4_metrics_comp_.RegisterGauge("numOfBlocksDeleted", (block_chain_.getGenesisBlockId() - 1))} {
  if (native_client_->createColumnFamilyIfNotExisting(v4blockchain::detail::MISC_CF)) {
    LOG_INFO(V4_BLOCK_LOG, "Created [" << v4blockchain::detail::MISC_CF << "] column family");
  }
  if (!link_st_chain) return;
  // Mark version of blockchain
  native_client_->put(v4blockchain::detail::MISC_CF, kvbc::keyTypes::blockchain_version, kvbc::V4Version());
  v4_metrics_comp_.Register();
  if (state_transfer_chain_.getLastBlockId() == 0) return;
  // Make sure that if linkSTChainFrom() has been interrupted (e.g. a crash or an abnormal shutdown), all DBAdapter
  // methods will return the correct values. For example, if state transfer had completed and linkSTChainFrom() was
  // interrupted, getLatestBlockId() should be equal to getLastReachableBlockId() on the next startup. Another example
  // is getValue() that returns keys from the blockchain only and ignores keys in the temporary state
  // transfer chain.
  LOG_INFO(V4_BLOCK_LOG, "Try to link ST temporary chain, this might take some time...");
  auto old_last_reachable_block_id = getLastReachableBlockId();
  linkSTChain();
  auto new_last_reachable_block_id = getLastReachableBlockId();
  if (new_last_reachable_block_id > old_last_reachable_block_id) {
    LOG_INFO(V4_BLOCK_LOG,
             "Done linking ST temporary chain:" << KVLOG(old_last_reachable_block_id, new_last_reachable_block_id));
  }
}

//////////////////////////// ADDER////////////////////////////////////////////
/*
  1 - Async check if snapshot is needed
  2 - add the block to the blocks CF
  3 - add the keys to the latest CF
  4 - atomic write to storage
  5 - increment last reachable block.
*/
BlockId KeyValueBlockchain::add(categorization::Updates &&updates) {
  auto scoped = v4blockchain::detail::ScopedDuration{"Add block"};
  // Should be performed before we add the block with the current Updates.
  auto future_seq_num_ = thread_pool_.async(
      [this](const categorization::Updates &updates) { return onNewBFTSequenceNumber(updates); }, updates);
  addGenesisBlockKey(updates);
  v4blockchain::detail::Block block;
  block.addUpdates(updates);
  auto block_size = block.size();
  auto write_batch = native_client_->getBatch(block_size * updates_to_final_size_ration_);
  auto block_id = add(updates, block, write_batch);
  LOG_DEBUG(V4_BLOCK_LOG,
            "Block size is " << block_size << " reserving batch to be " << updates_to_final_size_ration_ * block_size
                             << " size of final block is " << write_batch.size());
  auto sequence_number = future_seq_num_.get();
  native_client_->write(std::move(write_batch));
  block_chain_.setBlockId(block_id);
  if (sequence_number > 0) setLastBlockSequenceNumber(sequence_number);
  return block_id;
}

BlockId KeyValueBlockchain::add(const categorization::Updates &updates,
                                v4blockchain::detail::Block &block,
                                storage::rocksdb::NativeWriteBatch &write_batch) {
  BlockId block_id{};
  { block_id = block_chain_.addBlock(block, write_batch); }
  { latest_keys_.addBlockKeys(updates, block_id, write_batch); }
  return block_id;
}

//////////////////////////// DELETER////////////////////////////////////////////

BlockId KeyValueBlockchain::deleteBlocksUntil(BlockId until) {
  auto scoped = v4blockchain::detail::ScopedDuration{"deleteBlocksUntil"};
  auto id = block_chain_.deleteBlocksUntil(until);
  blocks_deleted_.Get().Set(id);
  v4_metrics_comp_.UpdateAggregator();
  return id;
}

void KeyValueBlockchain::deleteGenesisBlock() {
  auto scoped = v4blockchain::detail::ScopedDuration{"deleteGenesisBlock"};
  block_chain_.deleteGenesisBlock();
  blocks_deleted_++;
  v4_metrics_comp_.UpdateAggregator();
}

void KeyValueBlockchain::deleteLastReachableBlock() {
  auto last_reachable_id = block_chain_.getLastReachable();
  auto genesis_id = block_chain_.getGenesisBlockId();
  if (last_reachable_id == detail::Blockchain::INVALID_BLOCK_ID) {
    throw std::logic_error{"Blockchain empty, cannot delete last reachable block"};
  } else if (last_reachable_id == genesis_id) {
    throw std::logic_error{"Cannot delete only block as a last reachable one"};
  }
  auto recoverdb =
      KeyValueBlockchain::BlockchainRecovery::getRecoveryDB(native_client_->path(), chkpnt_block_id_.has_value());
  auto key = concord::kvbc::v4blockchain::detail::Blockchain::generateKey(last_reachable_id);
  auto batch_data = recoverdb->get(key);
  if (!batch_data.has_value()) {
    LOG_FATAL(V4_BLOCK_LOG,
              "No recovery data found in recovery db " << recoverdb->path() << " for block " << last_reachable_id
                                                       << ", did you call storeLastReachableRevertBatch?");
    ConcordAssert(false);
  }
  auto write_batch = native_client_->getBatch(std::move(*batch_data));
  native_client_->write(std::move(write_batch));
  block_chain_.setBlockId(--last_reachable_id);
  LOG_INFO(V4_BLOCK_LOG,
           "Wrote revert updates of block " << last_reachable_id + 1 << " last reachable is " << last_reachable_id);
}

/*
We delete last blocks that are defined as unstable.Two scenarios can trigger the deletion:
1 - when crashing during execution of requests of a BFT sequence number, we trim the blocks that were added as part
ofthat sn. the execution BFT sn is defined as not stable and the previous sn is defined as stable.
2 - when finishing creating a DB checkpoint we trim the blocks that were added during it's creation,
those blocks are defined as non stable and the block that was the last block prioir starting the checkpoint is defined
as stable. the set of updates needed for reverting a block is saved on a recovery DB, and is used when the delete
block is called.
*/
void KeyValueBlockchain::storeLastReachableRevertBatch(const std::optional<kvbc::BlockId> &block_id_at_chkpnt) {
  std::shared_ptr<storage::rocksdb::NativeClient> recoverdb;
  uint64_t unstable_version{};
  auto last_reachable_id = block_chain_.getLastReachable();
  auto genesis_id = block_chain_.getGenesisBlockId();
  if (genesis_id == last_reachable_id) {
    LOG_INFO(V4_BLOCK_LOG,
             "can't revert single block,  genesis_id " << genesis_id << " last_reachable_id " << last_reachable_id);
    return;
  }
  auto sh_key = RecoverySnapshot::getStorableKey(block_id_at_chkpnt);
  auto opt_seq_num = native_client_->get(v4blockchain::detail::MISC_CF, sh_key);
  if (!opt_seq_num.has_value()) {
    LOG_INFO(V4_BLOCK_LOG, "Snapshot data is nullopt, first start?");
    return;
  }
  auto updates = block_chain_.getBlockUpdates(last_reachable_id);
  ConcordAssert(updates.has_value());
  // if we're on recovery then we look at bft sequnce numbers, on checkpoint we look at blocks
  unstable_version = block_id_at_chkpnt.has_value() ? last_reachable_id : getBlockSequenceNumber(*updates);

  auto internal_snapshot = RecoverySnapshot::getV4SnapShotFromSeqnum(*opt_seq_num);
  auto stable_version = RecoverySnapshot::getStableVersion(*opt_seq_num);
  /*
  iterate over the range, for each block X get the set of updates needed to revert to the state X-1,
  and insert it to the recovery db.
  */
  LOG_INFO(V4_BLOCK_LOG, "reverting from " << unstable_version << " to " << stable_version);
  while (unstable_version > stable_version) {
    if (!recoverdb) {
      recoverdb =
          KeyValueBlockchain::BlockchainRecovery::getRecoveryDB(native_client_->path(), block_id_at_chkpnt.has_value());
    }
    auto key = concord::kvbc::v4blockchain::detail::Blockchain::generateKey(last_reachable_id);
    auto opt_val = recoverdb->get(key);
    if (!opt_val.has_value()) {
      LOG_INFO(V4_BLOCK_LOG,
               "Reverting updates of block " << last_reachable_id << " unstable version " << unstable_version
                                             << " stable version " << stable_version);
      auto write_batch = native_client_->getBatch();
      latest_keys_.revertLastBlockKeys(*updates, last_reachable_id, write_batch, internal_snapshot.get());
      // delete from blockchain
      block_chain_.deleteBlock(last_reachable_id, write_batch);
      auto ser_batch = write_batch.data();
      recoverdb->put(key, ser_batch);
    } else {
      LOG_INFO(V4_BLOCK_LOG, "Recovery DB alredy has block " << last_reachable_id << " updates");
    }

    --last_reachable_id;
    if (last_reachable_id == genesis_id) {
      break;
    }
    updates = block_chain_.getBlockUpdates(last_reachable_id);
    unstable_version = block_id_at_chkpnt.has_value() ? last_reachable_id : getBlockSequenceNumber(*updates);
  }
}

/*
  If the bft sequence number for this updates is bigger than the last, it means that the last sequence number
  was commited
*/
uint64_t KeyValueBlockchain::onNewBFTSequenceNumber(const categorization::Updates &updates) {
  auto sequence_number = getBlockSequenceNumber(updates);
  if (sequence_number == 0) {
    LOG_INFO(V4_BLOCK_LOG, "couldn't find BFT sequence number in updates");
    return 0;
  }
  // same sn
  if (last_block_sn_ && *last_block_sn_ == sequence_number) return sequence_number;
  // This is abnormal
  if (last_block_sn_ && sequence_number < *last_block_sn_) {
    LOG_WARN(V4_BLOCK_LOG,
             "Sequence number " << sequence_number << " is lower than previous value " << *last_block_sn_);
    return 0;
  }
  auto scoped = v4blockchain::detail::ScopedDuration{"onNewBFTSequenceNumber take snap"};
  if (!last_block_sn_) {
    // check if we have snapshot
    auto opt_seq_num = native_client_->get(v4blockchain::detail::MISC_CF, kvbc::keyTypes::v4_snapshot_sequence);
    if (opt_seq_num) {
      // we have, it means that we are adding blocks after recovery.
      return sequence_number;
    }
    // else - first start, take snapshot.
  }
  /*
  New BFT sequקnce number:
  important note : at this place state of latest and blocks is stable as the blocks state is synced i.e. last
  reachable is from the prev sn therefore crashing here will not call deleteLastReachable.

  flow -
  1 - take db snapshot T, the state is stable and represents the state of the previous BFT SN.
  2 - read previous snapshote from db T'.
  3 - release T'
  4 - store T in v4_snapshot_sequence alnog with the BFT SN.
  */
  auto bft_stable_sn = last_block_sn_.has_value() ? *last_block_sn_ : 0;
  auto new_snap_shot = RecoverySnapshot{&native_client_->rawDB()};
  auto old_rock_sn = 0;
  if (snap_shot_) {
    old_rock_sn = snap_shot_->GetSequenceNumber();
    new_snap_shot.releasePreviousSnapshot(snap_shot_);
  }

  native_client_->put(v4blockchain::detail::MISC_CF,
                      kvbc::keyTypes::v4_snapshot_sequence,
                      new_snap_shot.getStorableSeqNumAndPreventRelease(bft_stable_sn));
  snap_shot_ = new_snap_shot.get();
  LOG_DEBUG(V4_BLOCK_LOG,
            "New sequence number identified "
                << sequence_number << " bft stable sn is " << bft_stable_sn << " snap shot taken with rocksdb seq num "
                << new_snap_shot.get()->GetSequenceNumber() << " released prev snap shot with rocks db sn "
                << old_rock_sn);

  return sequence_number;
}

uint64_t KeyValueBlockchain::getBlockSequenceNumber(const categorization::Updates &updates) const {
  static std::string key = std::string(1, IBlockMetadata::kBlockMetadataKey);
  const auto &input = updates.categoryUpdates();
  if (input.kv.count(categorization::kConcordInternalCategoryId) == 0) {
    return 0;
  }
  const auto &ver_input =
      std::get<categorization::VersionedInput>(input.kv.at(categorization::kConcordInternalCategoryId));
  if (ver_input.kv.count(key) == 0) {
    return 0;
  }
  return BlockMetadata::getSequenceNum(ver_input.kv.at(key).data);
}

void KeyValueBlockchain::addGenesisBlockKey(categorization::Updates &updates) const {
  const auto stale_on_update = true;
  updates.addCategoryIfNotExisting<categorization::VersionedInput>(categorization::kConcordInternalCategoryId);
  updates.appendKeyValue<categorization::VersionedUpdates>(
      categorization::kConcordInternalCategoryId,
      std::string{keyTypes::genesis_block_key},
      categorization::VersionedUpdates::ValueType{
          concordUtils::toBigEndianStringBuffer(block_chain_.getGenesisBlockId()), stale_on_update});
}

///////////// STATE TRANSFER////////////////////////
bool KeyValueBlockchain::hasBlock(BlockId block_id) const {
  const auto last_reachable_block = block_chain_.getLastReachable();
  if (block_id > last_reachable_block) {
    return state_transfer_chain_.hasBlock(block_id);
  }
  return block_chain_.hasBlock(block_id);
}

std::optional<std::string> KeyValueBlockchain::getBlockData(const BlockId &block_id) const {
  const auto last_reachable_block = getLastReachableBlockId();
  // Try to take it from the ST chain
  if (block_id > last_reachable_block) {
    return state_transfer_chain_.getBlockData(block_id);
  }
  // Try from the blockchain itself
  return block_chain_.getBlockData(block_id);
}

std::optional<BlockId> KeyValueBlockchain::getLastStatetransferBlockId() const {
  if (state_transfer_chain_.getLastBlockId() == 0) return std::nullopt;
  return state_transfer_chain_.getLastBlockId();
}

concord::util::digest::BlockDigest KeyValueBlockchain::parentDigest(BlockId block_id) const {
  const auto last_reachable_block = getLastReachableBlockId();
  if (block_id > last_reachable_block) {
    return state_transfer_chain_.getBlockParentDigest(block_id);
  }
  if (block_id < getGenesisBlockId()) {
    LOG_ERROR(V4_BLOCK_LOG,
              "Trying to get digest from block " << block_id << " while genesis is " << getGenesisBlockId());
    concord::util::digest::BlockDigest empty_digest;
    empty_digest.fill(0);
    return empty_digest;
  }
  return block_chain_.getBlockParentDigest(block_id);
}

void KeyValueBlockchain::addBlockToSTChain(const BlockId &block_id,
                                           const char *block,
                                           const uint32_t block_size,
                                           bool last_block) {
  const auto last_reachable_block = getLastReachableBlockId();
  if (block_id <= last_reachable_block) {
    const auto msg = "Cannot add an existing block ID " + std::to_string(block_id);
    throw std::invalid_argument{msg};
  }

  if (state_transfer_chain_.hasBlock(block_id)) {
    auto existing_block = state_transfer_chain_.getBlockData(block_id);
    ConcordAssert(existing_block.has_value());
    auto view = std::string_view{block, block_size};
    if (view != *existing_block) {
      LOG_ERROR(V4_BLOCK_LOG,
                "found existing (and different) block ID[" << block_id << "] when receiving from state transfer");

      // E.L I think it's dangerous to delete the block and there is no value in doing so
      // kvbc_->deleteBlock(blockId);
      throw std::runtime_error(
          __PRETTY_FUNCTION__ +
          std::string("found existing (and different) block when receiving state transfer, block ID: ") +
          std::to_string(block_id));
    }
    return;
  }
  LOG_DEBUG(V4_BLOCK_LOG, "Adding ST block " << block_id);
  state_transfer_chain_.addBlock(block_id, block, block_size);
  if (last_block) {
    try {
      LOG_INFO(V4_BLOCK_LOG, "ST last block added " << block_id << " linking to blockchain");
      linkSTChain();
    } catch (const std::exception &e) {
      LOG_FATAL(V4_BLOCK_LOG,
                "Aborting due to failure to link chains after block has been added, reason: " << e.what());
      std::terminate();
    } catch (...) {
      LOG_FATAL(V4_BLOCK_LOG, "Aborting due to failure to link chains after block has been added");
      std::terminate();
    }
  }
}

void KeyValueBlockchain::linkSTChain() {
  BlockId block_id = getLastReachableBlockId() + 1;
  const auto last_block_id = state_transfer_chain_.getLastBlockId();
  if (last_block_id == 0) return;

  for (auto i = block_id; i <= last_block_id; ++i) {
    auto block = state_transfer_chain_.getBlock(i);
    if (!block) {
      LOG_INFO(V4_BLOCK_LOG, "Block " << i << " wasn't found, started from block " << block_id);
      return;
    }
    auto updates = block->getUpdates();
    writeSTLinkTransaction(i, updates);
  }
  // Linking has fully completed and we should not have any more ST temporary blocks left. Therefore, make sure we
  // don't have any value for the latest ST temporary block ID cache.
  LOG_INFO(V4_BLOCK_LOG, "Fully Linked ST from " << block_id << " to " << last_block_id);
  state_transfer_chain_.resetChain();
}

void KeyValueBlockchain::pruneOnSTLink(const categorization::Updates &updates) {
  auto cat_it = updates.categoryUpdates().kv.find(categorization::kConcordInternalCategoryId);
  if (cat_it == updates.categoryUpdates().kv.cend()) {
    return;
  }
  const auto &internal_kvs = std::get<categorization::VersionedInput>(cat_it->second).kv;
  auto key_it = internal_kvs.find(keyTypes::genesis_block_key);
  if (key_it != internal_kvs.cend()) {
    const auto block_genesis_id = concordUtils::fromBigEndianBuffer<BlockId>(key_it->second.data.data());
    while (getGenesisBlockId() >= INITIAL_GENESIS_BLOCK_ID && getGenesisBlockId() < getLastReachableBlockId() &&
           block_genesis_id > getGenesisBlockId()) {
      deleteGenesisBlock();
    }
  }
}

// Atomic delete from state transfer and add to blockchain
void KeyValueBlockchain::writeSTLinkTransaction(const BlockId block_id, const categorization::Updates &updates) {
  auto sequence_number = onNewBFTSequenceNumber(updates);
  v4blockchain::detail::Block block;
  block.addUpdates(updates);
  auto block_size = block.size();
  auto write_batch = native_client_->getBatch(block_size * updates_to_final_size_ration_);
  state_transfer_chain_.deleteBlock(block_id, write_batch);
  auto new_block_id = add(updates, block, write_batch);
  native_client_->write(std::move(write_batch));
  block_chain_.setBlockId(new_block_id);
  pruneOnSTLink(updates);
  if (sequence_number > 0) setLastBlockSequenceNumber(sequence_number);
}

size_t KeyValueBlockchain::linkUntilBlockId(BlockId until_block_id) {
  const auto from_block_id = getLastReachableBlockId() + 1;
  ConcordAssertLE(from_block_id, until_block_id);

  static constexpr uint64_t report_thresh{1000};
  static uint64_t report_counter{};

  concord::util::DurationTracker<std::chrono::milliseconds> link_duration("link_duration", true);
  BlockId last_added = 0;
  for (auto i = from_block_id; i <= until_block_id; ++i) {
    auto block = state_transfer_chain_.getBlock(i);

    if (!block) {
      break;
    }
    last_added = i;
    // First prune and then link the block to the chain. Rationale is that this will preserve the same order of block
    // deletes relative to block adds on source and destination replicas.
    auto updates = block->getUpdates();
    writeSTLinkTransaction(i, updates);
    if ((++report_counter % report_thresh) == 0) {
      auto elapsed_time_ms = link_duration.totalDuration();
      uint64_t blocks_linked_per_sec{};
      uint64_t blocks_left_to_link{};
      uint64_t estimated_time_left_sec{};
      if (elapsed_time_ms > 0) {
        blocks_linked_per_sec = (((i - from_block_id + 1) * 1000) / (elapsed_time_ms));
        blocks_left_to_link = until_block_id - i;
        estimated_time_left_sec = blocks_left_to_link / blocks_linked_per_sec;
      }
      LOG_INFO(CAT_BLOCK_LOG,
               "Last block ID connected: " << i << ","
                                           << KVLOG(from_block_id,
                                                    until_block_id,
                                                    elapsed_time_ms,
                                                    blocks_linked_per_sec,
                                                    blocks_left_to_link,
                                                    estimated_time_left_sec));
    }
  }

  LOG_INFO(V4_BLOCK_LOG, "Linked st range from " << from_block_id << " until " << last_added);
  return (last_added - from_block_id) + 1;
}

std::optional<categorization::Value> KeyValueBlockchain::getValueFromUpdate(
    BlockId block_id, const std::string &key, const categorization::BlockMerkleInput &category_input) const {
  const auto valit = category_input.kv.find(key);
  if (valit == category_input.kv.cend()) {
    return std::nullopt;
  }
  return categorization::MerkleValue{{block_id, valit->second}};
}
std::optional<categorization::Value> KeyValueBlockchain::getValueFromUpdate(
    BlockId block_id, const std::string &key, const categorization::VersionedInput &category_input) const {
  const auto valit = category_input.kv.find(key);
  if (valit == category_input.kv.cend()) {
    return std::nullopt;
  }
  return categorization::VersionedValue{{block_id, (valit->second).data}};
}
std::optional<categorization::Value> KeyValueBlockchain::getValueFromUpdate(
    BlockId block_id, const std::string &key, const categorization::ImmutableInput &category_input) const {
  const auto valit = category_input.kv.find(key);
  if (valit == category_input.kv.cend()) {
    return std::nullopt;
  }
  return categorization::ImmutableValue{{block_id, (valit->second).data}};
}

std::optional<categorization::Value> KeyValueBlockchain::get(const std::string &category_id,
                                                             const std::string &key,
                                                             BlockId block_id) const {
  LOG_INFO(V4_BLOCK_LOG,
           "Explicit get on key " << std::hash<std::string>{}(key) << " from version " << block_id << " category_id "
                                  << category_id << " key is hex " << concordUtils::bufferToHex(key.data(), key.size())
                                  << " raw key " << key);
  auto updates_in_block = block_chain_.getBlockUpdates(block_id);
  if (!updates_in_block) {
    return std::nullopt;
  }
  const auto &kv_updates = updates_in_block->categoryUpdates(category_id);
  if (!kv_updates) {
    return std::nullopt;
  }
  std::optional<categorization::Value> ret;
  std::visit(
      [this, block_id, &key, &ret](const auto &specific_cat_updates) {
        ret = this->getValueFromUpdate(block_id, key, specific_cat_updates);
      },
      (*kv_updates).get());
  return ret;
}

std::optional<categorization::Value> KeyValueBlockchain::getLatest(const std::string &category_id,
                                                                   const std::string &key) const {
  return latest_keys_.getValue(category_id, key);
}

void KeyValueBlockchain::multiGet(const std::string &category_id,
                                  const std::vector<std::string> &keys,
                                  const std::vector<BlockId> &versions,
                                  std::vector<std::optional<categorization::Value>> &values) const {
  ConcordAssertEQ(keys.size(), versions.size());
  ConcordAssertEQ(keys.size(), versions.size());
  values.clear();
  values.reserve(keys.size());
  std::unordered_map<BlockId, std::optional<categorization::Updates>> unique_block_updates;
  block_chain_.multiGetBlockUpdates(versions, unique_block_updates);
  for (size_t i = 0; i < keys.size(); ++i) {
    const auto block_id = versions[i];
    const auto &key = keys[i];
    auto updates_in_block_it = unique_block_updates.find(block_id);
    bool value_added_is_good = false;
    if (updates_in_block_it != unique_block_updates.end()) {
      if (updates_in_block_it->second) {
        const auto &kv_updates = updates_in_block_it->second->categoryUpdates(category_id);
        if (kv_updates) {
          std::optional<categorization::Value> val;
          std::visit(
              [this, block_id, &key, &val](const auto &specific_cat_updates) {
                val = getValueFromUpdate(block_id, key, specific_cat_updates);
              },
              (*kv_updates).get());
          values.push_back(val);
          value_added_is_good = true;
        }
      }
    }
    if (!value_added_is_good) {
      values.push_back(std::nullopt);
    }
  }
}

void KeyValueBlockchain::multiGetLatest(const std::string &category_id,
                                        const std::vector<std::string> &keys,
                                        std::vector<std::optional<categorization::Value>> &values) const {
  return latest_keys_.multiGetValue(category_id, keys, values);
}

std::optional<categorization::TaggedVersion> KeyValueBlockchain::getLatestVersion(const std::string &category_id,
                                                                                  const std::string &key) const {
  return latest_keys_.getLatestVersion(category_id, key);
}

void KeyValueBlockchain::multiGetLatestVersion(
    const std::string &category_id,
    const std::vector<std::string> &keys,
    std::vector<std::optional<categorization::TaggedVersion>> &versions) const {
  return latest_keys_.multiGetLatestVersion(category_id, keys, versions);
}

void KeyValueBlockchain::trimBlocksFromSnapshot(BlockId block_id_at_checkpoint) {
  ConcordAssertNE(block_id_at_checkpoint, detail::Blockchain::INVALID_BLOCK_ID);
  ConcordAssertLE(block_id_at_checkpoint, getLastReachableBlockId());
  chkpnt_block_id_ = block_id_at_checkpoint;
  while (block_id_at_checkpoint < getLastReachableBlockId()) {
    LOG_INFO(V4_BLOCK_LOG,
             "Deleting last reachable block = " << getLastReachableBlockId()
                                                << ", DB checkpoint = " << native_client_->path());
    deleteLastReachableBlock();
  }
  KeyValueBlockchain::BlockchainRecovery::removeRecoveryDB(native_client_->path(), true);
  native_client_->del(v4blockchain::detail::MISC_CF, RecoverySnapshot::getStorableKey(block_id_at_checkpoint));
}

void KeyValueBlockchain::onFinishDeleteLastReachable() {
  LOG_INFO(V4_BLOCK_LOG, "replica state is in synced, removing recovery db");
  KeyValueBlockchain::BlockchainRecovery::removeRecoveryDB(native_client_->path(), false);
}

void KeyValueBlockchain::checkpointInProcess(bool flag, kvbc::BlockId block_id_at_chkpnt) {
  static std::mutex map_mutex;
  // shouldn't have any contention or overhead
  const std::lock_guard<std::mutex> lock(map_mutex);
  if (flag) {
    auto last_reachable_id = block_chain_.getLastReachable();
    ConcordAssertEQ(last_reachable_id, block_id_at_chkpnt);
    LOG_INFO(V4_BLOCK_LOG, "Taking snapshot at checkpoint start with block id " << last_reachable_id);
    auto new_snap_shot = RecoverySnapshot{&native_client_->rawDB()};
    chkpnt_snap_shots_[last_reachable_id] = new_snap_shot.get();
    native_client_->put(v4blockchain::detail::MISC_CF,
                        RecoverySnapshot::getStorableKey(block_id_at_chkpnt),
                        new_snap_shot.getStorableSeqNumAndPreventRelease(last_reachable_id));
  } else {
    if (chkpnt_snap_shots_.count(block_id_at_chkpnt) > 0) {
      RecoverySnapshot::releasePreviousSnapshot(chkpnt_snap_shots_[block_id_at_chkpnt], &native_client_->rawDB());
      chkpnt_snap_shots_.erase(block_id_at_chkpnt);
      native_client_->del(v4blockchain::detail::MISC_CF, RecoverySnapshot::getStorableKey(block_id_at_chkpnt));
      LOG_INFO(V4_BLOCK_LOG, "Released snapshot for checkpoint at block id " << block_id_at_chkpnt);
    } else {
      LOG_WARN(V4_BLOCK_LOG, "Didn't find snapshot for checkpoint at block id " << block_id_at_chkpnt);
    }
  }
}

/*
Open the blockchain is readonly mode and a recovery data base.
the it calls the storeLastReachableRevertBatch that reads data from the RO blockchain
and add it to the recovery data.
*/
KeyValueBlockchain::BlockchainRecovery::BlockchainRecovery(const std::string &path,
                                                           const std::optional<kvbc::BlockId> &block_id_at_chkpnt) {
  auto ro = true;
  if (!fs::exists(path) || !fs::is_directory(path)) {
    LOG_INFO(V4_BLOCK_LOG, "RocksDB directory path doesn't exist at " << path << " skipping recovery");
    return;
  }
  std::shared_ptr<storage::IDBClient> db{std::make_shared<storage::rocksdb::Client>(path)};
  try {
    db->init(ro);
  } catch (std::runtime_error &ex) {
    LOG_INFO(V4_BLOCK_LOG,
             "Failed to open RocksDB, proabably first start and no recovery is needed, exception msg " << ex.what());
    return;
  }
  auto native = concord::storage::rocksdb::NativeClient::fromIDBClient(db);
  if (!native->hasColumnFamily(v4blockchain::detail::MISC_CF)) {
    LOG_DEBUG(V4_BLOCK_LOG, "V4 cf wasn't found, it's either categorized storage or first run");
    return;
  }
  const auto link_temp_st_chain = false;
  try {
    auto v4_kvbc = concord::kvbc::v4blockchain::KeyValueBlockchain{native, link_temp_st_chain};
    v4_kvbc.storeLastReachableRevertBatch(block_id_at_chkpnt);
    LOG_INFO(V4_BLOCK_LOG, "Finished preparing recovery data");
  } catch (const std::exception &e) {
    LOG_INFO(V4_BLOCK_LOG,
             "Recovery process aborted due to failure to open the blockchain in readonly mode, reason: " << e.what());
  }
}

std::string KeyValueBlockchain::BlockchainRecovery::getPath(const std::string &path, bool is_chkpnt) {
  const std::string execution_recovery = "/concord_recovery_execution";
  const std::string chkpnt_recovery = "/concord_recovery_checkpoint";
  auto rec_path = path;
  if (is_chkpnt) {
    rec_path += chkpnt_recovery;
  } else {
    rec_path += execution_recovery;
  }
  ConcordAssertNE(path, rec_path);
  return rec_path;
}

void KeyValueBlockchain::BlockchainRecovery::removeRecoveryDB(const std::string &path, bool is_chkpnt) {
  auto rec_path = getPath(path, is_chkpnt);
  LOG_INFO(V4_BLOCK_LOG, "Removing recoverdb at " << rec_path);
  fs::remove_all(rec_path);
}

std::shared_ptr<storage::rocksdb::NativeClient> KeyValueBlockchain::BlockchainRecovery::getRecoveryDB(
    const std::string &path, bool is_chkpnt) {
  auto rec_path = getPath(path, is_chkpnt);
  auto ro = false;
  fs::create_directory(rec_path);
  LOG_INFO(V4_BLOCK_LOG, "Openned recoverdb at " << rec_path);
  std::shared_ptr<storage::IDBClient> db{std::make_shared<storage::rocksdb::Client>(rec_path)};
  db->init(ro);
  return concord::storage::rocksdb::NativeClient::fromIDBClient(db);
}

std::map<std::string, std::vector<std::string>> KeyValueBlockchain::getBlockStaleKeys(BlockId block_id) const {
  auto updates = block_chain_.getBlockUpdates(block_id);
  if (!updates) {
    const auto msg = "Failed to get updates for block ID = " + std::to_string(block_id);
    throw std::runtime_error{msg};
  }

  std::map<std::string, std::vector<std::string>> stale_keys;
  for (auto &&[category_id, update_input] : updates->categoryUpdates().kv) {
    stale_keys[category_id] =
        std::visit([&block_id, category_id = category_id, this](
                       const auto &update_input) { return getStaleKeys(block_id, category_id, update_input); },
                   update_input);
  }
  return stale_keys;
}

std::vector<std::string> KeyValueBlockchain::getStaleKeys(BlockId block_id,
                                                          const std::string &category_id,
                                                          const categorization::BlockMerkleInput &updates_input) const {
  std::vector<std::string> stale_keys;
  for (const auto &[k, _] : updates_input.kv) {
    (void)_;
    auto opt_val = getLatestVersion(category_id, k);
    if (!opt_val.has_value() || opt_val->version > block_id) {
      stale_keys.push_back(k);
    }
  }

  for (const auto &k : updates_input.deletes) {
    stale_keys.push_back(k);
  }
  return stale_keys;
}

std::vector<std::string> KeyValueBlockchain::getStaleKeys(BlockId block_id,
                                                          const std::string &category_id,
                                                          const categorization::VersionedInput &updates_input) const {
  std::vector<std::string> stale_keys;
  for (const auto &[k, v] : updates_input.kv) {
    auto opt_val = getLatestVersion(category_id, k);
    if (!opt_val.has_value() || opt_val->version > block_id || v.stale_on_update) {
      stale_keys.push_back(k);
    }
  }

  for (const auto &k : updates_input.deletes) {
    stale_keys.push_back(k);
  }
  return stale_keys;
}

std::vector<std::string> KeyValueBlockchain::getStaleKeys(BlockId block_id,
                                                          const std::string &category_id,
                                                          const categorization::ImmutableInput &updates_input) const {
  std::vector<std::string> stale_keys;
  for (const auto &[k, _] : updates_input.kv) {
    (void)_;
    stale_keys.push_back(k);
  }
  return stale_keys;
}

}  // namespace concord::kvbc::v4blockchain
