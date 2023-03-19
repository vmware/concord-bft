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

#include "util/endianness.hpp"
#include "v4blockchain/detail/keys_history.h"
#include "v4blockchain/detail/column_families.h"
#include "v4blockchain/detail/blockchain.h"
#include "rocksdb/details.h"
#include "bftengine/ReplicaConfig.hpp"

using namespace concord::kvbc;

namespace concord::kvbc::v4blockchain::detail {

KeysHistory::KeysHistory(
    const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
    const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>& categories)
    : native_client_{native_client}, category_mapping_(native_client, categories) {
  if (!bftEngine::ReplicaConfig::instance().enableKeysHistoryCF) {
    return;
  }
  if (native_client_->createColumnFamilyIfNotExisting(v4blockchain::detail::KEYS_HISTORY_CF,
                                                      KHCompactionFilter::getFilter())) {
    LOG_INFO(V4_BLOCK_LOG, "Created [" << v4blockchain::detail::KEYS_HISTORY_CF << "] column family for keys history");
  }
}

void KeysHistory::addBlockKeys(const concord::kvbc::categorization::Updates& updates,
                               const BlockId block_id,
                               storage::rocksdb::NativeWriteBatch& write_batch) {
  if (!bftEngine::ReplicaConfig::instance().enableKeysHistoryCF) {
    return;
  }
  LOG_DEBUG(V4_BLOCK_LOG, "Adding keys of block [" << block_id << "] to the keys history CF");
  const std::string block_key = v4blockchain::detail::Blockchain::generateKey(block_id);
  ConcordAssertEQ(block_key.size(), sizeof(BlockId));

  for (const auto& [category_id, category_updates] : updates.categoryUpdates().kv) {
    std::visit([category_id = category_id, &write_batch, &block_key, this](
                   const auto& updates) { handleCategoryUpdates(block_key, category_id, updates, write_batch); },
               category_updates);
  }
}

void KeysHistory::handleCategoryUpdates(const std::string& block_version,
                                        const std::string& category_id,
                                        const concord::kvbc::categorization::BlockMerkleInput& updates,
                                        concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  handleUpdatesImp(block_version, category_id, updates.kv, write_batch);
  handleDeletesUpdatesImp(block_version, category_id, updates.deletes, write_batch);
}

void KeysHistory::handleCategoryUpdates(const std::string& block_version,
                                        const std::string& category_id,
                                        const concord::kvbc::categorization::VersionedInput& updates,
                                        concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  handleUpdatesImp(block_version, category_id, updates.kv, write_batch);
  handleDeletesUpdatesImp(block_version, category_id, updates.deletes, write_batch);
}

void KeysHistory::handleCategoryUpdates(const std::string& block_version,
                                        const std::string& category_id,
                                        const concord::kvbc::categorization::ImmutableInput& updates,
                                        concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  handleUpdatesImp(block_version, category_id, updates.kv, write_batch);
}

template <typename UPDATES>
void KeysHistory::handleUpdatesImp(const std::string& block_version,
                                   const std::string& category_id,
                                   const UPDATES& updates_kv,
                                   concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  const std::string empty_val = "";
  for (const auto& [k, _] : updates_kv) {
    (void)_;  // unused
    LOG_DEBUG(V4_BLOCK_LOG,
              "Update history of key " << std::hash<std::string>{}(k) << " at version "
                                       << concordUtils::fromBigEndianBuffer<BlockId>(block_version.data())
                                       << ". category_id " << category_id << " prefix " << prefix << " key is hex "
                                       << concordUtils::bufferToHex(k.data(), k.size()) << " key size " << k.size()
                                       << " raw key " << k);
    write_batch.put(
        v4blockchain::detail::KEYS_HISTORY_CF, getSliceArray(prefix, k, block_version), getSliceArray(empty_val));
  }
}

template <typename DELETES>
void KeysHistory::handleDeletesUpdatesImp(const std::string& block_version,
                                          const std::string& category_id,
                                          const DELETES& deletes,
                                          concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  const std::string empty_val = "";
  for (const auto& k : deletes) {
    LOG_DEBUG(V4_BLOCK_LOG,
              "Update history of key " << std::hash<std::string>{}(k) << " at version "
                                       << concordUtils::fromBigEndianBuffer<BlockId>(block_version.data())
                                       << ". category_id " << category_id << " prefix " << prefix << " key is hex "
                                       << concordUtils::bufferToHex(k.data(), k.size()) << " key size " << k.size()
                                       << " raw key " << k);
    write_batch.put(
        v4blockchain::detail::KEYS_HISTORY_CF, getSliceArray(prefix, k, block_version), getSliceArray(empty_val));
  }
}

void KeysHistory::revertLastBlockKeys(const concord::kvbc::categorization::Updates& updates,
                                      const BlockId block_id,
                                      storage::rocksdb::NativeWriteBatch& write_batch) {
  if (!bftEngine::ReplicaConfig::instance().enableKeysHistoryCF) {
    return;
  }
  ConcordAssertGT(block_id, 0);
  LOG_DEBUG(V4_BLOCK_LOG, "Reverting keys of block [" << block_id << "] ");
  const std::string block_key = v4blockchain::detail::Blockchain::generateKey(block_id);
  ConcordAssertEQ(block_key.size(), sizeof(BlockId));
  for (const auto& [category_id, category_updates] : updates.categoryUpdates().kv) {
    std::visit([category_id = category_id, &write_batch, &block_key, this](
                   const auto& updates) { revertCategoryKeys(category_id, updates, block_key, write_batch); },
               category_updates);
  }
}

void KeysHistory::revertCategoryKeys(const std::string& category_id,
                                     const categorization::BlockMerkleInput& updates,
                                     const std::string& block_version,
                                     concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  revertKeysImp(category_id, updates.kv, block_version, write_batch);
  revertDeletedKeysImp(category_id, updates.deletes, block_version, write_batch);
}

void KeysHistory::revertCategoryKeys(const std::string& category_id,
                                     const categorization::VersionedInput& updates,
                                     const std::string& block_version,
                                     concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  revertKeysImp(category_id, updates.kv, block_version, write_batch);
  revertDeletedKeysImp(category_id, updates.deletes, block_version, write_batch);
}

void KeysHistory::revertCategoryKeys(const std::string& category_id,
                                     const categorization::ImmutableInput& updates,
                                     const std::string& block_version,
                                     concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  revertKeysImp(category_id, updates.kv, block_version, write_batch);
}

template <typename UPDATES>
void KeysHistory::revertKeysImp(const std::string& category_id,
                                const UPDATES& updates_kv,
                                const std::string& block_version,
                                concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  for (const auto& [k, _] : updates_kv) {
    (void)_;
    revertOneKeyAtVersion(category_id, block_version, prefix, k, write_batch);
  }
}

template <typename DELETES>
void KeysHistory::revertDeletedKeysImp(const std::string& category_id,
                                       const DELETES& deletes,
                                       const std::string& block_version,
                                       concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  const std::string& prefix = category_mapping_.categoryPrefix(category_id);
  for (const auto& k : deletes) {
    revertOneKeyAtVersion(category_id, block_version, prefix, k, write_batch);
  }
}

void KeysHistory::revertOneKeyAtVersion(const std::string& category_id,
                                        const std::string& block_version,
                                        const std::string& prefix,
                                        const std::string& key,
                                        concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  std::string get_key = prefix;
  get_key.append(key);
  get_key.append(block_version);
  auto opt_val = native_client_->get(v4blockchain::detail::KEYS_HISTORY_CF, get_key);
  if (opt_val) {
    write_batch.del(v4blockchain::detail::KEYS_HISTORY_CF, get_key);
    LOG_DEBUG(V4_BLOCK_LOG,
              "Revert key " << key << " in version " << concordUtils::fromBigEndianBuffer<BlockId>(block_version.data())
                            << " category " << category_id << " prefix " << prefix);
  } else {
    LOG_WARN(V4_BLOCK_LOG,
             "Couldn't find key " << key << " in version "
                                  << concordUtils::fromBigEndianBuffer<BlockId>(block_version.data()) << " category "
                                  << category_id << " prefix " << prefix);
  }
}

std::optional<BlockId> KeysHistory::getVersionFromHistory(const std::string& category_id,
                                                          const std::string& key,
                                                          const BlockId version) const {
  if (!bftEngine::ReplicaConfig::instance().enableKeysHistoryCF) {
    return std::nullopt;
  }
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  const auto& version_key = v4blockchain::detail::Blockchain::generateKey(version);
  std::string get_key = prefix;
  get_key.append(key);
  get_key.append(version_key);

  auto itr = native_client_->getIterator(v4blockchain::detail::KEYS_HISTORY_CF);
  try {
    itr.seekAtMost(get_key);
  } catch (concord::storage::rocksdb::RocksDBException& ex) {
    LOG_DEBUG(V4_BLOCK_LOG, "iterator seekAtMost failed. " << ex.what());
    return std::nullopt;
  }

  if (!itr) {
    LOG_DEBUG(V4_BLOCK_LOG,
              "Reading key " << std::hash<std::string>{}(key) << " not found at version " << version << " category_id "
                             << category_id << " prefix " << prefix << " key is hex "
                             << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
    return std::nullopt;
  }
  auto found_key = itr.key();
  const size_t total_key_size = found_key.size();
  auto user_key_with_prefix = found_key.substr(0, total_key_size - sizeof(BlockId));

  if (user_key_with_prefix != prefix + key) {
    LOG_DEBUG(V4_BLOCK_LOG,
              "Reading key " << std::hash<std::string>{}(key) << " not found at version " << version << " category_id "
                             << category_id << " prefix " << prefix << " key is hex "
                             << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
    return std::nullopt;
  }

  auto actual_version =
      concordUtils::fromBigEndianBuffer<BlockId>(found_key.c_str() + (total_key_size - sizeof(BlockId)));
  LOG_DEBUG(V4_BLOCK_LOG,
            "Reading key " << std::hash<std::string>{}(key) << " snapshot " << version << " actual version "
                           << actual_version << " category_id " << category_id << " prefix " << prefix << " key is hex "
                           << concordUtils::bufferToHex(found_key.data(), found_key.size()) << " raw key " << key);
  return actual_version;
}

bool KeysHistory::KHCompactionFilter::Filter(int /*level*/,
                                             const ::rocksdb::Slice& key,
                                             const ::rocksdb::Slice& /*val*/,
                                             std::string* /*new_value*/,
                                             bool* /*value_changed*/) const {
  if (key.size() <= sizeof(BlockId)) return false;
  auto ts_slice = ::rocksdb::Slice(key.data() + key.size() - sizeof(BlockId), sizeof(BlockId));
  auto key_version = concordUtils::fromBigEndianBuffer<uint64_t>(ts_slice.data());
  auto max_blocks_to_save = bftEngine::ReplicaConfig::instance().keysHistoryMaxBlocksNum;

  if (max_blocks_to_save != 0) {
    if (Blockchain::global_latest_block_id < max_blocks_to_save ||
        key_version > Blockchain::global_latest_block_id - max_blocks_to_save)
      return false;
    LOG_DEBUG(V4_BLOCK_LOG,
              "Filtering key with version " << key_version << ". latest block is " << Blockchain::global_latest_block_id
                                            << " max blocks to save is " << max_blocks_to_save);
  } else {
    // when keysHistoryMaxBlocksNum is set to 0, prune the history that's before the genesis block
    if (key_version >= concord::kvbc::v4blockchain::detail::Blockchain::global_genesis_block_id) return false;
    LOG_DEBUG(V4_BLOCK_LOG,
              "Filtering key with version "
                  << key_version << " genesis is "
                  << concord::kvbc::v4blockchain::detail::Blockchain::global_genesis_block_id);
  }
  return true;
}

}  // namespace concord::kvbc::v4blockchain::detail
