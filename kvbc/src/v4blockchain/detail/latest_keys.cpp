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

#include "endianness.hpp"
#include "v4blockchain/detail/latest_keys.h"
#include "v4blockchain/detail/column_families.h"
#include "Logger.hpp"
#include "rocksdb/time_stamp_comparator.h"
#include "v4blockchain/detail/blockchain.h"
#include "rocksdb/details.h"

using namespace concord::kvbc;
namespace concord::kvbc::v4blockchain::detail {

template <typename... Sliceable>
auto getSliceArray(const Sliceable&... sls) {
  return std::array<::rocksdb::Slice, sizeof...(sls)>{sls...};
}

LatestKeys::LatestKeys(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
                       const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>>& categories,
                       std::function<BlockId()>&& f)
    : native_client_{native_client}, category_mapping_(native_client, categories), comp_filter_(std::move(f)) {
  if (native_client_->createColumnFamilyIfNotExisting(v4blockchain::detail::LATEST_KEYS_CF,
                                                      concord::storage::rocksdb::getLexicographic64TsComparator(),
                                                      getCompFilter())) {
    LOG_INFO(V4_BLOCK_LOG,
             "Created [" << v4blockchain::detail::LATEST_KEYS_CF << "] column family for the latest keys");
  }
}

void LatestKeys::addBlockKeys(const concord::kvbc::categorization::Updates& updates,
                              BlockId block_id,
                              storage::rocksdb::NativeWriteBatch& write_batch) {
  LOG_DEBUG(V4_BLOCK_LOG, "Adding keys of block [" << block_id << "] to the latest CF");
  auto block_key = v4blockchain::detail::Blockchain::generateKey(block_id);
  ConcordAssertEQ(block_key.size(), concord::storage::rocksdb::TIME_STAMP_SIZE);
  for (const auto& [category_id, updates] : updates.categoryUpdates().kv) {
    std::visit([category_id = category_id, &write_batch, &block_key, this](
                   const auto& updates) { handleCategoryUpdates(block_key, category_id, updates, write_batch); },
               updates);
  }
}

void LatestKeys::handleCategoryUpdates(const std::string& block_version,
                                       const std::string& category_id,
                                       const categorization::BlockMerkleInput& updates,
                                       concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  // add keys
  Flags flags = {0};
  auto sl_flags = concord::storage::rocksdb::detail::toSlice(flags);
  for (const auto& [k, v] : updates.kv) {
    write_batch.put(
        v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k, block_version), getSliceArray(v, sl_flags));
  }
  for (const auto& k : updates.deletes) {
    write_batch.del(v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k, block_version));
  }
}

void LatestKeys::handleCategoryUpdates(const std::string& block_version,
                                       const std::string& category_id,
                                       const categorization::VersionedInput& updates,
                                       concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  // add keys
  Flags flags = {0};
  for (const auto& [k, v] : updates.kv) {
    if (v.stale_on_update) {
      flags = STALE_ON_UPDATE;
    }
    auto sl_flags = concord::storage::rocksdb::detail::toSlice(flags);

    write_batch.put(
        v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k, block_version), getSliceArray(v.data, sl_flags));
  }
  for (const auto& k : updates.deletes) {
    write_batch.del(v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k, block_version));
  }
}

void LatestKeys::handleCategoryUpdates(const std::string& block_version,
                                       const std::string& category_id,
                                       const categorization::ImmutableInput& updates,
                                       concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  static thread_local std::string out_ts;
  static thread_local std::string get_key;
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  // add keys
  auto sl_flags = concord::storage::rocksdb::detail::toSlice(STALE_ON_UPDATE);
  for (const auto& [k, v] : updates.kv) {
    get_key.clear();
    get_key.append(prefix);
    get_key.append(k);
    // check if key exists - immutable does not allow to update
    auto opt_val = native_client_->get(v4blockchain::detail::LATEST_KEYS_CF, get_key, block_version, &out_ts);
    if (opt_val) {
      throw std::runtime_error("Trying to update immutable key: " + k);
    }
    write_batch.put(
        v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k, block_version), getSliceArray(v.data, sl_flags));
  }
}

/*
Iterate over updates, for each key:
1 - read its previous version by calling get with version id - 1.
2 - if the previous version does not exist, mark the key for delete.
3 - if the previous version exists, update the value with old value and id as version.
*/
void LatestKeys::revertLastBlockKeys(const concord::kvbc::categorization::Updates& updates,
                                     BlockId block_id,
                                     storage::rocksdb::NativeWriteBatch& write_batch) {
  ConcordAssertGT(block_id, 0);
  LOG_DEBUG(V4_BLOCK_LOG, "Reverting keys of block [" << block_id << "] ");
  auto block_key = v4blockchain::detail::Blockchain::generateKey(block_id);
  auto prev_block_key = v4blockchain::detail::Blockchain::generateKey(block_id - 1);
  ConcordAssertEQ(block_key.size(), concord::storage::rocksdb::TIME_STAMP_SIZE);
  for (const auto& [category_id, updates] : updates.categoryUpdates().kv) {
    std::visit(
        [category_id = category_id, &write_batch, &block_key, &prev_block_key, this](const auto& updates) {
          revertCategoryKeys(block_key, prev_block_key, category_id, updates, write_batch);
        },
        updates);
  }
}

void LatestKeys::revertCategoryKeys(const std::string& block_version,
                                    const std::string& prev_block_version,
                                    const std::string& category_id,
                                    const categorization::BlockMerkleInput& updates,
                                    concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  revertCategoryKeysImp(block_version, prev_block_version, category_id, updates.kv, write_batch);
  revertDeletedKeysImp(block_version, prev_block_version, category_id, updates.deletes, write_batch);
}

void LatestKeys::revertCategoryKeys(const std::string& block_version,
                                    const std::string& prev_block_version,
                                    const std::string& category_id,
                                    const categorization::VersionedInput& updates,
                                    concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  revertCategoryKeysImp(block_version, prev_block_version, category_id, updates.kv, write_batch);
  revertDeletedKeysImp(block_version, prev_block_version, category_id, updates.deletes, write_batch);
}

void LatestKeys::revertCategoryKeys(const std::string& block_version,
                                    const std::string& prev_block_version,
                                    const std::string& category_id,
                                    const categorization::ImmutableInput& updates,
                                    concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  revertCategoryKeysImp(block_version, prev_block_version, category_id, updates.kv, write_batch);
}

template <typename UPDATES>
void LatestKeys::revertCategoryKeysImp(const std::string& block_version,
                                       const std::string& prev_block_version,
                                       const std::string& category_id,
                                       const UPDATES& updates,
                                       concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  std::string get_key;
  std::string out_ts;
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  for (const auto& [k, _] : updates) {
    (void)_;  // unsued
    get_key.append(prefix);
    get_key.append(k);
    // check if key exists for the previous version
    auto opt_val = native_client_->get(v4blockchain::detail::LATEST_KEYS_CF, get_key, prev_block_version, &out_ts);
    // if no previous version, delete it.
    if (!opt_val) {
      write_batch.del(v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k, block_version));
      continue;
    }
    // add the previous value with the current block as its version.
    write_batch.put(
        v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k, block_version), getSliceArray(*opt_val));
  }
}

template <typename DELETES>
void LatestKeys::revertDeletedKeysImp(const std::string& block_version,
                                      const std::string& prev_block_version,
                                      const std::string& category_id,
                                      const DELETES& deletes,
                                      concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  static thread_local std::string get_key;
  static thread_local std::string out_ts;
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  for (const auto& k : deletes) {
    get_key.clear();
    get_key.append(prefix);
    get_key.append(k);
    // check if key exists for the previous version
    auto opt_val = native_client_->get(v4blockchain::detail::LATEST_KEYS_CF, get_key, prev_block_version, &out_ts);
    // A key was deleted in the last block, but we can't find the previous version for revert.
    // I think it's not a scenario for assertion or excepion as the input is from external source i.e. execution engine.
    if (!opt_val) {
      LOG_WARN(V4_BLOCK_LOG, "Couldn't find previous version for deleted key " << get_key);
      continue;
    }
    // add the previous value with the current block as its version.
    write_batch.put(
        v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k, block_version), getSliceArray(*opt_val));
  }
}

void LatestKeys::trimHistoryUntil(BlockId block_id) {
  auto block_key = v4blockchain::detail::Blockchain::generateKey(block_id);
  auto& raw_db = native_client_->rawDB();
  auto status = raw_db.IncreaseFullHistoryTsLow(
      native_client_->columnFamilyHandle(v4blockchain::detail::LATEST_KEYS_CF), block_key);
  if (!status.ok()) {
    LOG_ERROR(V4_BLOCK_LOG, "Failed trimming history due to " << status.ToString());
    throw std::runtime_error(status.ToString());
  }
}

std::optional<categorization::Value> LatestKeys::getValue(BlockId latest_block_id,
                                                          const std::string& category_id,
                                                          const std::string& latest_version,
                                                          const std::string& key) const {
  std::string get_key;
  std::string out_ts;
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  get_key.append(prefix);
  get_key.append(key);
  auto opt_val = native_client_->get(v4blockchain::detail::LATEST_KEYS_CF, get_key, latest_version, &out_ts);
  if (!opt_val) {
    return std::nullopt;
  }
  auto actual_version = concordUtils::fromBigEndianBuffer<BlockId>(out_ts.data());
  const size_t total_val_size = opt_val->size();

  switch (category_mapping_.categoryType(category_id)) {
    case concord::kvbc::categorization::CATEGORY_TYPE::block_merkle:
      return categorization::MerkleValue{{actual_version, opt_val->substr(0, total_val_size - FLAGS_SIZE)}};
    case concord::kvbc::categorization::CATEGORY_TYPE::immutable: {
      return categorization::ImmutableValue{{actual_version, opt_val->substr(0, total_val_size - FLAGS_SIZE)}};
    }
    case concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv: {
      return categorization::VersionedValue{{actual_version, opt_val->substr(0, total_val_size - FLAGS_SIZE)}};
    }
    default:
      ConcordAssert(false);
  }
}

void LatestKeys::multiGetValue(BlockId latest_block_id,
                               const std::string& category_id,
                               const std::string& latest_version,
                               const std::vector<std::string>& keys,
                               std::vector<std::optional<categorization::Value>>& values) const {
  values.clear();
  values.reserve(keys.size());
  for (const auto& key : keys) {
    values.push_back(getValue(latest_block_id, category_id, latest_version, key));
  }
}

std::optional<categorization::TaggedVersion> LatestKeys::getLatestVersion(const std::string& category_id,
                                                                          const std::string& latest_version,
                                                                          const std::string& key) const {
  std::string get_key;
  std::string out_ts;
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  get_key.append(prefix);
  get_key.append(key);
  auto opt_val = native_client_->get(v4blockchain::detail::LATEST_KEYS_CF, get_key, latest_version, &out_ts);
  if (!opt_val) {
    return std::nullopt;
  }
  BlockId version = concordUtils::fromBigEndianBuffer<BlockId>(out_ts.data());
  return categorization::TaggedVersion{false, version};
}

void LatestKeys::multiGetLatestVersion(const std::string& category_id,
                                       const std::string& latest_version,
                                       const std::vector<std::string>& keys,
                                       std::vector<std::optional<categorization::TaggedVersion>>& versions) const {
  versions.clear();
  versions.reserve(keys.size());
  for (const auto& key : keys) {
    versions.push_back(getLatestVersion(category_id, latest_version, key));
  }
}

bool LatestKeys::LKCompactionFilter::Filter(int /*level*/,
                                            const ::rocksdb::Slice& key,
                                            const ::rocksdb::Slice& val,
                                            std::string* /*new_value*/,
                                            bool* /*value_changed*/) const {
  if (!LatestKeys::isStaleOnUpdate(val)) return false;
  auto genesis = genesis_id();
  auto ts_slice = storage::rocksdb::ExtractTimestampFromUserKey(key, concord::storage::rocksdb::TIME_STAMP_SIZE);
  auto key_version = concordUtils::fromBigEndianBuffer<uint64_t>(ts_slice.data());
  if (key_version >= genesis) return false;
  LOG_INFO(V4_BLOCK_LOG, "Filtering key with version " << key_version << " genesis is " << genesis);
  return true;
}

}  // namespace concord::kvbc::v4blockchain::detail
