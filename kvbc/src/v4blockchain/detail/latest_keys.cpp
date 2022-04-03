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
                       const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>>& categories)
    : native_client_{native_client}, category_mapping_(native_client, categories) {
  if (native_client_->createColumnFamilyIfNotExisting(v4blockchain::detail::LATEST_KEYS_CF,
                                                      concord::storage::rocksdb::getLexicographic64TsComparator())) {
    LOG_INFO(V4_BLOCK_LOG,
             "Created [" << v4blockchain::detail::LATEST_KEYS_CF << "] column family for the latest keys");
  }
}

void LatestKeys::addBlockKeys(const concord::kvbc::categorization::Updates& updates,
                              BlockId block_id,
                              storage::rocksdb::NativeWriteBatch& write_batch) {
  LOG_DEBUG(V4_BLOCK_LOG, "Adding keys of block [" << block_id << "] to the latest CF");
  auto block_key = v4blockchain::detail::Blockchain::generateKey(block_id);
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
  static thread_local std::vector<uint8_t> serialized_value;
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  // add keys
  Flags flags = {0};
  for (const auto& [k, v] : updates.kv) {
    serialized_value.clear();
    if (v.stale_on_update) {
      flags = STALE_ON_UPDATE;
    }
    auto sl_flags = concord::storage::rocksdb::detail::toSlice(flags);
    concord::kvbc::categorization::serialize(serialized_value, v);
    auto sl_value = concord::storage::rocksdb::detail::toSlice(serialized_value);

    write_batch.put(v4blockchain::detail::LATEST_KEYS_CF,
                    getSliceArray(prefix, k, block_version),
                    getSliceArray(sl_value, sl_flags));
  }
  for (const auto& k : updates.deletes) {
    write_batch.del(v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k, block_version));
  }
}

void LatestKeys::handleCategoryUpdates(const std::string& block_version,
                                       const std::string& category_id,
                                       const categorization::ImmutableInput& updates,
                                       concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  static thread_local std::vector<uint8_t> serialized_value;
  static thread_local std::string out_ts;
  static thread_local std::string get_key;
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  // add keys
  Flags flags = {0};
  auto sl_flags = concord::storage::rocksdb::detail::toSlice(flags);
  for (const auto& [k, v] : updates.kv) {
    get_key.clear();
    serialized_value.clear();
    get_key.append(prefix);
    get_key.append(k);
    // check if key exists - immutable does not allow to update
    auto opt_val = native_client_->get(v4blockchain::detail::LATEST_KEYS_CF, get_key, block_version, &out_ts);
    if (opt_val) {
      throw std::runtime_error("Trying to update immutable key: " + k);
    }
    concord::kvbc::categorization::serialize(serialized_value, v);
    auto sl_value = concord::storage::rocksdb::detail::toSlice(serialized_value);
    write_batch.put(v4blockchain::detail::LATEST_KEYS_CF,
                    getSliceArray(prefix, k, block_version),
                    getSliceArray(sl_value, sl_flags));
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

}  // namespace concord::kvbc::v4blockchain::detail
