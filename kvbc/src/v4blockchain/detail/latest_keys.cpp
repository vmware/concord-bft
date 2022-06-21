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
                                                      LKCompactionFilter::getFilter())) {
    LOG_INFO(V4_BLOCK_LOG,
             "Created [" << v4blockchain::detail::LATEST_KEYS_CF << "] column family for the latest keys");
  }
  if (native_client_->createColumnFamilyIfNotExisting(v4blockchain::detail::IMMUTABLE_KEYS_CF,
                                                      LKCompactionFilter::getFilter())) {
    LOG_INFO(V4_BLOCK_LOG,
             "Created [" << v4blockchain::detail::IMMUTABLE_KEYS_CF << "] column family for the immutable keys");
  }
}

void LatestKeys::addBlockKeys(const concord::kvbc::categorization::Updates& updates,
                              BlockId block_id,
                              storage::rocksdb::NativeWriteBatch& write_batch) {
  LOG_DEBUG(V4_BLOCK_LOG, "Adding keys of block [" << block_id << "] to the latest CF");
  auto block_key = v4blockchain::detail::Blockchain::generateKey(block_id);
  ConcordAssertEQ(block_key.size(), VERSION_SIZE);
  for (const auto& [category_id, updates] : updates.categoryUpdates().kv) {
    std::visit([cat_id = category_id, &write_batch, &block_key, this](
                   const auto& updates) { handleCategoryUpdates(block_key, cat_id, updates, write_batch); },
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
    LOG_DEBUG(V4_BLOCK_LOG,
              "Adding key " << std::hash<std::string>{}(k) << " at version "
                            << concordUtils::fromBigEndianBuffer<BlockId>(block_version.data()) << " category_id "
                            << category_id << " prefix " << prefix << " key is hex "
                            << concordUtils::bufferToHex(k.data(), k.size()) << " key size " << k.size()
                            << " value size " << v.size() << " raw key " << k);
    write_batch.put(
        v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k), getSliceArray(v, sl_flags, block_version));
  }
  for (const auto& k : updates.deletes) {
    LOG_DEBUG(V4_BLOCK_LOG,
              "Deleting key " << std::hash<std::string>{}(k) << " at version "
                              << concordUtils::fromBigEndianBuffer<BlockId>(block_version.data()) << " category_id "
                              << category_id << " prefix " << prefix << " key is hex "
                              << concordUtils::bufferToHex(k.data(), k.size()) << " raw key " << k);
    write_batch.del(v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k));
  }
}

void LatestKeys::handleCategoryUpdates(const std::string& block_version,
                                       const std::string& category_id,
                                       const categorization::VersionedInput& updates,
                                       concord::storage::rocksdb::NativeWriteBatch& write_batch) {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  // add keys
  for (const auto& [k, v] : updates.kv) {
    Flags flags = {0};
    if (v.stale_on_update) {
      flags = STALE_ON_UPDATE;
    }
    LOG_DEBUG(V4_BLOCK_LOG,
              "Adding key " << std::hash<std::string>{}(k) << " at version "
                            << concordUtils::fromBigEndianBuffer<BlockId>(block_version.data()) << " category_id "
                            << category_id << " prefix " << prefix << " key is hex "
                            << concordUtils::bufferToHex(k.data(), k.size()) << " key size " << k.size()
                            << " value size " << v.data.size() << " raw key " << k);
    auto sl_flags = concord::storage::rocksdb::detail::toSlice(flags);

    write_batch.put(
        v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k), getSliceArray(v.data, sl_flags, block_version));
  }
  for (const auto& k : updates.deletes) {
    LOG_DEBUG(V4_BLOCK_LOG,
              "Deleting key " << std::hash<std::string>{}(k) << " at version "
                              << concordUtils::fromBigEndianBuffer<BlockId>(block_version.data()) << " category_id "
                              << category_id << " prefix " << prefix << " key is hex "
                              << concordUtils::bufferToHex(k.data(), k.size()) << " raw key " << k);
    write_batch.del(v4blockchain::detail::LATEST_KEYS_CF, getSliceArray(prefix, k));
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
    // E.L - this check does not exist in the categorized storage, therefore commenting it
    // check if key exists - immutable does not allow to update
    // auto opt_val = native_client_->get(v4blockchain::detail::LATEST_KEYS_CF, get_key, block_version, &out_ts);
    // if (opt_val) {
    //   throw std::runtime_error("Trying to update immutable key: " + k);
    // }
    LOG_DEBUG(V4_BLOCK_LOG,
              "Adding key " << std::hash<std::string>{}(k) << " at version "
                            << concordUtils::fromBigEndianBuffer<BlockId>(block_version.data()) << " category_id "
                            << category_id << " prefix " << prefix << " key is hex "
                            << concordUtils::bufferToHex(k.data(), k.size()) << " key size " << k.size()
                            << " value size " << v.data.size() << " raw key " << k);
    write_batch.put(v4blockchain::detail::IMMUTABLE_KEYS_CF,
                    getSliceArray(prefix, k),
                    getSliceArray(v.data, sl_flags, block_version));
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
                                     storage::rocksdb::NativeWriteBatch& write_batch,
                                     ::rocksdb::Snapshot* snpsht) {
  ConcordAssertGT(block_id, 0);
  LOG_DEBUG(V4_BLOCK_LOG, "Reverting keys of block [" << block_id << "] ");
  for (const auto& [category_id, updates] : updates.categoryUpdates().kv) {
    std::visit([category_id = category_id, &write_batch, &snpsht, this](
                   const auto& updates) { revertCategoryKeys(category_id, updates, write_batch, snpsht); },
               updates);
  }
}

void LatestKeys::revertCategoryKeys(const std::string& category_id,
                                    const categorization::BlockMerkleInput& updates,
                                    concord::storage::rocksdb::NativeWriteBatch& write_batch,
                                    ::rocksdb::Snapshot* snpsht) {
  revertCategoryKeysImp(v4blockchain::detail::LATEST_KEYS_CF, category_id, updates.kv, write_batch, snpsht);
  revertDeletedKeysImp(category_id, updates.deletes, write_batch, snpsht);
}

void LatestKeys::revertCategoryKeys(const std::string& category_id,
                                    const categorization::VersionedInput& updates,
                                    concord::storage::rocksdb::NativeWriteBatch& write_batch,
                                    ::rocksdb::Snapshot* snpsht) {
  revertCategoryKeysImp(v4blockchain::detail::LATEST_KEYS_CF, category_id, updates.kv, write_batch, snpsht);
  revertDeletedKeysImp(category_id, updates.deletes, write_batch, snpsht);
}

void LatestKeys::revertCategoryKeys(const std::string& category_id,
                                    const categorization::ImmutableInput& updates,
                                    concord::storage::rocksdb::NativeWriteBatch& write_batch,
                                    ::rocksdb::Snapshot* snpsht) {
  revertCategoryKeysImp(v4blockchain::detail::IMMUTABLE_KEYS_CF, category_id, updates.kv, write_batch, snpsht);
}

template <typename UPDATES>
void LatestKeys::revertCategoryKeysImp(const std::string& cFamily,
                                       const std::string& category_id,
                                       const UPDATES& updates,
                                       concord::storage::rocksdb::NativeWriteBatch& write_batch,
                                       ::rocksdb::Snapshot* snpsht) {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  auto sn = snpsht->GetSequenceNumber();
  ::rocksdb::ReadOptions ro;
  ro.snapshot = snpsht;

  std::vector<std::string> keys;
  std::vector<::rocksdb::PinnableSlice> values;
  std::vector<::rocksdb::Status> statuses;

  for (const auto& [k, _] : updates) {
    (void)_;  // unsued
    keys.push_back(prefix + k);
  }
  native_client_->multiGet(cFamily, keys, values, statuses, ro);
  for (auto i = 0ull; i < keys.size(); ++i) {
    const auto& status = statuses[i];
    auto& val = values[i];
    const auto& key = keys[i];
    if (status.ok()) {
      const char* data = nullptr;
      size_t size{};
      if (val.IsPinned()) {
        data = val.data();
        size = val.size();
      } else {
        data = val.GetSelf()->data();
        size = val.GetSelf()->size();
      }
      // add the previous value , already contains currect version as postfix
      auto actual_version = concordUtils::fromBigEndianBuffer<BlockId>(data + (size - sizeof(BlockId)));
      LOG_DEBUG(V4_BLOCK_LOG,
                "Found previous version for key " << key << " rocks sn " << sn << " version " << actual_version);
      write_batch.put(cFamily, key, val);
    } else if (status.IsNotFound()) {
      LOG_DEBUG(V4_BLOCK_LOG, "Couldn't find previous version for key " << key << " rocks sn " << sn);
      write_batch.del(cFamily, key);
    } else {
      throw std::runtime_error{"Revert multiGet() failure: " + status.ToString()};
    }
  }
}

template <typename DELETES>
void LatestKeys::revertDeletedKeysImp(const std::string& category_id,
                                      const DELETES& deletes,
                                      concord::storage::rocksdb::NativeWriteBatch& write_batch,
                                      ::rocksdb::Snapshot* snpsht) {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  auto sn = snpsht->GetSequenceNumber();
  ::rocksdb::ReadOptions ro;
  ro.snapshot = snpsht;
  std::vector<std::string> keys;
  keys.reserve(deletes.size());
  std::vector<::rocksdb::PinnableSlice> values;
  std::vector<::rocksdb::Status> statuses;
  for (const auto& k : deletes) {
    keys.emplace_back(prefix + k);
  }
  native_client_->multiGet(v4blockchain::detail::LATEST_KEYS_CF, keys, values, statuses, ro);
  for (auto i = 0ull; i < keys.size(); ++i) {
    const auto& status = statuses[i];
    auto& val = values[i];
    const auto& key = keys[i];
    if (status.ok()) {
      // add the previous value , already contains currect version as postfix
      const char* data = nullptr;
      size_t size{};
      if (val.IsPinned()) {
        data = val.data();
        size = val.size();
      } else {
        data = val.GetSelf()->data();
        size = val.GetSelf()->size();
      }
      auto actual_version = concordUtils::fromBigEndianBuffer<BlockId>(data + (size - sizeof(BlockId)));
      LOG_DEBUG(V4_BLOCK_LOG,
                "Found previous version for key " << key << " rocks sn " << sn << " version " << actual_version);
      write_batch.put(v4blockchain::detail::LATEST_KEYS_CF, key, val);
    } else if (status.IsNotFound()) {
      LOG_WARN(V4_BLOCK_LOG, "Couldn't find previous version for key " << key << " rocks sn " << sn);
    } else {
      throw std::runtime_error{"Revert multiGet() failure: " + status.ToString()};
    }
  }
}

const std::string& LatestKeys::getColumnFamilyFromCategory(const std::string& category_id) const {
  auto category_type = category_mapping_.categoryType(category_id);
  if (category_type == concord::kvbc::categorization::CATEGORY_TYPE::immutable) {
    return v4blockchain::detail::IMMUTABLE_KEYS_CF;
  }
  return v4blockchain::detail::LATEST_KEYS_CF;
}

std::optional<categorization::Value> LatestKeys::getValue(const std::string& category_id,
                                                          const std::string& key) const {
  std::string get_key;
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  get_key.append(prefix);
  get_key.append(key);
  auto category_type = category_mapping_.categoryType(category_id);
  const auto& column_family_str = getColumnFamilyFromCategory(category_id);

  auto opt_val = native_client_->get(column_family_str, get_key);
  if (!opt_val) {
    LOG_DEBUG(V4_BLOCK_LOG,
              "Reading key " << std::hash<std::string>{}(key) << " not found,  category_id " << category_id
                             << " prefix " << prefix << " key is hex "
                             << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
    return std::nullopt;
  }
  auto actual_version =
      concordUtils::fromBigEndianBuffer<BlockId>(opt_val->c_str() + (opt_val->size() - sizeof(BlockId)));
  const size_t total_val_size = opt_val->size();
  LOG_DEBUG(V4_BLOCK_LOG,
            "Reading key " << std::hash<std::string>{}(key) << " version " << actual_version << " category_id "
                           << category_id << " prefix " << prefix << " key is hex "
                           << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
  switch (category_type) {
    case concord::kvbc::categorization::CATEGORY_TYPE::block_merkle:
      return categorization::MerkleValue{{actual_version, opt_val->substr(0, total_val_size - VALUE_POSTFIX_SIZE)}};
    case concord::kvbc::categorization::CATEGORY_TYPE::immutable: {
      return categorization::ImmutableValue{{actual_version, opt_val->substr(0, total_val_size - VALUE_POSTFIX_SIZE)}};
    }
    case concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv: {
      return categorization::VersionedValue{{actual_version, opt_val->substr(0, total_val_size - VALUE_POSTFIX_SIZE)}};
    }
    default:
      ConcordAssert(false);
  }
}

void LatestKeys::multiGetValue(const std::string& category_id,
                               const std::vector<std::string>& keys,
                               std::vector<std::optional<categorization::Value>>& values) const {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  const auto& column_family_str = getColumnFamilyFromCategory(category_id);
  auto category_type = category_mapping_.categoryType(category_id);
  values.clear();
  values.resize(keys.size());
  std::vector<std::string> get_keys;
  get_keys.reserve(keys.size());
  std::vector<::rocksdb::Status> statuses;
  std::vector<::rocksdb::PinnableSlice> sl_values;
  statuses.reserve(keys.size());
  sl_values.reserve(keys.size());

  for (const auto& k : keys) {
    get_keys.emplace_back(prefix + k);
  }

  native_client_->multiGet(column_family_str, get_keys, sl_values, statuses);

  for (auto i = 0ull; i < get_keys.size(); ++i) {
    const auto& status = statuses[i];
    auto& sl_val = sl_values[i];
    const auto& key = get_keys[i];
    if (status.ok()) {
      const char* data = nullptr;
      size_t size{};
      if (sl_val.IsPinned()) {
        data = sl_val.data();
        size = sl_val.size();
      } else {
        data = sl_val.GetSelf()->data();
        size = sl_val.GetSelf()->size();
      }
      auto actual_version = concordUtils::fromBigEndianBuffer<BlockId>(data + (size - VERSION_SIZE));
      LOG_DEBUG(V4_BLOCK_LOG,
                "Reading key " << std::hash<std::string>{}(key) << " version " << actual_version << " category_id "
                               << category_id << " prefix " << prefix << " key is hex "
                               << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
      switch (category_type) {
        case concord::kvbc::categorization::CATEGORY_TYPE::block_merkle:
          values[i] = categorization::MerkleValue{{actual_version, std::string(data, size - VALUE_POSTFIX_SIZE)}};
          break;
        case concord::kvbc::categorization::CATEGORY_TYPE::immutable: {
          values[i] = categorization::ImmutableValue{{actual_version, std::string(data, size - VALUE_POSTFIX_SIZE)}};
          break;
        }
        case concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv: {
          values[i] = categorization::VersionedValue{{actual_version, std::string(data, size - VALUE_POSTFIX_SIZE)}};
          break;
        }
        default:
          ConcordAssert(false);
      }
    } else if (status.IsNotFound()) {
      LOG_DEBUG(V4_BLOCK_LOG,
                "Reading key " << std::hash<std::string>{}(key) << " not found,  category_id " << category_id
                               << " prefix " << prefix << " key is hex "
                               << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
      values[i] = std::nullopt;
    } else {
      throw std::runtime_error{"Revert multiGet() failure: " + status.ToString()};
    }
  }
}

std::optional<categorization::TaggedVersion> LatestKeys::getLatestVersion(const std::string& category_id,
                                                                          const std::string& key) const {
  auto category_type = category_mapping_.categoryType(category_id);
  const std::string* column_family_ptr = nullptr;
  if (category_type == concord::kvbc::categorization::CATEGORY_TYPE::immutable) {
    column_family_ptr = &(v4blockchain::detail::IMMUTABLE_KEYS_CF);
  } else {
    column_family_ptr = &(v4blockchain::detail::LATEST_KEYS_CF);
  }
  std::string get_key;
  std::string out_ts;
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  get_key.append(prefix);
  get_key.append(key);
  auto opt_val = native_client_->get(*column_family_ptr, get_key);
  if (!opt_val) {
    LOG_DEBUG(V4_BLOCK_LOG,
              "Reading key version " << std::hash<std::string>{}(key) << " not found, category_id " << category_id
                                     << " prefix " << prefix << " key is hex "
                                     << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
    return std::nullopt;
  }
  BlockId version = concordUtils::fromBigEndianBuffer<BlockId>(opt_val->c_str() + (opt_val->size() - sizeof(BlockId)));
  LOG_DEBUG(V4_BLOCK_LOG,
            "Reading key version " << std::hash<std::string>{}(key) << " version " << version << " category_id "
                                   << category_id << " prefix " << prefix << " key is hex "
                                   << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
  return categorization::TaggedVersion{false, version};
}

void LatestKeys::multiGetLatestVersion(const std::string& category_id,
                                       const std::vector<std::string>& keys,
                                       std::vector<std::optional<categorization::TaggedVersion>>& versions) const {
  const auto& prefix = category_mapping_.categoryPrefix(category_id);
  const auto& column_family_str = getColumnFamilyFromCategory(category_id);
  std::vector<std::string> get_keys;
  get_keys.reserve(keys.size());
  std::vector<::rocksdb::Status> statuses;
  std::vector<::rocksdb::PinnableSlice> sl_values;
  versions.clear();
  versions.resize(keys.size());
  statuses.reserve(keys.size());
  sl_values.reserve(keys.size());

  for (const auto& k : keys) {
    get_keys.emplace_back(prefix + k);
  }

  native_client_->multiGet(column_family_str, get_keys, sl_values, statuses);

  for (auto i = 0ull; i < get_keys.size(); ++i) {
    const auto& status = statuses[i];
    auto& sl_val = sl_values[i];
    const auto& key = get_keys[i];
    if (status.ok()) {
      const char* data = nullptr;
      size_t size{};
      if (sl_val.IsPinned()) {
        data = sl_val.data();
        size = sl_val.size();
      } else {
        data = sl_val.GetSelf()->data();
        size = sl_val.GetSelf()->size();
      }
      auto actual_version = concordUtils::fromBigEndianBuffer<BlockId>(data + (size - VERSION_SIZE));
      LOG_DEBUG(V4_BLOCK_LOG,
                "Reading key version " << std::hash<std::string>{}(key) << " version " << actual_version
                                       << " category_id " << category_id << " prefix " << prefix << " key is hex "
                                       << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
      versions[i] = categorization::TaggedVersion{false, actual_version};
    } else if (status.IsNotFound()) {
      LOG_DEBUG(V4_BLOCK_LOG,
                "Reading key version " << std::hash<std::string>{}(key) << " not found, category_id " << category_id
                                       << " prefix " << prefix << " key is hex "
                                       << concordUtils::bufferToHex(key.data(), key.size()) << " raw key " << key);
      versions[i] = std::nullopt;
    } else {
      throw std::runtime_error{"Revert multiGet() failure: " + status.ToString()};
    }
  }
}

bool LatestKeys::LKCompactionFilter::Filter(int /*level*/,
                                            const ::rocksdb::Slice& key,
                                            const ::rocksdb::Slice& val,
                                            std::string* /*new_value*/,
                                            bool* /*value_changed*/) const {
  if (!LatestKeys::isStaleOnUpdate(val) || val.size() <= VERSION_SIZE) return false;
  auto ts_slice = ::rocksdb::Slice(val.data() + val.size() - VERSION_SIZE, VERSION_SIZE);
  auto key_version = concordUtils::fromBigEndianBuffer<uint64_t>(ts_slice.data());
  if (key_version >= concord::kvbc::v4blockchain::detail::Blockchain::global_genesis_block_id) return false;
  LOG_INFO(V4_BLOCK_LOG,
           "Filtering key with version " << key_version << " genesis is "
                                         << concord::kvbc::v4blockchain::detail::Blockchain::global_genesis_block_id);
  return true;
}

}  // namespace concord::kvbc::v4blockchain::detail
