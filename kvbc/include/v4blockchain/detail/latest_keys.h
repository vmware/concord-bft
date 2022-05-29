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

#pragma once

#include "rocksdb/native_client.h"
#include <memory>
#include <unordered_map>
#include "categorization/updates.h"
#include "v4blockchain/detail/categories.h"
#include "rocksdb/time_stamp_comparator.h"
#include <rocksdb/compaction_filter.h>
#include "endianness.hpp"
#include "hex_tools.h"

namespace concord::kvbc::v4blockchain::detail {

/*
The latest keys are the state of the blockchain.
It's implemented as a column family, where all the keys of a block that is being added are added to it.
It uses the RocksDb timestamp API to mark the version of the key.
A key can be:
- newly added - in this case it has a single version i.e. the block that it was added.
- updated - a key may have several versions (accessible) until we mark its history as save to delete.
- deleted - trying to access with the deletion version or higher will return nullopt.

For backward compatibility with the previous categorized implementation, a key belongs to a category.
the category is represented by a prefix, the following properties of categories will be honored by this
implementation as well:
- version category : stale on update i.e. a keys is prunable although it's the latest version when its block is deleted.
- immutable - updating an immutable key is an error.
*/
class LatestKeys {
 public:
  // Array is used for making it "sliceable"
  using Flags = std::array<char, 1>;
  // E.L need to be used with compaction filter
  static constexpr Flags STALE_ON_UPDATE = {0x1};
  static constexpr size_t FLAGS_SIZE = STALE_ON_UPDATE.size();
  LatestKeys(const std::shared_ptr<concord::storage::rocksdb::NativeClient>&,
             const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>&,
             std::function<BlockId()>&& f);
  void addBlockKeys(const concord::kvbc::categorization::Updates&, BlockId, storage::rocksdb::NativeWriteBatch&);

  void handleCategoryUpdates(const std::string& block_version,
                             const std::string& category_id,
                             const concord::kvbc::categorization::BlockMerkleInput&,
                             concord::storage::rocksdb::NativeWriteBatch&);
  void handleCategoryUpdates(const std::string& block_version,
                             const std::string& category_id,
                             const concord::kvbc::categorization::VersionedInput&,
                             concord::storage::rocksdb::NativeWriteBatch&);
  void handleCategoryUpdates(const std::string& block_version,
                             const std::string& category_id,
                             const concord::kvbc::categorization::ImmutableInput&,
                             concord::storage::rocksdb::NativeWriteBatch&);

  // Delete the last added block keys
  void revertLastBlockKeys(const concord::kvbc::categorization::Updates&, BlockId, storage::rocksdb::NativeWriteBatch&);

  void revertCategoryKeys(const std::string& block_version,
                          const std::string& prev_block_version,
                          const std::string& category_id,
                          const categorization::BlockMerkleInput& updates,
                          concord::storage::rocksdb::NativeWriteBatch& write_batch);
  void revertCategoryKeys(const std::string& block_version,
                          const std::string& prev_block_version,
                          const std::string& category_id,
                          const categorization::VersionedInput& updates,
                          concord::storage::rocksdb::NativeWriteBatch& write_batch);
  void revertCategoryKeys(const std::string& block_version,
                          const std::string& prev_block_version,
                          const std::string& category_id,
                          const categorization::ImmutableInput& updates,
                          concord::storage::rocksdb::NativeWriteBatch& write_batch);

  template <typename UPDATES>
  void revertCategoryKeysImp(const std::string& cFamily,
                             const std::string& block_version,
                             const std::string& prev_block_version,
                             const std::string& category_id,
                             const UPDATES& updates,
                             concord::storage::rocksdb::NativeWriteBatch& write_batch);
  template <typename DELETES>
  void revertDeletedKeysImp(const std::string& block_version,
                            const std::string& prev_block_version,
                            const std::string& category_id,
                            const DELETES& deletes,
                            concord::storage::rocksdb::NativeWriteBatch& write_batch);

  const std::string& getCategoryPrefix(const std::string& category) const {
    return category_mapping_.categoryPrefix(category);
  }
  // Mark a version that is safe to delete on compaction.
  void trimHistoryUntil(BlockId block_id);

  static ::rocksdb::Slice getFlagsSlice(const ::rocksdb::Slice& val) {
    ConcordAssertGE(val.size(), FLAGS_SIZE);
    return ::rocksdb::Slice(val.data() + val.size() - FLAGS_SIZE, FLAGS_SIZE);
  }
  // check the key flags posfix for stale on update
  static bool isStaleOnUpdate(const ::rocksdb::Slice& val) {
    auto flags_sl = getFlagsSlice(val);
    auto stale_flag = concord::storage::rocksdb::detail::toSlice(STALE_ON_UPDATE);
    return flags_sl == stale_flag;
  }

  ::rocksdb::CompactionFilter* getCompFilter() { return &comp_filter_; }

  // get the value and return deserialized value if needed.
  std::optional<categorization::Value> getValue(const std::string& category_id,
                                                const std::string& version,
                                                const std::string& key) const;

  // return multiple values, supposed to be more efficient.
  void multiGetValue(const std::string& category_id,
                     const std::string& version,
                     const std::vector<std::string>& keys,
                     std::vector<std::optional<categorization::Value>>& values) const;

  // returns the latest block id nearest to the last block id or latest version.
  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string& category_id,
                                                                const std::string& latest_version,
                                                                const std::string& key) const;
  // returns multiple latest block ids which which are nearest to the last block id or latest version.
  void multiGetLatestVersion(const std::string& category_id,
                             const std::string& latest_version,
                             const std::vector<std::string>& keys,
                             std::vector<std::optional<categorization::TaggedVersion>>& versions) const;

  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> getCategories() const {
    return category_mapping_.getCategories();
  }

 private:
  // This filter is used to delete stale on update keys if their version is smaller than the genesis block
  // It's being called by RocksDB on compaction
  struct LKCompactionFilter : ::rocksdb::CompactionFilter {
    LKCompactionFilter(std::function<BlockId()> f) : genesis_id(f) {}
    const char* Name() const override { return "LatestKeysCompactionFilter"; }
    bool Filter(int /*level*/,
                const ::rocksdb::Slice& key,
                const ::rocksdb::Slice& val,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override;
    std::function<BlockId()> genesis_id;
  };

  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  v4blockchain::detail::Categories category_mapping_;
  LKCompactionFilter comp_filter_;
};

}  // namespace concord::kvbc::v4blockchain::detail
